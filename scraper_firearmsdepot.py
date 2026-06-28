import os
import re
import time
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

from scraper_lib import (
    insert_price_history,
    CALIBERS, now_iso, with_stock_fields, parse_purchase_limit,
    parse_brand_with_url, sanity_check_ppr, clean_title, normalize_caliber,
    parse_bullet_type_with_url_fallback,
    mark_retailer_scraped,
    load_parent_paths, category_redirected, report_empty_first_pages,
)

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "firearmsdepot"
SITE_BASE = "https://firearmsdepot.com"

# BigCommerce stencil store. No per-caliber static URLs exist — caliber
# filtering is hash-based JS (/ammunition/#/filter:...) that breaks the cheap
# ?page=N pagination — so we crawl the three usage-type parent categories and
# bucket each product by normalize_caliber (off-list calibers like .45 ACP /
# 12GA / .22 WMR silently skipped). The parent paths now live in
# caliber_paths/firearmsdepot.json under parent_paths (expansion #4 Step-2
# migration) — transcribed verbatim, parity-proven byte-identical.
# load_parent_paths returns [entry, ...]; entry['url'] is a drop-in for the
# old parent-path string.
PARENT_PATHS = load_parent_paths('firearmsdepot')

# Defensive cap. Handgun parent had ~2747 listings on probe day at
# 24/page = ~115 pages. 160 leaves headroom; the loop bails early
# when a page returns no cards.
MAX_PAGES = 160

# Non-ammo products that get cross-listed under /ammunition/ but are
# storage / reloading accessories. Their titles often contain
# "Holds 1000rds of 9mm" type copy, which fools both normalize_caliber
# (matches "9mm") and parse_rounds (matches "1000rds"), letting the
# accessory ride into listings as a $4970/round 9mm SKU. Surfaced
# 2026-04-26 by the price audit (Hornady 99137 plastic ammo box, id
# 53297). Block these by title/URL substring before extraction.
NON_AMMO_TITLE_BLOCKLIST = (
    'ammo box', 'ammobox',
    'ammo storage', 'ammo-storage',
    'ammo tray', 'ammo-tray',
    'case feeder', 'casefeeder',
    'bullet puller',
    'reloading', 'loading block',
    'brass catcher',
    'speed loader', 'speedloader',
    'powder funnel', 'priming tool', 'primer tool',
    'magazine loader',
    'cleaning kit', 'bore snake', 'gun cleaner',
)
NON_AMMO_URL_BLOCKLIST = (
    'ammo-box', 'ammo-can-only', 'ammo-storage',
    '/storage/', '/reloading/', '/cleaning/',
)


def is_non_ammo_product(name, product_url):
    """Return True for storage/accessory products that share the
    /ammunition/ category tree with real ammo. Conservative — only
    rejects on phrases that wouldn't appear in a legit ammo title."""
    nl = (name or '').lower()
    ul = (product_url or '').lower()
    for kw in NON_AMMO_TITLE_BLOCKLIST:
        if kw in nl:
            return True
    for kw in NON_AMMO_URL_BLOCKLIST:
        if kw in ul:
            return True
    return False


# ---------------------------------------------------------------------------
# Source-mislabel guard: title caliber vs URL-slug caliber.
#
# firearmsdepot occasionally publishes a product whose TITLE names one caliber
# while the URL slug reveals a different, look-alike caliber — the canonical
# case being a ".270 Win" title on a .270 WSM product whose slug says
# "270wsm". normalize_caliber faithfully reads the (wrong) title and tags it
# 270win, so the defect is in the SOURCE's title, not our detection; a
# title-only re-tag cannot catch it (DECISIONS 2026-06-27). When the slug
# carries a conflicting caliber token that the title hides, we trust the slug
# and DROP the row (honest blank) rather than mis-tag it.
#
# Table-driven so new look-alike pairs are a one-line add. Keyed by the
# caliber the TITLE normalizes to; each value is a tuple of regexes that, when
# found in the slug but ABSENT from the title, mark a genuine caliber
# conflict. ONLY real caliber tokens belong here — never bullet diameters or
# SKU digits, which would over-drop legitimate rows.
TITLE_SLUG_CALIBER_CONFLICTS = {
    # .270 Win (tracked) vs .270 WSM (untracked, confusable). FD strips the
    # "WSM"/"Short Mag" from the title but leaves it in the slug. Both
    # spellings covered. Verified live: catches the X270SDSLF Deer Season WSM
    # row, drops nothing legitimate (2026-06-28 dry-run).
    '270win': (r'270[\s\-]*wsm', r'\bwsm\b', r'short[\s\-]*mag'),
}


def slug_contradicts_title_caliber(title_caliber_norm, title, product_url):
    """Return True when the URL slug reveals a caliber that conflicts with the
    title's normalized caliber — a source mislabel we drop rather than trust.

    Conservative by construction: fires only on the curated look-alike tokens
    in TITLE_SLUG_CALIBER_CONFLICTS, and only when a token appears in the slug
    but NOT in the title (so a correctly-titled product is never dropped)."""
    patterns = TITLE_SLUG_CALIBER_CONFLICTS.get(title_caliber_norm)
    if not patterns:
        return False
    slug = (product_url or '').lower()
    title_l = (title or '').lower()
    for pat in patterns:
        if re.search(pat, slug) and not re.search(pat, title_l):
            return True
    return False


def get_retailer_id():
    result = supabase.table("retailers").select("id").eq("slug", RETAILER_SLUG).execute()
    if not result.data:
        print(f"ERROR: Retailer '{RETAILER_SLUG}' not found in database")
        return None
    return result.data[0]["id"]


def parse_grain(text):
    m = re.search(r'(\d+)[\s-]*gr(?:ain)?\b', text, re.IGNORECASE)
    return int(m.group(1)) if m else None


def parse_rounds(text):
    # FD card titles use a few interchangeable forms:
    #   "1000 Rounds", "500 Rnds", "20 Rounds [MPN: ...]",
    #   "1000 Rnd Case", "500 Round Case", "50 Rounds Per Box".
    # Match these explicitly so a stray bracketed MPN like "[MPN: 5200]"
    # never gets read as a round count.
    patterns = [
        r'(\d[\d,]*)\s*[- ]?\s*rounds?\b',
        r'(\d[\d,]*)\s*[- ]?\s*rnds?\b',
        r'(\d[\d,]*)\s*[- ]?\s*rds?\b',
        r'(\d[\d,]*)\s*[- ]?\s*rd\s*(?:box|case|pack)',
        r'(\d[\d,]*)\s*per\s*box',
        r'(\d[\d,]*)\s*[- ]?\s*count\b',
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return int(m.group(1).replace(',', ''))
    return None


def parse_case_material(text):
    text_lower = text.lower()
    steel_brands = ['wolf', 'tula', 'tulammo', 'brown bear', 'silver bear', 'golden bear', 'barnaul']
    if any(brand in text_lower for brand in steel_brands):
        return 'Steel'
    if 'steel case' in text_lower or 'steel-case' in text_lower:
        return 'Steel'
    if 'steel' in text_lower:
        return 'Steel'
    elif 'brass' in text_lower:
        return 'Brass'
    elif 'aluminum' in text_lower:
        return 'Aluminum'
    elif 'nickel' in text_lower:
        return 'Nickel'
    return 'Brass'


def parse_country(text):
    text_lower = text.lower()
    mapping = {
        'federal': 'USA', 'winchester': 'USA', 'remington': 'USA',
        'cci': 'USA', 'speer': 'USA', 'hornady': 'USA',
        'blazer': 'USA', 'fiocchi': 'USA', 'american eagle': 'USA',
        'magtech': 'Brazil', 'cbc': 'Brazil',
        'ppu': 'Serbia', 'prvi partizan': 'Serbia',
        'sellier': 'Czech Republic', 'tula': 'Russia',
        'wolf': 'Russia', 'aguila': 'Mexico', 'sterling': 'Turkey',
        'igman': 'Bosnia',
    }
    for keyword, country in mapping.items():
        if keyword in text_lower:
            return country
    return None


def scrape_parent(page, parent_path, retailer_id, seen_ids, counts):
    base = SITE_BASE + parent_path
    saved = 0
    skipped = 0
    empty_first_page = False

    for page_num in range(1, MAX_PAGES + 1):
        url = base if page_num == 1 else f"{base}?page={page_num}"
        print(f"\n[{parent_path}] Loading page {page_num}: {url}")
        try:
            resp = page.goto(url, wait_until='domcontentloaded', timeout=60000)
        except Exception as e:
            print(f"  goto failed: {e}")
            if page_num == 1:
                empty_first_page = True
            break
        if resp and resp.status >= 400:
            print(f"  HTTP {resp.status} - stopping parent.")
            if page_num == 1:
                empty_first_page = True
            break
        # Redirect guard (NEW 2026-06-14, expansion #4 Step-2 — FD had none
        # before): a parent category that 301s to a DIFFERENT page (renamed)
        # stops the parent and flags it empty, feeding the storefront-drift
        # guardrail rather than silently crawling the wrong tree.
        if page_num == 1 and category_redirected(url, page.url):
            print(f"  REDIRECTED to {page.url} - parent moved/renamed; stopping.")
            empty_first_page = True
            break
        time.sleep(2)

        # FD's BigCommerce stencil wraps every card in <article class="card">
        # directly under <ul class="productGrid">. The user explicitly
        # called out this scoped selector — broad fallbacks like
        # 'article.card' alone match cards from related-products carousels
        # rendered on every category page.
        cards = page.query_selector_all('.productGrid article.card')
        if not cards:
            print(f"  No cards on page {page_num}, stopping parent.")
            if page_num == 1:
                empty_first_page = True
            break

        for card in cards:
            try:
                # Title + URL — both live on the card-title h4's child <a>.
                link_el = card.query_selector('h4.card-title a')
                if not link_el:
                    skipped += 1
                    continue
                href = link_el.get_attribute('href') or ''
                if not href:
                    skipped += 1
                    continue
                product_url = href if href.startswith('http') else SITE_BASE + href

                # Skip brand-carousel cards if they ever appear in the grid.
                if '/brands/' in product_url:
                    skipped += 1
                    continue

                raw_name = (link_el.get_attribute('title')
                            or link_el.inner_text() or '').strip()
                name = clean_title(raw_name)
                if not name:
                    skipped += 1
                    continue

                # Filter storage/reloading accessories before any
                # caliber/rounds parsing — their titles ("Holds
                # 1000rds of 9mm") otherwise extract a fake 9mm
                # listing.
                if is_non_ammo_product(name, product_url):
                    skipped += 1
                    continue

                # Bucket by caliber detected from the title. Skip anything
                # outside our 10 calibers (shotgun, .45 ACP, .22 WMR, etc.).
                caliber_display, caliber_norm = normalize_caliber(name)
                if not caliber_norm:
                    skipped += 1
                    continue

                # Source-mislabel guard: when the URL slug reveals a caliber
                # that conflicts with the title (e.g. a ".270 Win" title on a
                # product whose slug says "270wsm"), trust the slug and drop
                # rather than mis-tag. See DECISIONS 2026-06-27.
                if slug_contradicts_title_caliber(caliber_norm, name, product_url):
                    skipped += 1
                    print(f"  Dropped (title/slug caliber conflict): "
                          f"{name[:55]} | {product_url}")
                    continue

                # Stock detection. FD renders an "Out of Stock" badge in
                # .out-of-stock or as text on .card-figcaption-button.
                # Sold-out cards still expose the price element so we
                # record them with in_stock=False rather than dropping.
                card_text = (card.inner_text() or '')
                card_lower = card_text.lower()
                in_stock = ('out of stock' not in card_lower and
                            'sold out' not in card_lower and
                            'unavailable' not in card_lower)

                # Real price lives in [data-product-price-without-tax]
                # inside .card-price. The .price--rrp / .price--non-sale
                # spans hold compare-at amounts we don't want.
                price_el = (card.query_selector('[data-product-price-without-tax]')
                            or card.query_selector('.card-price .price--withoutTax'))
                base_price = None
                if price_el:
                    price_text = (price_el.inner_text() or '').strip()
                    m = re.search(r'\$?(\d{1,5}(?:,\d{3})*(?:\.\d{1,2})?)', price_text)
                    if m:
                        try:
                            base_price = float(m.group(1).replace(',', ''))
                        except ValueError:
                            base_price = None
                if not base_price or base_price <= 0:
                    skipped += 1
                    print(f"  Skipped (no price): {name[:55]}")
                    continue

                total_rounds = parse_rounds(name)
                if not total_rounds or total_rounds <= 0:
                    skipped += 1
                    continue

                price_per_round = round(base_price / total_rounds, 4)
                if not sanity_check_ppr(price_per_round, base_price, total_rounds,
                                        context=f'{RETAILER_SLUG} {caliber_norm}',
                                        caliber=caliber_norm):
                    skipped += 1
                    continue

                grain = parse_grain(name)
                case_material = parse_case_material(name)
                # FD slugs commonly expose v-max / lrn / hornady-vmx
                # tokens that the title parser misses; the audit found
                # 96 slug-says-but-NULL rows here. Slug fallback closes
                # most of those.
                bullet_type = parse_bullet_type_with_url_fallback(name, product_url)
                country = parse_country(name)
                manufacturer = parse_brand_with_url(name, product_url) or "Unknown"
                purchase_limit = parse_purchase_limit(card_text)

                # FD exposes a stable numeric id as data-product-id on the
                # card root. Falls back to the URL slug if the attribute
                # ever drops (other BigCommerce stencils have done this
                # on related-products variants).
                product_id = card.get_attribute('data-product-id') \
                    or product_url.rstrip('/').split('/')[-1][:100]
                if not product_id or product_id in seen_ids:
                    continue
                seen_ids.add(product_id)

                listing = {
                    'retailer_id': retailer_id,
                    'retailer_product_id': product_id,
                    'product_url': product_url,
                    'caliber': caliber_display,
                    'caliber_normalized': caliber_norm,
                    'grain': grain,
                    'bullet_type': bullet_type,
                    'case_material': case_material,
                    'condition_type': 'New',
                    'country_of_origin': country,
                    'manufacturer': manufacturer,
                    'rounds_per_box': total_rounds,
                    'boxes_per_case': 1,
                    'total_rounds': total_rounds,
                    'base_price': base_price,
                    'price_per_round': price_per_round,
                    'purchase_limit': purchase_limit,
                    'last_updated': now_iso(),
                }
                with_stock_fields(listing, in_stock)

                result = supabase.table('listings').upsert(
                    listing,
                    on_conflict='retailer_id,retailer_product_id'
                ).execute()

                insert_price_history(supabase, {
                    'listing_id': result.data[0]['id'],
                    'price': base_price,
                    'price_per_round': price_per_round,
                    'in_stock': in_stock,
                })

                saved += 1
                counts[caliber_norm] = counts.get(caliber_norm, 0) + 1
                print(f"  Saved [{caliber_norm}]: {name[:55]} | ${base_price} | {price_per_round}/rd")

            except Exception as e:
                skipped += 1
                print(f"  Skipped: {e}")
                continue

    return saved, skipped, empty_first_page


def scrape():
    print(f"[{datetime.now()}] Starting Firearms Depot scraper (all calibers)...")
    retailer_id = get_retailer_id()
    if not retailer_id:
        return

    print(f"Retailer ID: {retailer_id}")

    total_saved = 0
    total_skipped = 0
    seen_ids = set()
    counts = {}
    empty_handles = []  # (label, parent url) for the storefront-drift guardrail

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })

        for entry in PARENT_PATHS:
            saved, skipped, empty = scrape_parent(page, entry['url'], retailer_id, seen_ids, counts)
            total_saved += saved
            total_skipped += skipped
            if empty:
                empty_handles.append(('parent', entry['url']))

        browser.close()

    # Storefront-drift guardrail + freshness honesty (NEW 2026-06-14): all
    # three parent categories empty on first page (>= EMPTY_FAIL_THRESHOLD)
    # exits non-zero (site restructure / wall); only advance last_scraped_at
    # when the run actually saved something.
    report_empty_first_pages(empty_handles, 'Firearms Depot')
    mark_retailer_scraped(supabase, retailer_id, had_success=(total_saved > 0))
    print(f"\nDone! Saved: {total_saved} | Skipped: {total_skipped}")
    print("Per-caliber counts:")
    for cal in CALIBERS:
        print(f"  {cal}: {counts.get(cal, 0)}")


if __name__ == '__main__':
    scrape()
