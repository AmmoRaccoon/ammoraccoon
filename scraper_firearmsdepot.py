import os
import re
import time
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

from scraper_lib import (
    CALIBERS, now_iso, with_stock_fields, parse_purchase_limit,
    parse_brand, sanity_check_ppr, clean_title, normalize_caliber,
)

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "firearmsdepot"
SITE_BASE = "https://firearmsdepot.com"

# BigCommerce stencil store. Verified 2026-04-26: no per-caliber static
# URLs exist — caliber filtering is hash-based JS (e.g.
# /ammunition/#/filter:custom_caliber_or_gauge:9MM) which doesn't
# support the cheap ?page=N pagination we lean on elsewhere.
#
# We crawl the three usage-type parent categories instead and use the
# scraper_lib normalize_caliber() helper to bucket each product into
# one of the 10 calibers we track. Anything that doesn't match (e.g.
# .45 ACP, 12GA, .22 WMR) is silently skipped.
PARENT_PATHS = [
    '/ammunition/centerfire-handgun-rounds/',  # 9mm, 380, 40sw, 38spl, 357mag, +others
    '/ammunition/centerfire-rifle-rounds/',    # 223-556, 308, 762x39, 300blk, +others
    '/ammunition/rimfire-rounds/',             # 22lr, +others
]

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


def parse_bullet_type(text):
    text_upper = text.upper()
    for bt in ['FMJ', 'JHP', 'HP', 'OTM', 'TMJ', 'SP', 'FP']:
        if bt in text_upper:
            return bt
    if 'HOLLOW POINT' in text_upper:
        return 'JHP'
    if 'FULL METAL' in text_upper:
        return 'FMJ'
    return None


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

    for page_num in range(1, MAX_PAGES + 1):
        url = base if page_num == 1 else f"{base}?page={page_num}"
        print(f"\n[{parent_path}] Loading page {page_num}: {url}")
        try:
            resp = page.goto(url, wait_until='domcontentloaded', timeout=60000)
        except Exception as e:
            print(f"  goto failed: {e}")
            break
        if resp and resp.status >= 400:
            print(f"  HTTP {resp.status} - stopping parent.")
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
                bullet_type = parse_bullet_type(name)
                country = parse_country(name)
                manufacturer = parse_brand(name) or "Unknown"
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

                supabase.table('price_history').insert({
                    'listing_id': result.data[0]['id'],
                    'price': base_price,
                    'price_per_round': price_per_round,
                    'in_stock': in_stock,
                }).execute()

                saved += 1
                counts[caliber_norm] = counts.get(caliber_norm, 0) + 1
                print(f"  Saved [{caliber_norm}]: {name[:55]} | ${base_price} | {price_per_round}/rd")

            except Exception as e:
                skipped += 1
                print(f"  Skipped: {e}")
                continue

    return saved, skipped


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

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })

        for parent in PARENT_PATHS:
            saved, skipped = scrape_parent(page, parent, retailer_id, seen_ids, counts)
            total_saved += saved
            total_skipped += skipped

        browser.close()

    print(f"\nDone! Saved: {total_saved} | Skipped: {total_skipped}")
    print("Per-caliber counts:")
    for cal in CALIBERS:
        print(f"  {cal}: {counts.get(cal, 0)}")


if __name__ == '__main__':
    scrape()
