import os
import re
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

from scraper_lib import (
    insert_price_history,
    CALIBERS, now_iso, with_stock_fields, parse_purchase_limit,
    parse_brand_with_url, sanity_check_ppr, parse_bullet_type,
    mark_retailer_scraped,
    load_caliber_paths, category_redirected, report_empty_first_pages,
)

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "shadowsmith"
# shadowsmithammo.com 301-redirects to www.shadowsmith.net.
SITE_BASE = "https://www.shadowsmith.net"

# Per-caliber category URLs now live in caliber_paths/shadowsmith.json
# (expansion #4 Step-2 migration) — transcribed verbatim, parity-proven
# byte-identical. WooCommerce store; the canonical host is
# www.shadowsmith.net (shadowsmithammo.com 301s to it). Values are now
# LISTS of entries and entry['url'] is a drop-in for the old SITE_BASE +
# path string.
CALIBER_PATHS = load_caliber_paths('shadowsmith')

def get_retailer_id():
    result = supabase.table("retailers").select("id").eq("slug", RETAILER_SLUG).execute()
    if not result.data:
        print(f"ERROR: Retailer '{RETAILER_SLUG}' not found in database")
        return None
    return result.data[0]["id"]

def parse_grain(text):
    match = re.search(r'(\d+)[\s-]*gr(?:ain)?', text, re.IGNORECASE)
    return int(match.group(1)) if match else None

def is_non_ammo(title):
    """Reject snap caps, dummy rounds, and other non-ammo trainers
    that show up in ammunition categories. These have no real round
    count and aren't products users are price-comparing."""
    t = title.lower()
    return any(k in t for k in [
        'snap cap', 'snap caps', 'dummy round', 'dummy rounds',
        'a-zoom', 'azoom',
    ])


def parse_rounds(text):
    # Shadowsmith titles use a wider variety of round-count formats
    # than other retailers — explicit "20 rounds", compact "1400RD",
    # slash-separated "250/ct", "20/Box", and prose like "Box of 20".
    # The earlier patterns only caught the first two; everything else
    # was falling through to "no round count" and getting skipped.
    patterns = [
        r'(\d[\d,]*)\s*rounds?',
        r'(\d[\d,]*)\s*rd[s]?\b',
        r'(\d[\d,]*)\s*rnd[s]?\b',
        r'(\d[\d,]*)\s*/\s*ct\b',
        r'(\d[\d,]*)\s*ct\b',
        r'(\d[\d,]*)\s*/\s*box\b',
        r'(\d[\d,]*)\s*/\s*bx\b',
        r'(\d[\d,]*)\s*per\s*box',
        r'\bbox\s+of\s+(\d[\d,]*)\b',
        r'(\d[\d,]*)\s*-\s*count\b',
        # Manufacturer SKU notation "50/10" = 50 rounds per box ×
        # 10 boxes per case. The displayed price is per-box, so we
        # take the first number as the listing's round count.
        # Bound the first number to 1–5000 so we don't grab dates
        # or UPC fragments.
        r'\b(\d{2,4})/\d{1,4}\b',
        r'(\d[\d,]*)\s*count',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return int(match.group(1).replace(',', ''))
    return None

def parse_case_material(text):
    text_lower = text.lower()
    steel_brands = ['wolf', 'tula', 'tulammo', 'brown bear', 'silver bear', 'golden bear', 'barnaul']
    if any(brand in text_lower for brand in steel_brands):
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
        'wolf': 'Russia', 'aguila': 'Mexico',
    }
    for keyword, country in mapping.items():
        if keyword in text_lower:
            return country
    return None

def scrape_category_page(page, entry, caliber_norm, caliber_display, retailer_id, seen_ids):
    url = SITE_BASE + entry['url']
    print(f"\n[{caliber_norm}] Loading: {url}")
    try:
        resp = page.goto(url, wait_until='domcontentloaded', timeout=90000)
    except Exception as e:
        print(f"  goto failed: {e}")
        return 0, 0, True
    if resp and resp.status >= 400:
        print(f"  HTTP {resp.status} - skipping caliber.")
        return 0, 0, True
    # Redirect guard (NEW 2026-06-14, expansion #4 Step-2 — Shadowsmith
    # had none before). WooCommerce duplicate-slug renames are exactly
    # the drift this catches: a category that 301s to a DIFFERENT page
    # with HTTP 200 skips loudly and counts as an empty handle.
    if category_redirected(url, page.url):
        print(f"  REDIRECTED to {page.url} - skipping (category moved/renamed).")
        return 0, 0, True
    # Shadowsmith renders behind an age-verification overlay but the
    # product DOM mounts regardless. Wait for the title selector
    # explicitly so we don't race the JS that paints products into the
    # grid.
    try:
        page.wait_for_selector('.woocommerce-loop-product__title', timeout=20000)
    except Exception:
        print(f"  no product titles rendered after 20s - skipping caliber.")
        return 0, 0, True
    time.sleep(2)

    # WooCommerce: products are <li class="...product..."> inside a
    # <ul class="products">. Class string starts with "entry content-bg
    # loop-entry product type-product post-NNNN ..." — Playwright's CSS
    # selector handles unordered class matching so li.product works.
    products = page.query_selector_all('ul.products li.product, li.product')
    print(f"  Found {len(products)} products")
    if not products:
        return 0, 0, True

    saved = 0
    skipped = 0

    for product in products:
        try:
            # Title link — Shadowsmith uses /product/ (singular) URLs.
            link_el = (product.query_selector('h2.woocommerce-loop-product__title a')
                       or product.query_selector('a.woocommerce-LoopProduct-link-title')
                       or product.query_selector('a[href*="/product/"]'))
            if not link_el:
                skipped += 1
                continue
            href = link_el.get_attribute('href') or ''
            if not href:
                skipped += 1
                continue
            product_url = href if href.startswith('http') else SITE_BASE + href

            # Skip brand-carousel cards that occasionally render inside
            # the product grid wrapper. They look like products to the
            # price/round regex (promo banner + carousel number) and
            # otherwise get saved with the loop's caliber tag attached.
            if '/brands/' in product_url:
                skipped += 1
                continue

            title_el = product.query_selector('h2.woocommerce-loop-product__title, .woocommerce-loop-product__title')
            raw_name = (title_el.inner_text().strip() if title_el
                        else link_el.inner_text().strip())
            # Drop common typographic glyphs so the listings table stays
            # ASCII-clean across terminals/locales.
            TYPOGRAPHIC = str.maketrans({
                '–': '-', '—': '-',
                '‘': "'", '’': "'", '“': '"', '”': '"',
                '®': '', '™': '',
                '·': '*', '•': '*', '×': 'x',
            })
            name = raw_name.translate(TYPOGRAPHIC).strip()
            if not name:
                skipped += 1
                continue
            # Snap caps / dummy rounds get listed in ammo categories on
            # Shadowsmith. Drop them before they pollute the skipped
            # count or end up as junk listings with imputed defaults.
            if is_non_ammo(name):
                skipped += 1
                print(f"  Skipped (not real ammo): {name[:55]}")
                continue

            # Listing price — same .price .woocommerce-Price-amount pair
            # as Velocity. The pre-fix regex on the whole card text was
            # vulnerable to "Sale!" badges and "$X off" promo overlays.
            price_el = product.query_selector('.price .woocommerce-Price-amount, span.price .amount')
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

            # Round count must come from the title — Shadowsmith doesn't
            # display CPR or surface variant grids on the category page.
            total_rounds = parse_rounds(name)
            if not total_rounds or total_rounds <= 0:
                skipped += 1
                print(f"  Skipped (no round count): {name[:55]}")
                continue

            price_per_round = round(base_price / total_rounds, 4)
            if not sanity_check_ppr(price_per_round, base_price, total_rounds,
                                    context=f'{RETAILER_SLUG} {caliber_norm}', caliber=caliber_norm):
                skipped += 1
                continue

            grain = parse_grain(name)
            case_material = parse_case_material(name)
            bullet_type = parse_bullet_type(name)
            country = parse_country(name)
            manufacturer = parse_brand_with_url(name, product_url) or "Unknown"
            product_id = product_url.rstrip('/').split('/')[-1]
            if not product_id or product_id in seen_ids:
                continue
            seen_ids.add(product_id)

            # Stock status: Shadowsmith adds an "outofstock" class to
            # the <li> wrapper for sold-out products (price still
            # renders, unlike Velocity). Read the wrapper's class list
            # directly instead of grepping inner_text.
            wrapper_class = product.get_attribute('class') or ''
            in_stock = ('outofstock' not in wrapper_class)
            card_text = product.inner_text() or ''
            purchase_limit = parse_purchase_limit(card_text)

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
            print(f"  Saved [{caliber_norm}]: {name[:55]} | ${base_price} | {price_per_round}/rd")

        except Exception as e:
            skipped += 1
            print(f"  Skipped: {e}")
            continue

    return saved, skipped, False


def scrape_caliber(page, caliber_norm, caliber_display, retailer_id, seen_ids, empty_handles):
    """Scrape every configured category URL for a caliber (always a list).
    Appends (caliber, url) to empty_handles for any URL whose first page
    rendered zero product cards."""
    saved = 0
    skipped = 0
    for entry in CALIBER_PATHS[caliber_norm]:
        s, k, empty = scrape_category_page(page, entry, caliber_norm,
                                           caliber_display, retailer_id, seen_ids)
        saved += s
        skipped += k
        if empty:
            empty_handles.append((caliber_norm, entry['url']))
    return saved, skipped


def scrape():
    print(f"[{datetime.now()}] Starting Shadowsmith Ammo scraper (all calibers)...")
    retailer_id = get_retailer_id()
    if not retailer_id:
        return

    print(f"Retailer ID: {retailer_id}")

    total_saved = 0
    total_skipped = 0
    seen_ids = set()
    empty_handles = []  # (caliber, url) for the storefront-drift guardrail

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })

        for caliber_norm in CALIBER_PATHS:
            caliber_display = CALIBERS[caliber_norm]
            saved, skipped = scrape_caliber(page, caliber_norm, caliber_display, retailer_id, seen_ids, empty_handles)
            total_saved += saved
            total_skipped += skipped

        browser.close()

    # Storefront-drift guardrail + freshness honesty (NEW 2026-06-14).
    report_empty_first_pages(empty_handles, 'Shadowsmith Ammo')
    mark_retailer_scraped(supabase, retailer_id, had_success=(total_saved > 0))
    print(f"\nDone! Saved: {total_saved} | Skipped: {total_skipped}")

if __name__ == '__main__':
    scrape()
