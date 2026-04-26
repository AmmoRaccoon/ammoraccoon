import os
import re
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

from scraper_lib import (
    CALIBERS, now_iso, with_stock_fields, parse_purchase_limit,
    parse_brand, sanity_check_ppr,
)

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "georgiaarms"
SITE_BASE = "https://www.georgia-arms.com"

# BigCommerce. Slugs use trailing -1 disambiguators on most categories
# (Georgia Arms re-imported their catalog at some point and the legacy
# slugs collided). Verified 2026-04-25 against the live homepage nav:
# .22 LR is /22-long-rifle/ (was /22-lr/ — 404), and 7.62x39 sits at
# /7-62x39-1/ but currently has zero in-stock product.
CALIBER_PATHS = {
    '9mm':     '/9mm-luger-1/',
    '380acp':  '/380-acp-1/',
    '40sw':    '/40-s-w-1/',
    '38spl':   '/38-special-1/',
    '357mag':  '/357-mag-1/',
    '22lr':    '/22-long-rifle/',
    '223-556': '/223-rem-1/',
    '308win':  '/308-win-1/',
    '762x39':  '/7-62x39-1/',
    '300blk':  '/300-blackout/',
}

def get_retailer_id():
    result = supabase.table("retailers").select("id").eq("slug", RETAILER_SLUG).execute()
    if not result.data:
        print(f"ERROR: Retailer '{RETAILER_SLUG}' not found in database")
        return None
    return result.data[0]["id"]


def fetch_smallest_variant_rounds(page, product_url):
    """Visit a Georgia Arms product page and read its variant radio
    grid to recover the smallest pack size. Their BigCommerce stencil
    renders variants as:
        <label data-product-attribute-value="N">100pk</label>
        <label data-product-attribute-value="N">1000pk (10 - 100pk bags)</label>
    The category page only shows the in-stock variant's price (others
    are class="form-label unavailable"), and that is almost always the
    smallest pack size — pair that price with min(pack_count) and the
    listing's per-round math is correct.

    Returns None when no variant labels parse — caller should fall
    back to the title parser (a few SKUs encode the count there).
    """
    try:
        resp = page.goto(product_url, wait_until='domcontentloaded', timeout=60000)
    except Exception:
        return None
    if resp and resp.status >= 400:
        return None
    labels = page.query_selector_all('label.form-label[data-product-attribute-value]')
    counts = []
    for label in labels:
        text = (label.inner_text() or '').strip()
        # Handle the most common encodings: "100pk", "100 pk",
        # "100rd Box", "Box of 100", "100 Round Box".
        m = (re.search(r'(\d[\d,]*)\s*pk\b', text, re.IGNORECASE)
             or re.search(r'(\d[\d,]*)\s*rd[s]?\b', text, re.IGNORECASE)
             or re.search(r'(\d[\d,]*)\s*rounds?\b', text, re.IGNORECASE)
             or re.search(r'\bbox\s+of\s+(\d[\d,]*)\b', text, re.IGNORECASE))
        if m:
            n = int(m.group(1).replace(',', ''))
            if 5 <= n <= 10000:
                counts.append(n)
    return min(counts) if counts else None

def parse_grain(text):
    match = re.search(r'(\d+)[\s-]*gr(?:ain)?', text, re.IGNORECASE)
    return int(match.group(1)) if match else None

def parse_rounds(text):
    patterns = [
        r'(\d[\d,]*)\s*rounds?',
        r'(\d[\d,]*)\s*rds?\b',
        r'(\d[\d,]*)\s*count',
        r'(\d[\d,]*)\s*pk\b',
        r'-(\d[\d,]*)pk\b',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return int(match.group(1).replace(',', ''))
    return None

def parse_case_material(text):
    text_lower = text.lower()
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
    for bt in ['FMJ', 'JHP', 'HP', 'OTM', 'TMJ', 'SP', 'FP', 'WC']:
        if bt in text_upper:
            return bt
    if 'WADCUTTER' in text_upper:
        return 'WC'
    if 'HOLLOW POINT' in text_upper:
        return 'JHP'
    if 'FULL METAL' in text_upper:
        return 'FMJ'
    return None

def parse_country(text):
    return 'USA'  # Georgia Arms is a US manufacturer.

def scrape_caliber(page, caliber_norm, caliber_display, retailer_id, seen_ids):
    url = SITE_BASE + CALIBER_PATHS[caliber_norm]
    print(f"\n[{caliber_norm}] Loading: {url}")
    try:
        resp = page.goto(url, wait_until='domcontentloaded', timeout=90000)
    except Exception as e:
        print(f"  goto failed: {e}")
        return 0, 0
    if resp and resp.status >= 400:
        print(f"  HTTP {resp.status} - skipping caliber.")
        return 0, 0
    time.sleep(6)

    # Scope to the main grid wrapper. Pre-fix the broad selector
    # ('article.card, li.product, .productCard') matched carousel
    # cards from "Customers Also Viewed" / "Recently Viewed" rails
    # that don't carry round counts and silently dropped.
    products = page.query_selector_all('.productGrid li.product')
    if not products:
        products = page.query_selector_all('ul.productGrid li.product')
    print(f"  Found {len(products)} products")
    if not products:
        return 0, 0

    # Collect URL + title + price from the category page first; visit
    # each product page in a second pass for the variant grid. Two
    # passes keeps element handles from going stale when we navigate
    # away to a product detail page.
    candidates = []
    skipped = 0
    for product in products:
        try:
            link_el = product.query_selector('h4.card-title a, .card-title a, h3 a')
            if not link_el:
                skipped += 1
                continue
            href = link_el.get_attribute('href') or ''
            if not href:
                skipped += 1
                continue
            product_url = href if href.startswith('http') else SITE_BASE + href
            if '/brands/' in product_url:
                skipped += 1
                continue
            name = (link_el.inner_text() or '').strip() or (link_el.get_attribute('title') or '')

            # Real price — BigCommerce stencil renders it in
            # <span data-product-price-without-tax>. Only the in-stock
            # variant's price is shown; other variants get the same
            # span value or are flagged "unavailable" on the product
            # detail page.
            price_el = product.query_selector('[data-product-price-without-tax]')
            if not price_el:
                price_el = product.query_selector('.price.price--withoutTax')
            if not price_el:
                skipped += 1
                continue
            price_text = (price_el.inner_text() or '').strip()
            m = re.search(r'\$(\d{1,4}(?:,\d{3})*(?:\.\d{1,2})?)', price_text)
            if not m:
                skipped += 1
                continue
            base_price = float(m.group(1).replace(',', ''))
            if base_price <= 0:
                skipped += 1
                continue
            candidates.append((product_url, name, base_price))
        except Exception as e:
            skipped += 1
            print(f"  Skipped (card parse): {e}")
            continue

    saved = 0
    for product_url, name, base_price in candidates:
        try:
            # Round count: most Georgia Arms titles have NO round count
            # (e.g. "9MM Luger 115gr Full Metal Jacket"), so the title
            # parser silently dropped the entire run. Real source of
            # truth is the variant radio grid on each product page —
            # labels like "100pk" / "1000pk (Canned Heat)" — paired
            # with the lowest displayed price (always the smallest pack).
            total_rounds = fetch_smallest_variant_rounds(page, product_url)
            if not total_rounds:
                # Fallback for SKUs that bake the count into the title
                # (e.g. "...20rd Box", "...20pk").
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
            manufacturer = parse_brand(name) or "Georgia Arms"  # house brand default
            product_id = product_url.rstrip('/').split('/')[-1]
            if not product_id or product_id in seen_ids:
                continue
            seen_ids.add(product_id)

            # We're sitting on the product page after the variant
            # fetch, so read stock state and purchase-limit hints
            # straight from its body. Category-page card text isn't
            # available in this pass.
            page_text = (page.locator('body').inner_text() or '').lower()
            in_stock = ('out of stock' not in page_text and
                        'sold out' not in page_text and
                        'currently unavailable' not in page_text)
            purchase_limit = parse_purchase_limit(page_text)

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
            print(f"  Saved [{caliber_norm}]: {name[:55]} | ${base_price} | {price_per_round}/rd")

        except Exception as e:
            skipped += 1
            print(f"  Skipped: {e}")
            continue

    return saved, skipped


def scrape():
    print(f"[{datetime.now()}] Starting Georgia Arms scraper (all calibers)...")
    retailer_id = get_retailer_id()
    if not retailer_id:
        return

    print(f"Retailer ID: {retailer_id}")

    total_saved = 0
    total_skipped = 0
    seen_ids = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })

        for caliber_norm in CALIBER_PATHS:
            caliber_display = CALIBERS[caliber_norm]
            saved, skipped = scrape_caliber(page, caliber_norm, caliber_display, retailer_id, seen_ids)
            total_saved += saved
            total_skipped += skipped

        browser.close()

    print(f"\nDone! Saved: {total_saved} | Skipped: {total_skipped}")

if __name__ == '__main__':
    scrape()
