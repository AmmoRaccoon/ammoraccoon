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

RETAILER_SLUG = "blackbasin"
SITE_BASE = "https://blackbasin.com"

# BigCommerce stencil. Verified 2026-04-25 against the live homepage
# nav. Black Basin's slug pattern is INCONSISTENT — some calibers
# carry a leading dot, some don't, and they use abbreviated/expanded
# names interchangeably ("357-mag" not "magnum"; "40-smith-wesson"
# not "40-s-w"; "380-auto" not "380-acp"; "22-long-rifle" not "22-lr"
# in the rimfire path; "300-aac-blackout" without leading dot).
CALIBER_PATHS = {
    '9mm':     '/handgun-ammo/9mm/',
    '380acp':  '/handgun-ammo/.380-auto/',
    '40sw':    '/handgun-ammo/.40-smith-wesson/',
    '38spl':   '/handgun-ammo/.38-special/',
    '357mag':  '/handgun-ammo/.357-mag/',
    '22lr':    '/rimfire-ammo/22-long-rifle/',
    '223-556': '/rifle-ammo/.223-remington/',
    '308win':  '/rifle-ammo/7.62x51-.308/',
    '762x39':  '/rifle-ammo/7.62x39/',
    '300blk':  '/rifle-ammo/300-aac-blackout/',
}

def get_retailer_id():
    result = supabase.table("retailers").select("id").eq("slug", RETAILER_SLUG).execute()
    if not result.data:
        print(f"ERROR: Retailer '{RETAILER_SLUG}' not found in database")
        return None
    return result.data[0]["id"]


def fetch_smallest_variant(page, product_url):
    """Visit a Black Basin product page and pull the smallest variant
    from its option grid. BB renders variants as a QTY/PRICE/PRICE-PER-
    ROUND table:

        <div class="option-grid-value">
          <input type="radio" data-product-attribute-value="N">
          <label>50</label>
          <span class="option-grid-value--price">$24.07</span>
          <span class="option-grid-value--ppr">$0.48</span>
        </div>

    Returns (smallest_qty, smallest_qty_price) or (None, None) when no
    variant grid is present. The category-page tile shows the lowest
    variant's price already, but reading the grid lets us record the
    EXACT round count too (titles carry no count on Black Basin).
    """
    try:
        resp = page.goto(product_url, wait_until='domcontentloaded', timeout=60000)
    except Exception:
        return None, None
    if resp and resp.status >= 400:
        return None, None
    rows = page.query_selector_all('.option-grid-value')
    best_qty = None
    best_price = None
    for row in rows:
        label = row.query_selector('label.form-label[data-product-attribute-value]')
        price_el = row.query_selector('.option-grid-value--price')
        if not label or not price_el:
            continue
        qty_text = (label.inner_text() or '').strip()
        m = re.search(r'(\d[\d,]*)', qty_text)
        if not m:
            continue
        qty = int(m.group(1).replace(',', ''))
        price_text = (price_el.inner_text() or '').strip()
        pm = re.search(r'\$?(\d{1,5}(?:,\d{3})*(?:\.\d{1,2})?)', price_text)
        if not pm:
            continue
        price = float(pm.group(1).replace(',', ''))
        if best_qty is None or qty < best_qty:
            best_qty = qty
            best_price = price
    return best_qty, best_price

def parse_grain(text):
    match = re.search(r'(\d+)[\s-]*gr(?:ain)?', text, re.IGNORECASE)
    return int(match.group(1)) if match else None

def parse_rounds(text):
    patterns = [
        r'(\d[\d,]*)\s*rounds?',
        r'(\d[\d,]*)\s*rds?\b',
        r'(\d[\d,]*)\s*count',
        r'(\d[\d,]*)\s*per\s*box',
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
        'wolf': 'Russia', 'aguila': 'Mexico',
    }
    for keyword, country in mapping.items():
        if keyword in text_lower:
            return country
    return None

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

    # Scope to the main product grid. Pre-fix selector matched 100
    # cards including 50 phantom carousel items; .productGrid scopes
    # to the 50 real products.
    products = page.query_selector_all('.productGrid article.card')
    if not products:
        products = page.query_selector_all('ul.productGrid li.product')
    print(f"  Found {len(products)} products")
    if not products:
        return 0, 0

    # Two-pass loop — collect URL/title from category page, then visit
    # each product page for the QTY/PRICE/PRICE-PER-ROUND grid. Black
    # Basin titles never include round count and the category-page
    # price is a "$X.XX - $Y.YY" range, so the existing single-pass
    # extractor silently dropped every product.
    candidates = []
    skipped = 0
    for product in products:
        try:
            link_el = product.query_selector('h3.card-title a, h4.card-title a, .card-title a')
            if not link_el:
                skipped += 1
                continue
            href = link_el.get_attribute('href') or ''
            if not href:
                skipped += 1
                continue
            product_url = href if href.startswith('http') else SITE_BASE + href
            if '/brands/' in product_url or '/categories/' in product_url:
                skipped += 1
                continue
            name = (link_el.inner_text() or '').strip() or (link_el.get_attribute('title') or '').split(',')[0].strip()
            if not name:
                skipped += 1
                continue
            # data-name on the article is cleaner than scraping h3 text
            # which sometimes wraps mid-word with extra whitespace.
            data_name = product.get_attribute('data-name')
            if data_name:
                name = data_name.strip()
            candidates.append((product_url, name))
        except Exception as e:
            skipped += 1
            print(f"  Skipped (card parse): {e}")
            continue

    # Black Basin lists ~100 products per caliber. Visiting every
    # product page for the variant grid would push the per-caliber
    # runtime to several minutes; cap at 30 so the full scrape fits
    # in the 2-hour cron window. Sort by price-low-first so we cover
    # the most common (and cheapest) SKUs every cycle. The cap can be
    # raised once we move scraping to a worker with longer budgets.
    BLACK_BASIN_PER_CALIBER_CAP = 30
    candidates = candidates[:BLACK_BASIN_PER_CALIBER_CAP]

    saved = 0
    for product_url, name in candidates:
        try:
            # Smallest variant from the QTY grid — Black Basin is the
            # only retailer that publishes per-round price directly per
            # variant. We pair the smallest variant's QTY with its
            # price (not the category-page lowest price, since the two
            # always agree here but reading from the grid is unambiguous).
            total_rounds, base_price = fetch_smallest_variant(page, product_url)
            if not total_rounds or not base_price:
                # Fallback: title parsing for the rare single-variant SKU
                # whose option grid is replaced by a plain price block.
                total_rounds = parse_rounds(name)
                if not total_rounds:
                    skipped += 1
                    print(f"  Skipped (no variant grid + no round count): {name[:55]}")
                    continue
                # No price either — last resort: try the page's main
                # price element after the goto in fetch_smallest_variant.
                price_el = page.query_selector('[data-product-price-without-tax]')
                if price_el:
                    pt = (price_el.inner_text() or '').strip()
                    pm = re.search(r'\$(\d{1,5}(?:,\d{3})*(?:\.\d{1,2})?)', pt)
                    if pm:
                        base_price = float(pm.group(1).replace(',', ''))
                if not base_price or base_price <= 0:
                    skipped += 1
                    print(f"  Skipped (no price): {name[:55]}")
                    continue
            if total_rounds <= 0 or base_price <= 0:
                skipped += 1
                continue

            price_per_round = round(base_price / total_rounds, 4)
            if not sanity_check_ppr(price_per_round, base_price, total_rounds,
                                    context=f'{RETAILER_SLUG} {caliber_norm}'):
                skipped += 1
                continue

            grain = parse_grain(name)
            case_material = parse_case_material(name)
            bullet_type = parse_bullet_type(name)
            country = parse_country(name)
            manufacturer = parse_brand(name) or "Unknown"
            product_id = product_url.rstrip('/').split('/')[-1]
            if not product_id or product_id in seen_ids:
                continue
            seen_ids.add(product_id)

            # We're sitting on the product page after fetch_smallest_variant.
            # Read stock + purchase-limit from its body text.
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
    print(f"[{datetime.now()}] Starting Black Basin scraper (all calibers)...")
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
