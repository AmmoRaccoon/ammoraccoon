import os
import re
import time
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

from scraper_lib import (
    CALIBERS, now_iso, with_stock_fields, parse_purchase_limit,
    parse_brand, sanity_check_ppr, clean_title, parse_bullet_type,
)

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "south-georgia-outdoors"
SITE_BASE = "https://www.sgogunsandammo.com"

# Volusion store. Categories are exposed as numeric IDs at /category-s/<id>.htm.
# Verified 2026-04-25 by fetching each path and reading <title>. Inventory is
# small (typically <20 products per caliber, fits on one page) so no pagination.
CALIBER_PATHS = {
    '9mm':     '/category-s/1845.htm',
    '380acp':  '/category-s/1831.htm',
    '40sw':    '/category-s/1832.htm',
    '38spl':   '/category-s/1829.htm',
    '357mag':  '/category-s/1827.htm',
    '22lr':    '/category-s/1912.htm',
    '223-556': '/category-s/1856.htm',
    '308win':  '/category-s/1883.htm',
    '762x39':  '/category-s/1905.htm',
    '300blk':  '/category-s/1876.htm',
}


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
    # SGO titles use hyphenated forms ("20-ROUND BOX", "50-RD") and
    # the rimfire abbreviation "RND" alongside the standard "RD"/"ROUNDS"
    # spellings. Allow both " " and "-" between the number and the unit.
    patterns = [
        r'(\d[\d,]*)\s*[- ]?\s*rounds?\b',
        r'(\d[\d,]*)\s*[- ]?\s*rn?ds?\b',
        r'(\d[\d,]*)\s*round\s*(?:box|case|pack)',
        r'(\d[\d,]*)\s*[- ]?\s*count\b',
        r'(\d[\d,]*)\s*[- ]?\s*ct\b',
        r'(\d[\d,]*)\s*per\s*box',
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
    }
    for keyword, country in mapping.items():
        if keyword in text_lower:
            return country
    return None


def scrape_caliber(page, caliber_norm, caliber_display, retailer_id, seen_ids):
    url = SITE_BASE + CALIBER_PATHS[caliber_norm]
    print(f"\n[{caliber_norm}] Loading: {url}")
    try:
        resp = page.goto(url, wait_until='domcontentloaded', timeout=60000)
    except Exception as e:
        print(f"  goto failed: {e}")
        return 0, 0
    if resp and resp.status >= 400:
        print(f"  HTTP {resp.status} - skipping caliber.")
        return 0, 0
    time.sleep(3)

    # Volusion's Fuego template wraps each row of products in
    # <table class="v65-productDisplay">, with one product per <td>
    # column. Scope to the wrapper table so sidebar promo blocks
    # (also <td>'d) don't get treated as products.
    cards = page.query_selector_all('table.v65-productDisplay td:has(a.productnamecolor)')
    print(f"  Found {len(cards)} product cells")
    if not cards:
        return 0, 0

    saved = 0
    skipped = 0

    for card in cards:
        try:
            # Title link — Volusion gives every product card the same
            # .productnamecolor class on its name <a>; the visible name
            # is in the inner <span itemprop="name">.
            link_el = card.query_selector('a.productnamecolor')
            if not link_el:
                skipped += 1
                continue
            href = link_el.get_attribute('href') or ''
            if not href:
                skipped += 1
                continue
            product_url = href if href.startswith('http') else SITE_BASE + href

            # Skip brand-carousel cards — same defensive filter the
            # BigCommerce-stencil scrapers use.
            if '/brands/' in product_url:
                skipped += 1
                continue

            name_el = link_el.query_selector('span[itemprop="name"]') or link_el
            raw_name = (name_el.inner_text() or '').strip()
            name = clean_title(raw_name)
            if not name:
                skipped += 1
                continue

            # Price lives in a nested <font> inside .product_productprice
            # as plain text "Our Price: $XX.XX". Other amounts on the
            # card (compare-at, "You save") would confuse a card-text
            # regex, so anchor to the priced wrapper.
            price_wrapper = card.query_selector('.product_productprice')
            if not price_wrapper:
                skipped += 1
                continue
            price_text = (price_wrapper.inner_text() or '').strip()
            m = re.search(r'\$\s*(\d{1,4}(?:,\d{3})*(?:\.\d{1,2})?)', price_text)
            if not m:
                skipped += 1
                continue
            base_price = float(m.group(1).replace(',', ''))
            if base_price <= 0:
                skipped += 1
                continue

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

            # Stock copy on Volusion: "<N> in stock!" when available,
            # "(Out of Stock)" otherwise. Read the whole card text — both
            # markers sit outside the priced wrapper.
            card_text = card.inner_text() or ''
            card_lower = card_text.lower()
            in_stock = 'in stock' in card_lower and 'out of stock' not in card_lower
            purchase_limit = parse_purchase_limit(card_text)

            grain = parse_grain(name)
            case_material = parse_case_material(name)
            bullet_type = parse_bullet_type(name)
            country = parse_country(name)
            manufacturer = parse_brand(name) or "Unknown"
            # Volusion product URLs end in /product-p/<sku>.htm — the
            # SKU is the natural retailer_product_id.
            slug_match = re.search(r'/product-p/([^/.]+)\.htm', product_url)
            product_id = slug_match.group(1) if slug_match else product_url.rstrip('/').split('/')[-1]
            product_id = product_id[:100]
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
            print(f"  Saved [{caliber_norm}]: {name[:55]} | ${base_price} | {price_per_round}/rd")

        except Exception as e:
            skipped += 1
            print(f"  Skipped: {e}")
            continue

    return saved, skipped


def scrape():
    print(f"[{datetime.now()}] Starting South Georgia Outdoors scraper (all calibers)...")
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
