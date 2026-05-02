import os
import re
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

from scraper_lib import (
    CALIBERS, now_iso, with_stock_fields, parse_purchase_limit,
    parse_brand, sanity_check_ppr, parse_bullet_type,
)

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "bulkmunitions"
SITE_BASE = "https://www.bulkmunitions.com"

# WordPress/WooCommerce. Categories use a flat /handgun|rifle|rimfire/
# parent with caliber slug appended.
CALIBER_PATHS = {
    '9mm':     '/handgun/9mm-ammo/',
    '380acp':  '/handgun/380-acp-ammo/',
    '40sw':    '/handgun/40-s-w-ammo/',
    '38spl':   '/handgun/38-special-ammo/',
    '357mag':  '/handgun/357-magnum-ammo/',
    '22lr':    '/rimfire/22-long-rifle-ammo/',
    '223-556': '/rifle/223-rem-556x45-ammo/',
    '308win':  '/rifle/308-win-ammo/',
    '762x39':  '/rifle/762x39-ammo/',
    '300blk':  '/rifle/300-aac-blackout-ammo/',
}

def get_retailer_id():
    result = supabase.table("retailers").select("id").eq("slug", RETAILER_SLUG).execute()
    if not result.data:
        print(f"ERROR: Retailer '{RETAILER_SLUG}' not found in database")
        return None
    return result.data[0]["id"]

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

    products = page.query_selector_all('li.product, ul.products li, [class*="product-item"]')
    if not products:
        products = page.query_selector_all('[class*="product"]')
    print(f"  Found {len(products)} products")
    if not products:
        return 0, 0

    saved = 0
    skipped = 0

    for product in products:
        try:
            link_el = product.query_selector('a.woocommerce-LoopProduct-link, h2.woocommerce-loop-product__title, h3 a, h2 a, a[href*="/product/"]')
            if not link_el:
                skipped += 1
                continue

            href = link_el.get_attribute('href') or ''
            title_el = product.query_selector('.woocommerce-loop-product__title, h2, h3')
            name = (title_el.inner_text().strip() if title_el else '') or link_el.inner_text().strip()
            if not name or not href:
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

            card_text = product.inner_text()

            price_matches = re.findall(r'\$(\d{1,4}(?:,\d{3})*(?:\.\d{1,2})?)', card_text)
            if len(price_matches) >= 2 and price_matches[0] != price_matches[1]:
                skipped += 1
                continue
            if not price_matches:
                skipped += 1
                continue
            base_price = float(price_matches[0].replace(',', ''))
            if base_price <= 0:
                skipped += 1
                continue

            total_rounds = parse_rounds(name) or parse_rounds(card_text)
            if not total_rounds or total_rounds <= 0:
                skipped += 1
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
            manufacturer = parse_brand(name) or "Unknown"
            product_id = product_url.rstrip('/').split('/')[-1]
            if not product_id or product_id in seen_ids:
                continue
            seen_ids.add(product_id)

            card_lower = card_text.lower()
            in_stock = ('out of stock' not in card_lower and
                        'sold out' not in card_lower and
                        'unavailable' not in card_lower)
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
    print(f"[{datetime.now()}] Starting Bulk Munitions scraper (all calibers)...")
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
