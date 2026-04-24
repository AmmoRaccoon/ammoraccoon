import os
import re
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

from scraper_lib import CALIBERS, now_iso

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "target-sports"
SITE_BASE = "https://www.targetsportsusa.com"

CALIBER_PATHS = {
    '9mm':     '/9mm-luger-ammo-c-51.aspx?pp=240&SortOrder=PriceAscending',
    '380acp':  '/380-acp-ammo-c-49.aspx?pp=240&SortOrder=PriceAscending',
    '40sw':    '/40-sw-ammo-c-52.aspx?pp=240&SortOrder=PriceAscending',
    '38spl':   '/38-special-ammo-c-48.aspx?pp=240&SortOrder=PriceAscending',
    '357mag':  '/357-magnum-ammo-c-47.aspx?pp=240&SortOrder=PriceAscending',
    '22lr':    '/22-lr-ammo-c-65.aspx?pp=240&SortOrder=PriceAscending',
    '223-556': '/223-rem-5-56-nato-ammo-c-66.aspx?pp=240&SortOrder=PriceAscending',
    '308win':  '/308-win-7-62x51-ammo-c-67.aspx?pp=240&SortOrder=PriceAscending',
    '762x39':  '/7-62x39-ammo-c-71.aspx?pp=240&SortOrder=PriceAscending',
    '300blk':  '/300-aac-blackout-ammo-c-69.aspx?pp=240&SortOrder=PriceAscending',
}

def get_retailer_id():
    result = supabase.table("retailers").select("id").eq("slug", RETAILER_SLUG).execute()
    if not result.data:
        print(f"ERROR: Retailer '{RETAILER_SLUG}' not found in database")
        return None
    return result.data[0]["id"]

def parse_grain(text):
    match = re.search(r'(\d+)\s*gr(?:ain)?', text, re.IGNORECASE)
    return int(match.group(1)) if match else None

def parse_rounds(text):
    patterns = [
        r'(\d[\d,]*)\s*rounds?',
        r'(\d[\d,]*)\s*rds',
        r'(\d[\d,]*)/box',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return int(match.group(1).replace(',', ''))
    return None

def parse_case_material(text):
    text_lower = text.lower()
    steel_brands = ['wolf', 'tula', 'tulammo', 'brown bear', 'silver bear', 'golden bear']
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
    if 'FULL METAL JACKET' in text_upper:
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
        'wolf': 'Russia', 'aguila': 'Mexico', 'pmc': 'South Korea',
        'geco': 'Germany', 'lapua': 'Finland', 'norma': 'Sweden',
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
    print("  Waiting for products to load...")
    time.sleep(20)

    products = page.query_selector_all('li a[href*="-p-"]')
    print(f"  Found {len(products)} candidate links")
    if not products:
        return 0, 0

    saved = 0
    skipped = 0

    for product in products:
        try:
            product_url = product.get_attribute('href')
            if not product_url:
                continue

            if not product_url.startswith('http'):
                product_url = SITE_BASE + product_url

            text = product.inner_text().strip()
            if not text:
                continue

            name_el = product.query_selector('h2')
            if not name_el:
                skipped += 1
                continue
            name = name_el.inner_text().strip()

            cpr_match = re.search(r'\$(\d+\.\d+)\s*Per\s*Round', text, re.IGNORECASE)
            if not cpr_match:
                skipped += 1
                continue
            price_per_round = float(cpr_match.group(1))

            price_matches = re.findall(r'\$(\d+\.?\d*)', text)
            prices = [float(p) for p in price_matches if float(p) > 1]
            if not prices:
                skipped += 1
                continue
            base_price = min(prices)

            total_rounds = parse_rounds(name)
            if not total_rounds and price_per_round > 0:
                total_rounds = round(base_price / price_per_round)
            if not total_rounds:
                skipped += 1
                continue

            grain = parse_grain(name)
            case_material = parse_case_material(name)
            bullet_type = parse_bullet_type(name)
            country = parse_country(name)
            product_id = product_url.split('/')[-1].replace('.aspx', '')
            if product_id in seen_ids:
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
                'rounds_per_box': total_rounds,
                'boxes_per_case': 1,
                'total_rounds': total_rounds,
                'base_price': base_price,
                'price_per_round': price_per_round,
                'in_stock': True,
                'stock_level': 'In Stock',
                'last_updated': now_iso(),
            }

            result = supabase.table('listings').upsert(
                listing,
                on_conflict='retailer_id,retailer_product_id'
            ).execute()

            supabase.table('price_history').insert({
                'listing_id': result.data[0]['id'],
                'price': base_price,
                'price_per_round': price_per_round,
                'in_stock': True,
            }).execute()

            saved += 1
            print(f"  Saved [{caliber_norm}]: {name[:55]} | ${base_price} | {price_per_round}/rd")

        except Exception as e:
            skipped += 1
            print(f"  Skipped: {e}")
            continue

    return saved, skipped


def scrape():
    print(f"[{datetime.now()}] Starting Target Sports USA scraper (all calibers)...")
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
