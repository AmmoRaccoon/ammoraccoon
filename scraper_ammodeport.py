import os
import re
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")

print(f"URL present: {bool(SUPABASE_URL)}")
print(f"KEY present: {bool(SUPABASE_KEY)}")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "ammunition-depot"
BASE_URL = "https://www.ammunitiondepot.com/ammo/9mm/?sort=price-asc&limit=96"

def get_retailer_id():
    result = supabase.table("retailers").select("id").eq("slug", RETAILER_SLUG).execute()
    return result.data[0]["id"]

def parse_grain(text):
    match = re.search(r'(\d+)\s*gr(?:ain)?', text, re.IGNORECASE)
    return int(match.group(1)) if match else None

def parse_rounds(text):
    patterns = [
        r'(\d[\d,]*)\s*rounds?',
        r'(\d[\d,]*)\s*rd',
        r'(\d[\d,]*)\s*count',
        r'\((\d[\d,]*)\s*rounds?\)',
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
    if 'FULL METAL' in text_upper:
        return 'FMJ'
    return None

def parse_country(text):
    text_lower = text.lower()
    mapping = {
        'federal': 'USA',
        'winchester': 'USA',
        'remington': 'USA',
        'cci': 'USA',
        'speer': 'USA',
        'hornady': 'USA',
        'blazer': 'USA',
        'fiocchi': 'USA',
        'american eagle': 'USA',
        'magtech': 'Brazil',
        'cbc': 'Brazil',
        'ppu': 'Serbia',
        'prvi partizan': 'Serbia',
        'sellier': 'Czech Republic',
        'tula': 'Russia',
        'wolf': 'Russia',
        'aguila': 'Mexico',
    }
    for keyword, country in mapping.items():
        if keyword in text_lower:
            return country
    return None

def scrape():
    print(f"[{datetime.now()}] Starting Ammunition Depot scraper...")
    retailer_id = get_retailer_id()
    print(f"Retailer ID: {retailer_id}")

    listings_saved = 0
    listings_skipped = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })

        print(f"Loading: {BASE_URL}")
        page.goto(BASE_URL, wait_until='domcontentloaded', timeout=90000)
        time.sleep(8)

        products = page.query_selector_all('.product-item')
        print(f"Found {len(products)} products")

        for product in products:
            try:
                name_el = product.query_selector('a.product-item-link')
                if not name_el:
                    listings_skipped += 1
                    continue
                name = name_el.inner_text().strip()
                product_url = name_el.get_attribute('href')

                price_el = product.query_selector('span.rounds-price')
                if not price_el:
                    listings_skipped += 1
                    continue

                price_text = price_el.inner_text().strip()
                price_matches = re.findall(r'\$(\d+\.\d+)', price_text)
                if not price_matches:
                    listings_skipped += 1
                    continue

                cpr = float(price_matches[0])
                total_rounds = parse_rounds(name)
                if not total_rounds or total_rounds <= 0:
                    listings_skipped += 1
                    continue

                base_price = round(cpr * total_rounds, 2)
                price_per_round = cpr
                grain = parse_grain(name)
                case_material = parse_case_material(name)
                bullet_type = parse_bullet_type(name)
                country = parse_country(name)
                product_id = product_url.split('/')[-1].replace('.html', '') if product_url else name[:50]

                listing = {
                    'retailer_id': retailer_id,
                    'retailer_product_id': product_id,
                    'product_url': product_url,
                    'caliber': '9mm Luger',
                    'caliber_normalized': '9mm',
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
                    'last_updated': datetime.now(timezone.utc).isoformat(),
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

                listings_saved += 1
                print(f"  Saved: {name[:60]} | ${base_price} | {price_per_round}c/rd | {total_rounds}rds | {case_material}")

            except Exception as e:
                listings_skipped += 1
                print(f"  Skipped: {e}")
                continue

        browser.close()

    print(f"\nDone! Saved: {listings_saved} | Skipped: {listings_skipped}")

if __name__ == '__main__':
    scrape()