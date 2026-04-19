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
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "academy"
BASE_URL = "https://www.academy.com/c/shops/9mm-shop/9mm-ammunition"

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
        r'(\d[\d,]*)\s*rd',
        r'(\d[\d,]*)\s*count',
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
        'monarch': 'USA', 'magtech': 'Brazil', 'cbc': 'Brazil',
        'ppu': 'Serbia', 'prvi partizan': 'Serbia',
        'sellier': 'Czech Republic', 'tula': 'Russia',
        'wolf': 'Russia', 'aguila': 'Mexico',
    }
    for keyword, country in mapping.items():
        if keyword in text_lower:
            return country
    return None

def scrape():
    print(f"[{datetime.now()}] Starting Academy Sports scraper...")
    retailer_id = get_retailer_id()
    if not retailer_id:
        return

    print(f"Retailer ID: {retailer_id}")
    listings_saved = 0
    listings_skipped = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-blink-features=AutomationControlled']
        )
        context = browser.new_context(
            viewport={'width': 1280, 'height': 800},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            locale='en-US',
        )
        page = context.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        print(f"Loading: {BASE_URL}")
        page.goto(BASE_URL, wait_until='domcontentloaded', timeout=90000)
        time.sleep(10)

        products = page.query_selector_all('[class*="productCard"]')
        print(f"Found {len(products)} products")

        for product in products:
            try:
                # Name and URL
                name_el = product.query_selector('a[data-auid="product-title"]')
                if not name_el:
                    name_el = product.query_selector('a.title--HjbR1')
                if not name_el:
                    listings_skipped += 1
                    continue

                name = name_el.get_attribute('title') or name_el.inner_text().strip()
                product_url = 'https://www.academy.com' + name_el.get_attribute('href')

                # Price - look for dollar amount
                price_el = product.query_selector('[data-auid="product-price"], [class*="price"], [class*="Price"]')
                if not price_el:
                    listings_skipped += 1
                    continue

                price_text = price_el.inner_text().strip()
                price_matches = re.findall(r'\$(\d+\.?\d*)', price_text)
                if not price_matches:
                    listings_skipped += 1
                    continue

                # Take the first (lowest/sale) price
                base_price = float(price_matches[0])
                if base_price <= 0:
                    listings_skipped += 1
                    continue

                # Parse product details from name
                total_rounds = parse_rounds(name)
                if not total_rounds or total_rounds <= 0:
                    listings_skipped += 1
                    continue

                price_per_round = round(base_price / total_rounds, 4)
                grain = parse_grain(name)
                case_material = parse_case_material(name)
                bullet_type = parse_bullet_type(name)
                country = parse_country(name)
                product_id = name_el.get_attribute('href').split('/')[-1]

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