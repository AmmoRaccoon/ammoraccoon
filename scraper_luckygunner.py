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

RETAILER_SLUG = "lucky-gunner"
BASE_URL = "https://www.luckygunner.com/handgun/9mm-ammo?show=100"

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
    match = re.search(r'(\d[\d,]*)\s*rounds?', text, re.IGNORECASE)
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
        'magtech': 'Brazil', 'cbc': 'Brazil',
        'ppu': 'Serbia', 'prvi partizan': 'Serbia',
        'sellier': 'Czech Republic', 'tula': 'Russia',
        'wolf': 'Russia', 'aguila': 'Mexico', 'sterling': 'Turkey',
        'bvac': 'USA',
    }
    for keyword, country in mapping.items():
        if keyword in text_lower:
            return country
    return None

def scrape():
    print(f"[{datetime.now()}] Starting Lucky Gunner scraper...")
    retailer_id = get_retailer_id()
    if not retailer_id:
        return

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

        products = page.query_selector_all('.ammo-list-container li, li.ammo-list-item, .products-list li')
        
        if not products:
            products = page.query_selector_all('li[class*="ammo"], div[class*="ammo-item"]')

        if not products:
            # fallback - find by price class
            products = page.query_selector_all('.cprc')
            print(f"Found {len(products)} price elements - using fallback")

        print(f"Found {len(products)} products")

        # If still nothing use a different approach - parse the whole page
        if not products:
            print("Trying full page parse...")
            html = page.content()
            
        # Better approach - get all product containers
        products = page.query_selector_all('li.item')
        if not products:
            products = page.query_selector_all('.product-item, li[class*="item"]')
        
        print(f"Found {len(products)} products (second pass)")

        for product in products:
            try:
                # Name
                name_el = product.query_selector('h2 a, h3 a, .product-name a, a.product-name')
                if not name_el:
                    listings_skipped += 1
                    continue

                name = name_el.inner_text().strip()
                product_url = name_el.get_attribute('href')

                # Price
                price_el = product.query_selector('.price, [class*="price"]')
                if not price_el:
                    listings_skipped += 1
                    continue

                price_text = price_el.inner_text().strip()
                price_matches = re.findall(r'\$(\d+\.?\d*)', price_text)
                if not price_matches:
                    listings_skipped += 1
                    continue

                base_price = float(price_matches[-1])  # take last price (sale price)

                # CPR
                cpr_el = product.query_selector('.cprc')
                if cpr_el:
                    cpr_text = cpr_el.inner_text().strip()
                    cpr_match = re.search(r'(\d+\.?\d*)¢', cpr_text)
                    if cpr_match:
                        price_per_round = float(cpr_match.group(1)) / 100
                    else:
                        price_per_round = None
                else:
                    price_per_round = None

                total_rounds = parse_rounds(name)
                if not total_rounds or total_rounds <= 0:
                    listings_skipped += 1
                    continue

                if not price_per_round:
                    price_per_round = round(base_price / total_rounds, 4)

                grain = parse_grain(name)
                case_material = parse_case_material(name)
                bullet_type = parse_bullet_type(name)
                country = parse_country(name)
                product_id = product_url.split('/')[-1] if product_url else name[:50]

                # Stock
                stock_el = product.query_selector('.in-stock, .availability')
                in_stock = True
                if stock_el:
                    in_stock = 'in stock' in stock_el.inner_text().lower()

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
                    'in_stock': in_stock,
                    'stock_level': 'In Stock' if in_stock else 'Out of Stock',
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
                    'in_stock': in_stock,
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