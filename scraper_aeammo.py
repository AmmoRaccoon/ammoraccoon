import asyncio
import os
import re
from datetime import datetime, timezone
from playwright.async_api import async_playwright
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
RETAILER_ID = 11
BASE_URL = "https://aeammo.com/Ammo/Handgun-Ammo/9mm-Ammo"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def parse_rounds(title):
    multi = re.search(r'(\d+)/box\s*\[(\d+)\s*boxes?\]', title, re.IGNORECASE)
    if multi:
        return int(multi.group(1)) * int(multi.group(2))
    single_box = re.search(r'(\d+)/box', title, re.IGNORECASE)
    if single_box:
        return int(single_box.group(1))
    rounds = re.search(r'(\d+)\s*rounds?', title, re.IGNORECASE)
    if rounds:
        return int(rounds.group(1))
    rd = re.search(r'(\d+)\s*rd\b', title, re.IGNORECASE)
    if rd:
        return int(rd.group(1))
    return None

def parse_grain(title):
    m = re.search(r'(\d+)\s*gr', title, re.IGNORECASE)
    return int(m.group(1)) if m else None

def parse_case_material(title):
    title_lower = title.lower()
    steel_brands = ['wolf', 'tula', 'tulammo', 'brown bear', 'sterling']
    if any(b in title_lower for b in steel_brands):
        return 'Steel'
    if 'steel' in title_lower:
        return 'Steel'
    if 'aluminum' in title_lower or 'aluminium' in title_lower:
        return 'Aluminum'
    if 'brass' in title_lower:
        return 'Brass'
    if 'polymer' in title_lower:
        return 'Polymer'
    return 'Brass'

def parse_bullet_type(title):
    title_upper = title.upper()
    if 'JHP' in title_upper or 'HOLLOW POINT' in title_upper:
        return 'JHP'
    if 'TMJ' in title_upper:
        return 'TMJ'
    if 'FMJ' in title_upper:
        return 'FMJ'
    if 'LRN' in title_upper or 'LEAD ROUND' in title_upper:
        return 'LRN'
    if 'JSP' in title_upper:
        return 'JSP'
    if 'FRANGIBLE' in title_upper:
        return 'Frangible'
    if 'HP' in title_upper:
        return 'JHP'
    return 'FMJ'

def parse_brand(title):
    brands = [
        'Federal', 'Winchester', 'Remington', 'Hornady', 'CCI', 'Speer',
        'Magtech', 'PMC', 'Fiocchi', 'Blazer', 'Wolf', 'Tula', 'TulAmmo',
        'Aguila', 'Browning', 'Sig Sauer', 'SIG Sauer', 'Prvi Partizan',
        'Sellier & Bellot', 'American Eagle', 'Norma', 'Lapua',
        'Black Hills', 'Underwood', 'Liberty', 'Maxxtech', 'Igman', 'Sterling',
    ]
    for brand in brands:
        if brand.lower() in title.lower():
            return brand
    return None

def parse_condition(title):
    if 'reman' in title.lower() or 'remanufactured' in title.lower():
        return 'Remanufactured'
    return 'New'

def extract_product_id(url):
    if not url:
        return None
    slug = url.rstrip('/').split('/')[-1]
    return slug[:100]

async def scrape():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        all_products = []
        page_num = 1

        while True:
            url = BASE_URL if page_num == 1 else f"{BASE_URL}?page={page_num}"
            print(f"Scraping page {page_num}: {url}")
            await page.goto(url, wait_until='domcontentloaded', timeout=30000)
            await page.wait_for_timeout(4000)

            cards = await page.query_selector_all('li.product')

            if not cards:
                print(f"No cards found on page {page_num}, stopping.")
                break

            for card in cards:
                try:
                    title_el = await card.query_selector('h4.card-title a')
                    if not title_el:
                        continue
                    title = (await title_el.inner_text()).strip()
                    link = await title_el.get_attribute('href')
                    if link and not link.startswith('http'):
                        link = 'https://aeammo.com' + link

                    price_el = await card.query_selector('.price--withoutTax')
                    if not price_el:
                        continue
                    price_text = (await price_el.inner_text()).strip()
                    price_match = re.search(r'\$?([\d,]+\.?\d*)', price_text.replace(',', ''))
                    if not price_match:
                        continue
                    price = float(price_match.group(1))

                    rounds = parse_rounds(title)
                    if not rounds or rounds < 1:
                        continue

                    grain = parse_grain(title)
                    case_material = parse_case_material(title)
                    bullet_type = parse_bullet_type(title)
                    brand = parse_brand(title)
                    condition = parse_condition(title)
                    ppr = round(price / rounds, 4)
                    product_id = extract_product_id(link)

                    product = {
                        'retailer_id': RETAILER_ID,
                        'retailer_product_id': product_id,
                        'caliber': '9mm',
                        'caliber_normalized': '9mm',
                        'product_url': link,
                        'base_price': round(price, 2),
                        'price_per_round': ppr,
                        'rounds_per_box': rounds,
                        'total_rounds': rounds,
                        'manufacturer': brand,
                        'grain': grain,
                        'bullet_type': bullet_type,
                        'case_material': case_material,
                        'condition_type': condition,
                        'in_stock': True,
                        'last_updated': datetime.now(timezone.utc).isoformat(),
                    }
                    all_products.append(product)
                    print(f"  [ok] {title[:60]} | ${price} | {rounds}rd | {ppr:.2f}c/rd")

                except Exception as e:
                    print(f"  Error on card: {e}")
                    continue

            next_btn = await page.query_selector('a[rel="next"], .pagination-item--next a')
            if not next_btn:
                break
            page_num += 1

        await browser.close()

        print(f"\nTotal scraped: {len(all_products)}")

        if not all_products:
            print("Nothing to upsert.")
            return

        now = datetime.now(timezone.utc).isoformat()
        for product in all_products:
            try:
                result = supabase.table('listings').upsert(
                    product,
                    on_conflict='retailer_id,retailer_product_id'
                ).execute()

                if result.data:
                    listing_id = result.data[0]['id']
                    supabase.table('price_history').insert({
                        'listing_id': listing_id,
                        'price': product['base_price'],
                        'price_per_round': product['price_per_round'],
                        'in_stock': True,
                        'recorded_at': now,
                    }).execute()

            except Exception as e:
                print(f"  DB error for {product.get('manufacturer','?')}: {e}")

        print("Done.")

if __name__ == "__main__":
    asyncio.run(scrape())