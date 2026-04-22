import asyncio
import os
import re
from datetime import datetime, timezone
from playwright.async_api import async_playwright
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
RETAILER_ID = 13
BASE_URL = "https://ammo.com/handgun/9mm-ammo"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def parse_rounds(title):
    # Primary ammo.com pattern: "... - 1000 Rounds of 115 Grain ..."
    primary = re.search(r'(\d+)\s*rounds?\s*of\b', title, re.IGNORECASE)
    if primary:
        return int(primary.group(1))

    case = re.search(r'(\d+)\s*(?:rd|rds|rounds?)\s*case', title, re.IGNORECASE)
    if case:
        return int(case.group(1))

    multi = re.search(r'(\d+)\s*(?:rd|rds|rounds?)\s*(?:box|loose)?\s*x\s*(\d+)', title, re.IGNORECASE)
    if multi:
        return int(multi.group(1)) * int(multi.group(2))

    box = re.search(r'(\d+)\s*(?:rd|rds)\s*box', title, re.IGNORECASE)
    if box:
        return int(box.group(1))

    rounds = re.search(r'(\d+)\s*rounds?\b', title, re.IGNORECASE)
    if rounds:
        return int(rounds.group(1))

    rd = re.search(r'(\d+)\s*rd\b', title, re.IGNORECASE)
    if rd:
        return int(rd.group(1))
    return None

def parse_grain(title):
    m = re.search(r'(\d+)\s*(?:grain|gr)\b', title, re.IGNORECASE)
    return int(m.group(1)) if m else None

def parse_case_material(title):
    title_lower = title.lower()
    steel_brands = ['wolf', 'tula', 'tulammo', 'brown bear', 'silver bear', 'barnaul', 'red army', 'sterling']
    if any(b in title_lower for b in steel_brands):
        return 'Steel'
    if 'steel' in title_lower:
        return 'Steel'
    if 'aluminum' in title_lower or 'aluminium' in title_lower:
        return 'Aluminum'
    if 'brass' in title_lower:
        return 'Brass'
    if 'nickel' in title_lower:
        return 'Nickel'
    if 'polymer' in title_lower:
        return 'Polymer'
    return 'Brass'

def parse_bullet_type(title):
    title_upper = title.upper()
    if 'JHP' in title_upper or 'HOLLOW POINT' in title_upper:
        return 'JHP'
    if 'TMJ' in title_upper:
        return 'TMJ'
    if 'FMJ' in title_upper or 'FULL METAL JACKET' in title_upper:
        return 'FMJ'
    if 'LRN' in title_upper or 'LEAD ROUND' in title_upper:
        return 'LRN'
    if 'JSP' in title_upper:
        return 'JSP'
    if 'FRANGIBLE' in title_upper:
        return 'Frangible'
    if 'FTX' in title_upper or 'FLEXLOCK' in title_upper:
        return 'JHP'
    if 'INCENDIARY' in title_upper:
        return 'Incendiary'
    if 'BLANK' in title_upper:
        return 'Blank'
    if 'HP' in title_upper:
        return 'JHP'
    return 'FMJ'

def parse_brand(title):
    brands = [
        'Federal American Eagle', 'American Eagle', 'Federal Champion', 'Federal',
        'Winchester USA Forged', 'Winchester', 'Remington', 'Hornady',
        'CCI Blazer', 'CCI', 'Speer', 'Magtech', 'PMC', 'Fiocchi', 'Blazer',
        'Wolf', 'Tula', 'TulAmmo', 'Aguila', 'Browning', 'Sig Sauer', 'SIG Sauer',
        'Prvi Partizan', 'Sellier and Bellot', 'Sellier & Bellot', 'Norma', 'Lapua',
        'Black Hills', 'Underwood', 'Liberty', 'Maxxtech', 'Igman', 'Sterling',
        'Barnes', 'Precision One', 'New Republic', 'Paraklese', 'Excalibur',
        'Barnaul', 'Silver Bear', 'Brown Bear', 'Red Army',
    ]
    title_lower = title.lower()
    for brand in brands:
        if brand.lower() in title_lower:
            if brand == 'Sellier & Bellot':
                return 'Sellier and Bellot'
            if brand in ('Federal American Eagle', 'American Eagle', 'Federal Champion'):
                return 'Federal'
            if brand == 'CCI Blazer':
                return 'CCI'
            if brand == 'Winchester USA Forged':
                return 'Winchester'
            return brand
    return None

def parse_condition(title):
    if 'reman' in title.lower() or 'remanufactured' in title.lower():
        return 'Remanufactured'
    return 'New'

async def scrape():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(user_agent=USER_AGENT)
        page = await ctx.new_page()

        all_products = []
        page_num = 1

        while True:
            url = BASE_URL if page_num == 1 else f"{BASE_URL}?p={page_num}"
            print(f"Scraping page {page_num}: {url}")
            resp = await page.goto(url, wait_until='domcontentloaded', timeout=45000)
            if resp and resp.status >= 400:
                print(f"Page {page_num} returned {resp.status}, stopping.")
                break
            await page.wait_for_timeout(4000)

            cards = await page.query_selector_all('li.b-product-list-item')

            if not cards:
                print(f"No cards found on page {page_num}, stopping.")
                break

            for card in cards:
                try:
                    title_el = await card.query_selector('h2.b-product-list-item__product-name')
                    if not title_el:
                        continue
                    title = (await title_el.inner_text()).strip()
                    product_slug = await title_el.get_attribute('id')
                    if not product_slug:
                        li_id = await card.get_attribute('id')
                        product_slug = li_id[len('product-'):] if li_id and li_id.startswith('product-') else li_id
                    if not product_slug:
                        continue
                    link = f"{BASE_URL}#{product_slug}"

                    # Current price: sale price if present, else regular.
                    price_el = await card.query_selector('.b-price-sale__special .price')
                    if not price_el:
                        price_el = await card.query_selector('.b-price-regular .price')
                    if not price_el:
                        continue
                    price_text = (await price_el.inner_text()).strip()
                    price_match = re.search(r'\$?([\d,]+\.?\d*)', price_text.replace(',', ''))
                    if not price_match:
                        continue
                    price = float(price_match.group(1))

                    in_stock_el = await card.query_selector('.b-availability__in-stock')
                    in_stock = in_stock_el is not None

                    rounds = parse_rounds(title)
                    if not rounds or rounds < 1:
                        continue

                    grain = parse_grain(title)
                    case_material = parse_case_material(title)
                    bullet_type = parse_bullet_type(title)
                    brand = parse_brand(title)
                    condition = parse_condition(title)
                    ppr = round(price / rounds * 100, 4)

                    product = {
                        'retailer_id': RETAILER_ID,
                        'retailer_product_id': product_slug[:100],
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
                        'in_stock': in_stock,
                        'last_updated': datetime.now(timezone.utc).isoformat(),
                    }
                    all_products.append(product)
                    print(f"  [ok] {title[:60]} | ${price} | {rounds}rd | {ppr:.2f}c/rd")

                except Exception as e:
                    print(f"  Error on card: {e}")
                    continue

            next_link = await page.query_selector('link[rel="next"]')
            if not next_link:
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
                        'in_stock': product['in_stock'],
                        'recorded_at': now,
                    }).execute()

            except Exception as e:
                print(f"  DB error for {product.get('manufacturer','?')}: {e}")

        print("Done.")

if __name__ == "__main__":
    asyncio.run(scrape())
