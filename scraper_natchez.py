import asyncio
import os
import re
from datetime import datetime, timezone
from playwright.async_api import async_playwright
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
RETAILER_ID = 16
BASE_URL = "https://www.natchezss.com/ammunition/handgun-ammunition/9mm-ammo"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def parse_rounds(title):
    # Natchez titles typically end with "1000/ct", "50/ct", etc.
    ct = re.search(r'(\d+)\s*/\s*ct\b', title, re.IGNORECASE)
    if ct:
        return int(ct.group(1))
    count = re.search(r'(\d+)\s*count\b', title, re.IGNORECASE)
    if count:
        return int(count.group(1))
    primary = re.search(r'(\d+)\s*rounds?\s*of\b', title, re.IGNORECASE)
    if primary:
        return int(primary.group(1))
    box = re.search(r'(\d+)\s*(?:rd|rds)\s*box', title, re.IGNORECASE)
    if box:
        return int(box.group(1))
    rounds = re.search(r'(\d+)\s*rounds?\b', title, re.IGNORECASE)
    if rounds:
        return int(rounds.group(1))
    return None

def parse_grain(title):
    m = re.search(r'(\d+)\s*(?:grain|gr)\.?\b', title, re.IGNORECASE)
    return int(m.group(1)) if m else None

def parse_case_material(title):
    t = title.lower()
    steel_brands = ['wolf', 'tula', 'tulammo', 'brown bear', 'silver bear', 'barnaul', 'red army']
    if 'steel case' in t or 'steel-case' in t:
        return 'Steel'
    if any(b in t for b in steel_brands):
        return 'Steel'
    if 'steel' in t:
        return 'Steel'
    if 'aluminum' in t or 'aluminium' in t:
        return 'Aluminum'
    if 'nickel' in t:
        return 'Nickel'
    if 'brass' in t:
        return 'Brass'
    if 'polymer' in t:
        return 'Polymer'
    return 'Brass'

def parse_bullet_type(title):
    u = title.upper()
    if 'JHP' in u or 'HOLLOW POINT' in u or 'BJHP' in u:
        return 'JHP'
    if 'TMJ' in u:
        return 'TMJ'
    if 'FMJ' in u or 'FULL METAL JACKET' in u:
        return 'FMJ'
    if 'LRN' in u or 'LEAD ROUND' in u:
        return 'LRN'
    if 'JSP' in u:
        return 'JSP'
    if 'FRANGIBLE' in u:
        return 'Frangible'
    if 'FTX' in u or 'FLEXLOCK' in u or 'XTP' in u or 'HONEYBADGER' in u:
        return 'JHP'
    if 'INCENDIARY' in u:
        return 'Incendiary'
    if 'BLANK' in u:
        return 'Blank'
    if 'HP' in u:
        return 'JHP'
    return 'FMJ'

def parse_brand(title):
    brands = [
        'Federal American Eagle', 'American Eagle', 'Federal Champion',
        'Federal Personal Defense', 'Federal Premium', 'Federal',
        'Winchester Supreme Elite', 'Winchester USA Forged', 'Winchester',
        'Remington Golden Saber', 'Remington HTP', 'Remington',
        'Hornady Critical Duty', 'Hornady Critical Defense', 'Hornady',
        'CCI Blazer', 'CCI', 'Speer Gold Dot', 'Speer',
        'Magtech', 'PMC', 'Fiocchi', 'Blazer', 'Wolf', 'Tula', 'TulAmmo',
        'Aguila', 'Browning', 'Sig Sauer', 'SIG Sauer', 'Prvi Partizan',
        'Sellier and Bellot', 'Sellier & Bellot', 'Seller & Bellot',
        'Norma', 'Lapua', 'Black Hills', 'Underwood', 'Liberty', 'Maxxtech',
        'Igman', 'Sterling', 'Barnes', 'Precision One', 'Armscor', 'Colt',
        'Corbon', 'Barnaul', 'Silver Bear', 'Brown Bear', 'Red Army',
    ]
    title_lower = title.lower()
    for brand in brands:
        if brand.lower() in title_lower:
            if brand in ('Sellier & Bellot', 'Seller & Bellot'):
                return 'Sellier and Bellot'
            if brand in ('Federal American Eagle', 'American Eagle', 'Federal Champion',
                         'Federal Personal Defense', 'Federal Premium'):
                return 'Federal'
            if brand == 'CCI Blazer':
                return 'CCI'
            if brand in ('Winchester Supreme Elite', 'Winchester USA Forged'):
                return 'Winchester'
            if brand in ('Remington Golden Saber', 'Remington HTP'):
                return 'Remington'
            if brand in ('Hornady Critical Duty', 'Hornady Critical Defense'):
                return 'Hornady'
            if brand == 'Speer Gold Dot':
                return 'Speer'
            return brand
    return None

def parse_condition(title):
    t = title.lower()
    if 'reman' in t or 'remanufactured' in t:
        return 'Remanufactured'
    return 'New'

async def scrape():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 2400},
            locale="en-US",
        )
        page = await ctx.new_page()

        all_products = []
        seen_ids = set()
        page_num = 1

        while True:
            url = BASE_URL if page_num == 1 else f"{BASE_URL}?p={page_num}"
            print(f"Scraping page {page_num}: {url}")
            try:
                resp = await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            except Exception as e:
                print(f"  goto failed: {e}")
                break
            if resp and resp.status >= 400:
                print(f"  status {resp.status}, stopping.")
                break
            try:
                await page.wait_for_selector('.product__tile', timeout=30000)
            except Exception:
                print(f"  no tiles on page {page_num}, stopping.")
                break
            await page.wait_for_timeout(2500)

            tiles = await page.query_selector_all('.product__tile')
            if not tiles:
                print(f"  no tiles, stopping.")
                break

            new_on_page = 0
            for tile in tiles:
                try:
                    sku = await tile.get_attribute('data-item-id')
                    href_el = await tile.query_selector('a[href]')
                    href = await href_el.get_attribute('href') if href_el else None
                    text = (await tile.inner_text()).strip()
                    if not sku or not href or not text:
                        continue

                    lines = [l.strip() for l in text.split('\n') if l.strip()]
                    # Title is the long descriptive line containing a caliber token.
                    title = None
                    for line in lines:
                        if re.search(r'\b(9mm|9 mm|9x19)\b', line, re.IGNORECASE) and len(line) > 25:
                            title = line
                            break
                    if not title:
                        continue

                    rounds = parse_rounds(title)
                    if not rounds or rounds < 1:
                        continue

                    # Prices: first $XX.XX is the current/sale price.
                    price_matches = re.findall(r'\$([\d,]+\.\d{2})', text)
                    # The PPR also starts with $, so exclude anything <= 10 as likely PPR
                    # by preferring the first dollar amount on its own price line.
                    price = None
                    for pm in price_matches:
                        val = float(pm.replace(',', ''))
                        if val >= 1.0:  # exclude PPR like $0.27
                            price = val
                            break
                    if price is None:
                        continue

                    in_stock = 'OUT OF STOCK' not in text.upper()

                    grain = parse_grain(title)
                    case_material = parse_case_material(title)
                    bullet_type = parse_bullet_type(title)
                    brand = parse_brand(title)
                    condition = parse_condition(title)
                    ppr = round(price / rounds * 100, 4)

                    product_id = sku[:100]
                    if product_id in seen_ids:
                        continue
                    seen_ids.add(product_id)

                    link = href if href.startswith('http') else 'https://www.natchezss.com' + href

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
                        'in_stock': in_stock,
                        'last_updated': datetime.now(timezone.utc).isoformat(),
                    }
                    all_products.append(product)
                    new_on_page += 1
                    print(f"  [ok] {title[:55]} | ${price} | {rounds}rd | {ppr:.2f}c/rd | {'in' if in_stock else 'OUT'}")
                except Exception as e:
                    print(f"  Error on tile: {e}")
                    continue

            if new_on_page == 0:
                print(f"  no new products on page {page_num}, stopping.")
                break
            page_num += 1
            if page_num > 30:  # hard safety stop
                print("  hit page cap 30, stopping.")
                break

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
