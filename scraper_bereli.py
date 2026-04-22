import asyncio
import os
import re
import urllib.request
from datetime import datetime, timezone
from playwright.async_api import async_playwright
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
RETAILER_ID = 19
BASE_URL = "https://www.bereli.com/ammunition/handgun-ammo/9mm-ammo/"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def parse_rounds(title):
    # Bereli titles vary: explicit "N Rounds" / hyphenated "50-ROUNDS" / trailing "500 Bulk Pack".
    # Try explicit forms first, then common trailing-quantity patterns. Products with
    # no quantity in the title get skipped; they'd require fetching the product page.
    primary = re.search(r'(\d[\d,]*)[\s-]*rounds?\b', title, re.IGNORECASE)
    if primary:
        return int(primary.group(1).replace(',', ''))
    case = re.search(r'(\d+)\s*(?:rd|rds|rounds?)\s*case', title, re.IGNORECASE)
    if case:
        return int(case.group(1))
    box = re.search(r'(\d+)\s*(?:rd|rds)\s*box', title, re.IGNORECASE)
    if box:
        return int(box.group(1))
    trailing_bulk = re.search(r'(\d{2,4})\s*(?:bulk\s*pack|bulk|pack|ct|count)\b', title, re.IGNORECASE)
    if trailing_bulk:
        return int(trailing_bulk.group(1))
    rd = re.search(r'(\d+)\s*rd\b', title, re.IGNORECASE)
    if rd:
        return int(rd.group(1))
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

def parse_condition(title):
    t = title.lower()
    if 'reman' in t or 'remanufactured' in t:
        return 'Remanufactured'
    return 'New'

def extract_product_id(url):
    if not url:
        return None
    slug = url.rstrip('/').split('/')[-1]
    return slug[:100]

def fetch_rounds_from_product_page(link):
    # Bereli product pages (BigCommerce) have the spec as an <li> in the
    # description: "<li>Quantity: N Rounds per box</li>". Matching only that
    # exact form avoids grabbing marketing copy or related-product blurbs.
    try:
        req = urllib.request.Request(link, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=20) as resp:
            html = resp.read().decode('utf-8', errors='replace')
    except Exception:
        return None
    m = re.search(r'<li>\s*Quantity\s*:?\s*(\d[\d,]*)\s*Rounds?\s*per\s*box\s*</li>', html, re.IGNORECASE)
    if m:
        return int(m.group(1).replace(',', ''))
    return None

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
            url = BASE_URL if page_num == 1 else f"{BASE_URL}?page={page_num}"
            print(f"Scraping page {page_num}: {url}")
            try:
                resp = await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            except Exception as e:
                print(f"  goto failed: {e}")
                break
            if resp and resp.status >= 400:
                print(f"  status {resp.status}, stopping.")
                break
            await page.wait_for_timeout(4000)

            cards = await page.query_selector_all('li.product')
            if not cards:
                print(f"  no cards on page {page_num}, stopping.")
                break

            new_on_page = 0
            for card in cards:
                try:
                    article = await card.query_selector('article.card')
                    if not article:
                        continue
                    entity_id = await article.get_attribute('data-entity-id')
                    name = await article.get_attribute('data-name')
                    brand = await article.get_attribute('data-product-brand')
                    data_price = await article.get_attribute('data-product-price')
                    if not entity_id or not name:
                        continue

                    link_el = await card.query_selector('.card-title a, .card-figure__link')
                    link = await link_el.get_attribute('href') if link_el else None
                    if link and not link.startswith('http'):
                        link = 'https://www.bereli.com' + link

                    # Prefer the visible "main" price element; fall back to data-product-price.
                    price_el = await card.query_selector('.price.price--main')
                    price = None
                    if price_el:
                        price_text = (await price_el.inner_text()).strip()
                        pm = re.search(r'\$?([\d,]+\.\d{2})', price_text.replace(',', ''))
                        if pm:
                            price = float(pm.group(1))
                    if price is None and data_price:
                        try:
                            price = float(data_price)
                        except ValueError:
                            pass
                    if price is None or price <= 0:
                        continue

                    rounds = parse_rounds(name)
                    if (not rounds or rounds < 1) and link:
                        rounds = fetch_rounds_from_product_page(link)
                    if not rounds or rounds < 1:
                        continue

                    grain = parse_grain(name)
                    case_material = parse_case_material(name)
                    bullet_type = parse_bullet_type(name)
                    condition = parse_condition(name)
                    ppr = round(price / rounds * 100, 4)
                    product_id = entity_id[:100]
                    if product_id in seen_ids:
                        continue
                    seen_ids.add(product_id)

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
                        'manufacturer': brand or None,
                        'grain': grain,
                        'bullet_type': bullet_type,
                        'case_material': case_material,
                        'condition_type': condition,
                        'in_stock': True,
                        'last_updated': datetime.now(timezone.utc).isoformat(),
                    }
                    all_products.append(product)
                    new_on_page += 1
                    print(f"  [ok] {name[:55]} | ${price} | {rounds}rd | {ppr:.2f}c/rd")
                except Exception as e:
                    print(f"  Error on card: {e}")
                    continue

            if new_on_page == 0:
                print(f"  no new products on page {page_num}, stopping.")
                break
            page_num += 1
            if page_num > 30:
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
