import asyncio
import os
import re
import sys
from datetime import datetime, timezone
from playwright.async_api import async_playwright
from supabase import create_client

from scraper_lib import CALIBERS, now_iso, with_stock_fields, parse_purchase_limit, sanity_check_ppr, parse_bullet_type, parse_brand

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
RETAILER_ID = 12
SITE_BASE = "https://rivertownmunitions.com"

# Rivertown WooCommerce paths per caliber. Values are lists for parity
# with the other CALIBER_PATHS scrapers. Sitemap (sitemap_index.xml,
# 220 /product-category/ URLs) confirmed as source of truth on
# 2026-05-09. Five of the 10 originally configured paths had drifted
# to 404; all five are recoverable via slug rename. Three calibers
# split into two collections each (380acp, 223-556, 308win), mirroring
# the Natchez/Ammo.com pattern for split-leg calibers.
CALIBER_PATHS = {
    '9mm':     ['/product-category/handgun/9mm/'],
    '380acp':  ['/product-category/handgun/380/',
                '/product-category/handgun/380-auto/'],
    '40sw':    ['/product-category/handgun/40-sw/'],
    '38spl':   ['/product-category/handgun/38-special/'],
    '357mag':  ['/product-category/handgun/357-mag/'],
    '22lr':    ['/product-category/rimfire/22lr/'],
    '223-556': ['/product-category/rifle/223/',
                '/product-category/rifle/5-56x45mm-nato/'],
    '308win':  ['/product-category/rifle/308-win/',
                '/product-category/rifle/308-7-62x51mm/'],
    '762x39':  ['/product-category/rifle/7-62x39mm/'],
    '300blk':  ['/product-category/rifle/300-blackout/'],
}

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def parse_rounds(title):
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
    steel_brands = ['wolf', 'tula', 'tulammo', 'brown bear', 'sterling']
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


def parse_condition(title):
    if 'reman' in title.lower() or 'remanufactured' in title.lower():
        return 'Remanufactured'
    return 'New'

def extract_product_id(url):
    if not url:
        return None
    slug = url.rstrip('/').split('/')[-1]
    return slug[:100]

async def scrape_caliber(page, caliber_norm, caliber_display, seen_ids):
    """Scrape every configured handle for a caliber.

    Returns (products, flags) where flags is a list of (handle,
    empty_first_page) tuples. The orchestrator in scrape() uses the
    flags to fire the storefront-drift guardrail when too many handles
    silently render zero li.product elements.
    """
    products = []
    flags = []

    for handle in CALIBER_PATHS[caliber_norm]:
        base = SITE_BASE + handle
        page_num = 1
        empty_first_page = False

        while True:
            url = base if page_num == 1 else f"{base}page/{page_num}/"
            print(f"\n[{caliber_norm}/{handle}] page {page_num}: {url}")
            try:
                resp = await page.goto(url, wait_until='domcontentloaded', timeout=30000)
            except Exception as e:
                print(f"  goto failed: {e}")
                if page_num == 1:
                    empty_first_page = True
                    print(f"  WARN: Rivertown collection {handle} returned "
                          f"zero products on first page (caliber {caliber_norm}).")
                break
            if resp and resp.status >= 400:
                print(f"  HTTP {resp.status} - skipping handle.")
                if page_num == 1:
                    empty_first_page = True
                    print(f"  WARN: Rivertown collection {handle} returned "
                          f"zero products on first page (caliber {caliber_norm}).")
                break
            await page.wait_for_timeout(3000)

            cards = await page.query_selector_all('li.product')
            if not cards:
                if page_num == 1:
                    empty_first_page = True
                    print(f"  WARN: Rivertown collection {handle} returned "
                          f"zero products on first page (caliber {caliber_norm}).")
                else:
                    print(f"  No cards on page {page_num}, stopping handle.")
                break

            new_on_page = 0
            for card in cards:
                try:
                    # WooCommerce marks OOS products with an 'outofstock' class
                    # on the <li class="product ..."> wrapper.
                    card_class = await card.get_attribute('class') or ''
                    in_stock = 'outofstock' not in card_class.lower()
                    card_text = await card.inner_text()
                    purchase_limit = parse_purchase_limit(card_text)

                    # Title selector: must use sequential fallback, NOT an
                    # OR-selector. Playwright returns the first DOM-order
                    # match of the union, and a.woocommerce-loop-product__link
                    # wraps the card image and appears earlier in the DOM
                    # than the title element. The OR-selector silently
                    # picked the (text-empty) link on every card, leaving
                    # parse_text relying solely on the URL slug. Confirmed
                    # via 2026-05-09 audit.
                    title_el = (await card.query_selector('.woocommerce-loop-product__title')
                                or await card.query_selector('h2 a')
                                or await card.query_selector('a.woocommerce-loop-product__link'))
                    link_el = (await card.query_selector('a.woocommerce-loop-product__link')
                               or await card.query_selector('h2 a'))
                    if not title_el:
                        continue
                    title = (await title_el.inner_text()).strip()
                    href_src = link_el or title_el
                    link = await href_src.get_attribute('href')
                    if link and not link.startswith('http'):
                        link = SITE_BASE + link

                    slug_text = link.rsplit('/', 2)[-2].replace('-', ' ') if link else ''
                    parse_text = f"{title} {slug_text}"

                    price_el = await card.query_selector('.price ins .woocommerce-Price-amount')
                    if not price_el:
                        price_el = await card.query_selector('.price > .woocommerce-Price-amount')
                    if not price_el:
                        price_el = await card.query_selector('.price .woocommerce-Price-amount')
                    if not price_el:
                        continue
                    price_text = (await price_el.inner_text()).strip()
                    price_match = re.search(r'\$?([\d,]+\.?\d*)', price_text.replace(',', ''))
                    if not price_match:
                        continue
                    price = float(price_match.group(1))

                    rounds = parse_rounds(parse_text)
                    if not rounds or rounds < 1:
                        continue

                    grain = parse_grain(parse_text)
                    case_material = parse_case_material(parse_text)
                    bullet_type = parse_bullet_type(parse_text)
                    brand = parse_brand(parse_text) or "Unknown"
                    condition = parse_condition(parse_text)
                    ppr = round(price / rounds, 4)
                    if not sanity_check_ppr(ppr, price, rounds, context=parse_text[:60], caliber=caliber_norm):
                        continue
                    product_id = extract_product_id(link)
                    if product_id in seen_ids:
                        continue
                    seen_ids.add(product_id)

                    product = {
                        'retailer_id': RETAILER_ID,
                        'retailer_product_id': product_id,
                        'caliber': caliber_display,
                        'caliber_normalized': caliber_norm,
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
                        'purchase_limit': purchase_limit,
                        'last_updated': now_iso(),
                    }
                    with_stock_fields(product, in_stock)
                    products.append(product)
                    new_on_page += 1
                    print(f"  [ok] {title[:55]} | ${price} | {rounds}rd | {ppr:.2f}/rd")
                except Exception as e:
                    print(f"  Error on card: {e}")
                    continue

            if new_on_page == 0:
                break

            next_btn = await page.query_selector('a.next.page-numbers')
            if not next_btn:
                break
            page_num += 1
            if page_num > 15:
                break

        flags.append((handle, empty_first_page))
    return products, flags


async def scrape():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        all_products = []
        seen_ids = set()
        empty_handles = []  # list of (caliber_norm, handle) for guardrail

        for caliber_norm in CALIBER_PATHS:
            caliber_display = CALIBERS[caliber_norm]
            products, flags = await scrape_caliber(page, caliber_norm, caliber_display, seen_ids)
            all_products.extend(products)
            for handle, empty in flags:
                if empty:
                    empty_handles.append((caliber_norm, handle))

        await browser.close()

        print(f"\nTotal scraped: {len(all_products)}")

        # Storefront-drift guardrail. A single transient empty handle is
        # fine; three or more is a strong signal that Rivertown renamed
        # WooCommerce category slugs and the scraper is silently producing
        # partial data (the exact symptom that hid 5 of 10 calibers from
        # the DB until the 2026-05-09 audit). Exit non-zero so CI runs go
        # red, and skip the upsert step so partial data doesn't replace
        # good rows.
        EMPTY_FAIL_THRESHOLD = 3
        if len(empty_handles) >= EMPTY_FAIL_THRESHOLD:
            print(f"\nFAIL: {len(empty_handles)} Rivertown collections returned "
                  f"zero products on first page — likely storefront drift:")
            for cal, h in empty_handles:
                print(f"  - {cal}: Rivertown collection {h} returned zero products on first page")
            sys.exit(1)
        elif empty_handles:
            print(f"\nWARN: {len(empty_handles)} Rivertown collection(s) returned "
                  f"zero products on first page (transient or worth investigating):")
            for cal, h in empty_handles:
                print(f"  - {cal}: Rivertown collection {h} returned zero products on first page")

        if not all_products:
            print("Nothing to upsert.")
            return

        now = now_iso()
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
