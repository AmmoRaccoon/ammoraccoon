import asyncio
import os
import re
import sys
from datetime import datetime, timezone
from playwright.async_api import async_playwright
from supabase import create_client

from scraper_lib import CALIBERS, now_iso, with_stock_fields, parse_purchase_limit, sanity_check_ppr, parse_bullet_type

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
RETAILER_ID = 14
SITE_BASE = "https://www.bulkammo.com"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
)

# BulkAmmo caliber category paths. Values are lists because some
# calibers map to multiple categories — BulkAmmo splits .223 and 5.56
# into separate collection pages (mirroring TrueShot + Lucky Gunner).
# Nine of ten entries below were renamed during a 2026-05-09-or-earlier
# storefront restructure: the prior values silently 404'd and produced
# empty pages until the audit on 2026-05-09 caught the absence (9 of
# 10 configured URLs were dead, exact-match to the 9 missing calibers
# in the DB). BulkAmmo standardized on leading-period caliber slugs
# (.380, .40, .357, .223, .300) plus shortened suffixes (-spl, -mag).
CALIBER_PATHS = {
    '9mm':     ['/handgun/bulk-9mm-ammo'],
    '380acp':  ['/handgun/bulk-.380-acp-ammo'],
    '40sw':    ['/handgun/bulk-.40-s-w-ammo'],
    '38spl':   ['/handgun/bulk-.38-spl-ammo'],
    '357mag':  ['/handgun/bulk-.357-mag-ammo'],
    '22lr':    ['/rimfire/bulk-.22-lr-ammo'],
    '223-556': ['/rifle/bulk-.223-ammo',
                '/rifle/bulk-5.56x45-ammo'],
    '308win':  ['/rifle/bulk-.308-win-ammo'],
    '762x39':  ['/rifle/bulk-7.62x39mm-ammo'],
    '300blk':  ['/rifle/bulk-.300-aac-blackout-ammo'],
}

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def parse_rounds(title):
    primary = re.search(r'^\s*(\d+)\s*rounds?\s*of\b', title, re.IGNORECASE)
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
        'Sellier and Bellot', 'Sellier & Bellot', 'Norma', 'Lapua',
        'Black Hills', 'Underwood', 'Liberty', 'Maxxtech', 'Igman', 'Sterling',
        'Barnes', 'Precision One', 'Armscor', 'Colt', 'Corbon',
        'Barnaul', 'Silver Bear', 'Brown Bear', 'Red Army',
    ]
    title_lower = title.lower()
    for brand in brands:
        if brand.lower() in title_lower:
            if brand == 'Sellier & Bellot':
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
    silently return zero products on first load.
    """
    products = []
    flags = []

    for handle in CALIBER_PATHS[caliber_norm]:
        base = SITE_BASE + handle
        page_num = 1
        empty_first_page = False

        while True:
            url = base if page_num == 1 else f"{base}?p={page_num}"
            print(f"\n[{caliber_norm}/{handle}] page {page_num}: {url}")
            try:
                resp = await page.goto(url, wait_until='domcontentloaded', timeout=45000)
            except Exception as e:
                print(f"  goto failed: {e}")
                if page_num == 1:
                    empty_first_page = True
                break
            if resp and resp.status >= 400:
                print(f"  HTTP {resp.status} — handle unreachable.")
                if page_num == 1:
                    empty_first_page = True
                    print(f"  WARN: BulkAmmo collection {handle} returned "
                          f"zero products on first page (caliber {caliber_norm}).")
                break
            await page.wait_for_timeout(3000)

            cards = await page.query_selector_all('#catalog-listing li.item')
            if not cards:
                if page_num == 1:
                    empty_first_page = True
                    # Loud, grep-friendly line so the cause is obvious
                    # in CI logs even if the run as a whole succeeds.
                    print(f"  WARN: BulkAmmo collection {handle} returned "
                          f"zero products on first page (caliber {caliber_norm}).")
                else:
                    print(f"  No cards on page {page_num}, stopping handle.")
                break

            new_on_page = 0
            for card in cards:
                try:
                    title_el = await card.query_selector('a.product-name')
                    if not title_el:
                        continue
                    title = (await title_el.inner_text()).strip()
                    link = await title_el.get_attribute('href')
                    if link and not link.startswith('http'):
                        link = SITE_BASE + link

                    price_el = await card.query_selector('.price-box .special-price .price')
                    if not price_el:
                        price_el = await card.query_selector('.price-box .regular-price .price')
                    if not price_el:
                        price_el = await card.query_selector('.price-box .price')
                    if not price_el:
                        continue
                    price_text = (await price_el.inner_text()).strip()
                    price_match = re.search(r'\$?([\d,]+\.?\d*)', price_text.replace(',', ''))
                    if not price_match:
                        continue
                    price = float(price_match.group(1))

                    in_stock_el = await card.query_selector('.availability .in-stock')
                    in_stock = in_stock_el is not None
                    card_text = await card.inner_text()
                    purchase_limit = parse_purchase_limit(card_text)

                    rounds = parse_rounds(title)
                    if not rounds or rounds < 1:
                        continue

                    grain = parse_grain(title)
                    case_material = parse_case_material(title)
                    bullet_type = parse_bullet_type(title)
                    brand = parse_brand(title) or "Unknown"
                    condition = parse_condition(title)
                    ppr = round(price / rounds, 4)
                    if not sanity_check_ppr(ppr, price, rounds, context=title[:60], caliber=caliber_norm):
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

            next_link = await page.query_selector('link[rel="next"], a.next.i-next')
            if not next_link:
                break
            page_num += 1
            if page_num > 15:
                break

        flags.append((handle, empty_first_page))
    return products, flags


async def scrape():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(user_agent=USER_AGENT)
        page = await ctx.new_page()

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
        # fine; three or more is a strong signal that BulkAmmo renamed
        # collection paths and the scraper is silently producing partial
        # data (the exact symptom that hid 9 of 10 calibers from the DB
        # until the 2026-05-09 audit). Exit non-zero so CI runs go red,
        # and skip the upsert step so partial data doesn't replace good
        # rows.
        EMPTY_FAIL_THRESHOLD = 3
        if len(empty_handles) >= EMPTY_FAIL_THRESHOLD:
            print(f"\nFAIL: {len(empty_handles)} BulkAmmo collections returned "
                  f"zero products on first page — likely storefront drift:")
            for cal, h in empty_handles:
                print(f"  - {cal}: BulkAmmo collection {h} returned zero products on first page")
            sys.exit(1)
        elif empty_handles:
            print(f"\nWARN: {len(empty_handles)} BulkAmmo collection(s) returned "
                  f"zero products on first page (transient or worth investigating):")
            for cal, h in empty_handles:
                print(f"  - {cal}: BulkAmmo collection {h} returned zero products on first page")

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
