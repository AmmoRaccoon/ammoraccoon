import asyncio
import os
import re
import sys
from datetime import datetime, timezone
from playwright.async_api import async_playwright
from supabase import create_client

from scraper_lib import CALIBERS, normalize_caliber, now_iso, with_stock_fields, parse_purchase_limit, sanity_check_ppr, parse_bullet_type, parse_brand

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
RETAILER_ID = 16
SITE_BASE = "https://www.natchezss.com"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# Natchez category paths per caliber. Values are lists for parity
# with the other CALIBER_PATHS scrapers. 223-556 splits into two
# collections (.223 and 5.56 are separate Natchez pages, mirroring
# TrueShot/LG/BulkAmmo). Seven entries below were renamed during a
# 2026-05-09-or-earlier storefront restructure: the prior values
# silently rendered zero .product__tile elements until the audit
# caught the absence (7 of 10 configured URLs were dead, exact-match
# to the 7 calibers missing in the DB). Natchez's suffix convention
# is inconsistent — most calibers use `-ammo`, but 380acp + 357mag
# (and a handful of others outside our tracking) use `-ammunition`.
CALIBER_PATHS = {
    '9mm':     ['/ammunition/handgun-ammunition/9mm-ammo'],
    '380acp':  ['/ammunition/handgun-ammunition/380-acp-ammunition'],
    '40sw':    ['/ammunition/handgun-ammunition/40-cal-sw-ammo'],
    '38spl':   ['/ammunition/handgun-ammunition/38-special-ammo'],
    '357mag':  ['/ammunition/handgun-ammunition/357-magnum-ammunition'],
    '22lr':    ['/ammunition/rimfire-ammunition/22-lr-ammo'],
    '223-556': ['/ammunition/rifle-ammunition/223-ammo',
                '/ammunition/rifle-ammunition/5-56-ammo'],
    '308win':  ['/ammunition/rifle-ammunition/308-ammo'],
    '762x39':  ['/ammunition/rifle-ammunition/7-62-x39-ammo'],
    '300blk':  ['/ammunition/rifle-ammunition/300-blackout-ammo'],
}

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def parse_rounds(title):
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


def parse_condition(title):
    t = title.lower()
    if 'reman' in t or 'remanufactured' in t:
        return 'Remanufactured'
    return 'New'

async def scrape_caliber(page, caliber_norm, caliber_display, seen_ids):
    """Scrape every configured handle for a caliber.

    Returns (products, flags) where flags is a list of (handle,
    empty_first_page) tuples. The orchestrator in scrape() uses the
    flags to fire the storefront-drift guardrail when too many handles
    silently render zero .product__tile elements.
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
                resp = await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            except Exception as e:
                print(f"  goto failed: {e}")
                if page_num == 1:
                    empty_first_page = True
                break
            if resp and resp.status >= 400:
                print(f"  HTTP {resp.status} — handle unreachable.")
                if page_num == 1:
                    empty_first_page = True
                    print(f"  WARN: Natchez collection {handle} returned "
                          f"zero products on first page (caliber {caliber_norm}).")
                break
            try:
                await page.wait_for_selector('.product__tile', timeout=30000)
            except Exception:
                if page_num == 1:
                    empty_first_page = True
                    # Loud, grep-friendly line so the cause is obvious
                    # in CI logs even when the run as a whole succeeds.
                    print(f"  WARN: Natchez collection {handle} returned "
                          f"zero products on first page (caliber {caliber_norm}).")
                else:
                    print(f"  no tiles on page {page_num}, stopping handle.")
                break
            await page.wait_for_timeout(2500)

            tiles = await page.query_selector_all('.product__tile')
            if not tiles:
                if page_num == 1:
                    empty_first_page = True
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
                    title = None
                    for line in lines:
                        if len(line) > 25:
                            _, detected = normalize_caliber(line)
                            if detected == caliber_norm:
                                title = line
                                break
                    if not title:
                        # Use the longest line as fallback.
                        title = max(lines, key=len, default='')
                        if not title:
                            continue

                    rounds = parse_rounds(title)
                    if not rounds or rounds < 1:
                        continue

                    price_matches = re.findall(r'\$([\d,]+\.\d{2})', text)
                    price = None
                    for pm in price_matches:
                        val = float(pm.replace(',', ''))
                        if val >= 1.0:
                            price = val
                            break
                    if price is None:
                        continue

                    in_stock = 'OUT OF STOCK' not in text.upper()
                    purchase_limit = parse_purchase_limit(text)

                    grain = parse_grain(title)
                    case_material = parse_case_material(title)
                    bullet_type = parse_bullet_type(title)
                    brand = parse_brand(title) or "Unknown"
                    condition = parse_condition(title)
                    ppr = round(price / rounds, 4)
                    if not sanity_check_ppr(ppr, price, rounds, context=title[:60], caliber=caliber_norm):
                        continue

                    product_id = sku[:100]
                    if product_id in seen_ids:
                        continue
                    seen_ids.add(product_id)

                    link = href if href.startswith('http') else SITE_BASE + href

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
                    print(f"  [ok] {title[:55]} | ${price} | {rounds}rd | {ppr:.2f}/rd | {'in' if in_stock else 'OUT'}")
                except Exception as e:
                    print(f"  Error on tile: {e}")
                    continue

            if new_on_page == 0:
                break
            page_num += 1
            if page_num > 30:
                break

        flags.append((handle, empty_first_page))
    return products, flags


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

        # Storefront-drift guardrail. A single transient empty handle
        # is fine; three or more is a strong signal that Natchez
        # renamed collection paths and the scraper is silently
        # producing partial data (the exact symptom that hid 7 of 10
        # calibers from the DB until the 2026-05-09 audit). Exit
        # non-zero so CI runs go red, and skip the upsert step so
        # partial data doesn't replace good rows.
        EMPTY_FAIL_THRESHOLD = 3
        if len(empty_handles) >= EMPTY_FAIL_THRESHOLD:
            print(f"\nFAIL: {len(empty_handles)} Natchez collections returned "
                  f"zero products on first page — likely storefront drift:")
            for cal, h in empty_handles:
                print(f"  - {cal}: Natchez collection {h} returned zero products on first page")
            sys.exit(1)
        elif empty_handles:
            print(f"\nWARN: {len(empty_handles)} Natchez collection(s) returned "
                  f"zero products on first page (transient or worth investigating):")
            for cal, h in empty_handles:
                print(f"  - {cal}: Natchez collection {h} returned zero products on first page")

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
