import asyncio
import os
import re
import sys
from datetime import datetime, timezone
from playwright.async_api import async_playwright
from supabase import create_client

from scraper_lib import CALIBERS, now_iso, with_stock_fields, parse_purchase_limit, sanity_check_ppr, parse_bullet_type, parse_brand, mark_retailer_scraped

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
RETAILER_ID = 17
SITE_BASE = "https://www.wideners.com"

# Wideners is Cloudflare-fronted. A local-IP Playwright probe on
# 2026-05-15 confirmed the site is fully reachable and parseable with
# the existing selectors, but the production GH Actions cron has been
# silent since 2026-04-22 — consistent with Cloudflare flagging the
# Actions runner IP ranges and not regular browser IPs. Following the
# Gunbuyer pattern: a fresh browser context per pagination page
# sidesteps Cloudflare's session-aware throttle so each request lands
# on a clean cookie/TLS handshake. See
# ammoraccoon-web/reports/scraper-investigation-2026-05-15.md.
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
EXTRA_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Upgrade-Insecure-Requests": "1",
}

# DRY_RUN=1 skips supabase writes (upsert + price_history insert +
# mark_retailer_scraped) so the scraper can be smoke-tested against
# the live storefront without touching the production database.
DRY_RUN = bool(os.environ.get('DRY_RUN'))

CALIBER_PATHS = {
    '9mm':     '/handgun/9mm-ammo',
    '380acp':  '/handgun/380-auto-ammo',
    '40sw':    '/handgun/40-cal-ammo',
    '38spl':   '/handgun/38-special-ammo',
    '357mag':  '/handgun/357-magnum-ammo',
    '22lr':    '/rimfire/22-lr-ammo',
    '223-556': '/rifle/223-5.56-ammo',
    '308win':  '/rifle/308-ammo',
    '762x39':  '/rifle/7.62x39mm-ammo',
    '300blk':  '/rifle/300-blackout-ammo',
}

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def parse_rounds(title):
    m = re.search(r'(\d[\d,]*)\s*rounds?\b', title, re.IGNORECASE)
    if m:
        return int(m.group(1).replace(',', ''))
    case = re.search(r'(\d+)\s*(?:rd|rds|rounds?)\s*case', title, re.IGNORECASE)
    if case:
        return int(case.group(1))
    box = re.search(r'(\d+)\s*(?:rd|rds)\s*box', title, re.IGNORECASE)
    if box:
        return int(box.group(1))
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


def parse_condition(title):
    t = title.lower()
    if 'reman' in t or 'remanufactured' in t:
        return 'Remanufactured'
    return 'New'

async def _open_context_and_goto(browser, url):
    """Open `url` in a fresh browser context and return (context, page, response).

    A fresh context per request sidesteps Cloudflare's session-aware
    throttle — every page load lands on a clean cookie/TLS handshake,
    so a single flagged session can't poison the next request. Caller
    is responsible for `await context.close()`.
    """
    context = await browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1366, "height": 2400},
        locale="en-US",
        extra_http_headers=EXTRA_HEADERS,
    )
    page = await context.new_page()
    resp = await page.goto(url, wait_until='domcontentloaded', timeout=60000)
    return context, page, resp


async def scrape_caliber(browser, caliber_norm, caliber_display, seen_ids):
    base = SITE_BASE + CALIBER_PATHS[caliber_norm]
    products = []
    page_num = 1

    while True:
        url = base if page_num == 1 else f"{base}?p={page_num}"
        print(f"\n[{caliber_norm}] page {page_num}: {url}")
        try:
            context, page, resp = await _open_context_and_goto(browser, url)
        except Exception as e:
            print(f"  goto failed: {e}")
            break
        try:
            if resp and resp.status >= 400:
                print(f"  HTTP {resp.status} - skipping caliber.")
                break
            try:
                await page.wait_for_selector('#products-list li.item', timeout=30000)
            except Exception:
                print(f"  no main product list on page {page_num}, stopping caliber.")
                break
            await page.wait_for_timeout(2500)

            cards = await page.query_selector_all('#products-list > li.item')
            if not cards:
                break

            new_on_page = 0
            for card in cards:
                try:
                    slug = await card.get_attribute('id')
                    title_el = await card.query_selector('h2.product-name')
                    if not slug or not title_el:
                        continue
                    title = (await title_el.inner_text()).strip()

                    price_el = await card.query_selector('.price-box .special-price .price')
                    if not price_el:
                        price_el = await card.query_selector('.price-box .regular-price .price')
                    if not price_el:
                        price_el = await card.query_selector('.price-box .price')
                    if not price_el:
                        continue
                    price_text = (await price_el.inner_text()).strip()
                    pm = re.search(r'\$?([\d,]+\.\d{2})', price_text.replace(',', ''))
                    if not pm:
                        continue
                    price = float(pm.group(1))

                    in_stock_el = await card.query_selector('.availability.in-stock')
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
                    # price is the displayed box/case dollar amount and rounds is
                    # the per-listing count, so ppr is dollars per round. The
                    # sanity guard catches any future regression that slips a
                    # cents-as-dollars value (or the inverse) back in.
                    ppr = round(price / rounds, 4)
                    if not sanity_check_ppr(ppr, price, rounds, context=title[:60], caliber=caliber_norm):
                        continue

                    product_id = slug[:100]
                    if product_id in seen_ids:
                        continue
                    seen_ids.add(product_id)

                    link = f"{base}#{slug}"

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
                    print(f"  Error on card: {e}")
                    continue

            if new_on_page == 0:
                break

            next_link = await page.query_selector('a.next[title="Next"]')
            if not next_link:
                break
            page_num += 1
            if page_num > 30:
                break
        finally:
            await context.close()
    return products


async def scrape():
    if DRY_RUN:
        print("[DRY_RUN] Supabase writes will be skipped.")
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--disable-blink-features=AutomationControlled'],
        )

        all_products = []
        seen_ids = set()
        empty_handles = []

        for caliber_norm in CALIBER_PATHS:
            caliber_display = CALIBERS[caliber_norm]
            products = await scrape_caliber(browser, caliber_norm, caliber_display, seen_ids)
            all_products.extend(products)
            if not products:
                empty_handles.append((caliber_norm, CALIBER_PATHS[caliber_norm]))

        await browser.close()

        print(f"\nTotal scraped: {len(all_products)}")

        # Storefront-drift / Cloudflare-wall guardrail — same
        # EMPTY_FAIL_THRESHOLD pattern as scraper_ammocom /
        # scraper_natchez / scraper_aeammo / scraper_freedommunitions.
        # 3+ calibers returning zero products is the signal that
        # something is fundamentally broken; exit non-zero so CI goes
        # red AND skip mark_retailer_scraped() so /status doesn't
        # falsely advertise a fresh scrape.
        EMPTY_FAIL_THRESHOLD = 3
        if len(empty_handles) >= EMPTY_FAIL_THRESHOLD:
            print(f"\nFAIL: {len(empty_handles)} Wideners calibers returned "
                  f"zero products — likely Cloudflare wall or storefront drift:")
            for cal, path in empty_handles:
                print(f"  - {cal}: {path}")
            sys.exit(1)
        elif empty_handles:
            print(f"\nWARN: {len(empty_handles)} Wideners caliber(s) returned "
                  f"zero products (transient or worth investigating):")
            for cal, path in empty_handles:
                print(f"  - {cal}: {path}")

        if not all_products:
            print("Nothing to upsert.")
            if not DRY_RUN:
                mark_retailer_scraped(supabase, RETAILER_ID, had_success=False)
            return

        if DRY_RUN:
            print(f"[DRY_RUN] Would upsert {len(all_products)} listings + price_history rows.")
            return

        now = now_iso()
        upserted = 0
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
                    upserted += 1

            except Exception as e:
                print(f"  DB error for {product.get('manufacturer','?')}: {e}")

        # had_success=(upserted > 0) so a wall-blocked scrape doesn't
        # falsely bump last_scraped_at. See
        # scraper_lib.mark_retailer_scraped docstring.
        mark_retailer_scraped(supabase, RETAILER_ID, had_success=(upserted > 0))

        print(f"Done. Upserted: {upserted}")

if __name__ == "__main__":
    asyncio.run(scrape())
