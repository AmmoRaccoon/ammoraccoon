import asyncio
import os
import re
import sys
from datetime import datetime, timezone
from playwright.async_api import async_playwright
from supabase import create_client

from scraper_lib import CALIBERS, now_iso, with_stock_fields, parse_purchase_limit, sanity_check_ppr, parse_bullet_type as _shared_bullet_type, parse_brand, mark_retailer_scraped, normalize_caliber, insert_price_history, load_caliber_paths, category_redirected, report_empty_first_pages

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
RETAILER_ID = 11
SITE_BASE = "https://aeammo.com"

# Per-caliber category paths now live in caliber_paths/aeammo.json
# (expansion #4 Step-2 migration) — transcribed verbatim, parity-proven
# byte-identical. entry['url'] is a drop-in for the old path string.
# BigCommerce stencil. 22lr points at the /Ammo/Rimfire-Ammo PARENT
# (lists 22 WMR/Short/CB alongside 22 LR); the card loop drops off-
# caliber SKUs via normalize_caliber() at runtime. The .223 Rem leg of
# 223-556 has no category page (filter-only, 403), so only the 5.56 leg
# is configured. 308win + 762x39 are status=parked in the config
# (genuine catalog absence, PARKED 2026-06-12 — not URL drift); the
# loader returns active-only so they are not fetched, and their recovery
# slugs are preserved in the config for re-activation.
CALIBER_PATHS = load_caliber_paths('aeammo')

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

def parse_bullet_type(text):
    """AE Ammo's catalog is dominated by their house-brand FMJ practice
    line where titles often omit the bullet-type token entirely. Fall
    back to FMJ when the canonical parser can't decide — preserves the
    pre-migration assumption baked into AE Ammo's product mix.
    """
    bt = _shared_bullet_type(text)
    return bt if bt is not None else 'FMJ'


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

    for entry in CALIBER_PATHS[caliber_norm]:
        handle = entry['url']
        base = SITE_BASE + handle
        page_num = 1
        empty_first_page = False

        while True:
            url = base if page_num == 1 else f"{base}?page={page_num}"
            print(f"\n[{caliber_norm}/{handle}] page {page_num}: {url}")
            try:
                resp = await page.goto(url, wait_until='domcontentloaded', timeout=30000)
            except Exception as e:
                print(f"  goto failed: {e}")
                if page_num == 1:
                    empty_first_page = True
                    print(f"  WARN: AE Ammo collection {handle} returned "
                          f"zero products on first page (caliber {caliber_norm}).")
                break
            if resp and resp.status >= 400:
                print(f"  HTTP {resp.status} - skipping handle.")
                if page_num == 1:
                    empty_first_page = True
                    print(f"  WARN: AE Ammo collection {handle} returned "
                          f"zero products on first page (caliber {caliber_norm}).")
                break
            # Redirect guard (NEW 2026-06-14, expansion #4 Step-2): a
            # category that 200s but lands on a DIFFERENT page (the TSUSA
            # renumber trap) is skipped loudly and counts as an empty
            # first page, feeding the storefront-drift guardrail.
            if page_num == 1 and category_redirected(url, page.url):
                print(f"  REDIRECTED to {page.url} - skipping (category moved/renamed).")
                empty_first_page = True
                break
            await page.wait_for_timeout(4000)

            cards = await page.query_selector_all('li.product')
            if not cards:
                if page_num == 1:
                    empty_first_page = True
                    print(f"  WARN: AE Ammo collection {handle} returned "
                          f"zero products on first page (caliber {caliber_norm}).")
                else:
                    print(f"  No cards on page {page_num}, stopping handle.")
                break

            new_on_page = 0
            for card in cards:
                try:
                    title_el = await card.query_selector('h4.card-title a')
                    if not title_el:
                        continue
                    title = (await title_el.inner_text()).strip()
                    link = await title_el.get_attribute('href')
                    if link and not link.startswith('http'):
                        link = SITE_BASE + link

                    # Skip brand-carousel cards that BigCommerce stencil
                    # sometimes renders inside the product grid wrapper.
                    if link and '/brands/' in link:
                        continue

                    # Parent-URL bucketing guard. The /Ammo/Rimfire-Ammo
                    # handle for 22lr lists 22 WMR / 22 Short / 22 CB
                    # alongside true 22 LR; this filter drops them
                    # silently. Also a drift safety-net for the other
                    # caliber-specific handles — if a future AE Ammo
                    # restructure mixes calibers into one of those
                    # category pages, off-caliber listings are skipped
                    # instead of being misbucketed.
                    _, detected_norm = normalize_caliber(title)
                    if detected_norm != caliber_norm:
                        continue

                    price_el = await card.query_selector('.price--withoutTax')
                    if not price_el:
                        continue
                    price_text = (await price_el.inner_text()).strip()
                    price_match = re.search(r'\$?([\d,]+\.?\d*)', price_text.replace(',', ''))
                    if not price_match:
                        continue
                    price = float(price_match.group(1))

                    # AE Ammo (BigCommerce) marks OOS variants with a
                    # "Sold Out" label or .out-of-stock class on the card.
                    oos_el = await card.query_selector('.out-of-stock, .soldout, .sold-out')
                    card_text = (await card.inner_text())
                    card_lower = card_text.lower()
                    in_stock = oos_el is None and \
                               'out of stock' not in card_lower and \
                               'sold out' not in card_lower
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

            next_btn = await page.query_selector('a[rel="next"], .pagination-item--next a')
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

        # Storefront-drift guardrail (centralized 2026-06-14, expansion
        # #4 Step-2 — was an inline EMPTY_FAIL_THRESHOLD block): >= 3
        # collections empty on first page exits non-zero (CI red) and
        # skips the upsert + freshness bump so partial data can't replace
        # good rows and /status can't falsely advertise a fresh scrape.
        # 308win + 762x39 are now status=parked in the config, so they no
        # longer fetch or count as baseline empties.
        report_empty_first_pages(empty_handles, 'AE Ammo')

        mark_retailer_scraped(supabase, RETAILER_ID, had_success=(len(all_products) > 0))

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
                    insert_price_history(supabase, {
                        'listing_id': listing_id,
                        'price': product['base_price'],
                        'price_per_round': product['price_per_round'],
                        'in_stock': product['in_stock'],
                        'recorded_at': now,
                    })

            except Exception as e:
                print(f"  DB error for {product.get('manufacturer','?')}: {e}")

        print("Done.")

if __name__ == "__main__":
    asyncio.run(scrape())
