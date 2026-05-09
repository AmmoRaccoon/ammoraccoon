import os
import re
import sys
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

from scraper_lib import CALIBERS, now_iso, with_stock_fields, parse_purchase_limit, parse_brand, sanity_check_ppr, parse_bullet_type

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "lucky-gunner"
SITE_BASE = "https://www.luckygunner.com"

# Lucky Gunner caliber category paths. Values are lists because some
# calibers map to multiple categories — Lucky Gunner splits .223 and
# 5.56 into separate collection pages, mirroring the TrueShot pattern.
# Five entries below were renamed during a 2026-05-09-or-earlier
# storefront restructure: the prior values silently 404'd and produced
# empty pages until the audit on 2026-05-09 caught the absence (5 of
# 10 configured URLs were dead, exact-match to the 5 missing calibers
# in the DB).
CALIBER_PATHS = {
    '9mm':     ['/handgun/9mm-ammo?show=100'],
    '380acp':  ['/handgun/380-auto-ammo?show=100'],
    '40sw':    ['/handgun/40-s-w-ammo?show=100'],
    '38spl':   ['/handgun/38-special-ammo?show=100'],
    '357mag':  ['/handgun/357-magnum-ammo?show=100'],
    '22lr':    ['/rimfire/22-lr-ammo?show=100'],
    '223-556': ['/rifle/223-remington-ammo?show=100',
                '/rifle/5.56x45-ammo?show=100'],
    '308win':  ['/rifle/308-ammo?show=100'],
    '762x39':  ['/rifle/7.62x39mm-ammo?show=100'],
    '300blk':  ['/rifle/300-blackout-ammo?show=100'],
}

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
    steel_brands = ['wolf', 'tula', 'tulammo', 'brown bear', 'silver bear', 'golden bear']
    if any(brand in text_lower for brand in steel_brands):
        return 'Steel'
    if 'steel' in text_lower:
        return 'Steel'
    elif 'brass' in text_lower:
        return 'Brass'
    elif 'aluminum' in text_lower:
        return 'Aluminum'
    elif 'nickel' in text_lower:
        return 'Nickel'
    return 'Brass'


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

def scrape_caliber(page, caliber_norm, caliber_display, retailer_id, seen_ids):
    """Scrape every configured handle for a caliber.

    Returns (saved, skipped, flags) where flags is a list of
    (handle, empty_first_page) tuples. The orchestrator in scrape()
    uses the flags to fire the storefront-drift guardrail when too
    many handles silently return zero products on first load.
    """
    saved = 0
    skipped = 0
    flags = []

    for handle in CALIBER_PATHS[caliber_norm]:
        url = SITE_BASE + handle
        empty_first_page = False
        print(f"\n[{caliber_norm}/{handle}] Loading: {url}")
        try:
            resp = page.goto(url, wait_until='domcontentloaded', timeout=90000)
        except Exception as e:
            print(f"  goto failed: {e}")
            empty_first_page = True
            flags.append((handle, empty_first_page))
            continue
        if resp and resp.status >= 400:
            print(f"  HTTP {resp.status} — handle unreachable.")
            print(f"  WARN: Lucky Gunner collection {handle} returned "
                  f"zero products on first page (caliber {caliber_norm}).")
            empty_first_page = True
            flags.append((handle, empty_first_page))
            continue
        time.sleep(8)

        products = page.query_selector_all('li.item')
        if not products:
            products = page.query_selector_all('.product-item, li[class*="item"]')
        print(f"  Found {len(products)} products")
        if not products:
            # Loud, grep-friendly line so the cause is obvious in CI
            # logs even when the run as a whole succeeds.
            print(f"  WARN: Lucky Gunner collection {handle} returned "
                  f"zero products on first page (caliber {caliber_norm}).")
            empty_first_page = True
            flags.append((handle, empty_first_page))
            continue

        for product in products:
            try:
                name_el = product.query_selector('h2 a, h3 a, .product-name a, a.product-name')
                if not name_el:
                    skipped += 1
                    continue

                name = name_el.inner_text().strip()
                product_url = name_el.get_attribute('href')

                price_el = product.query_selector('.price, [class*="price"]')
                if not price_el:
                    skipped += 1
                    continue

                price_text = price_el.inner_text().strip()
                price_matches = re.findall(r'\$(\d+\.?\d*)', price_text)
                if not price_matches:
                    skipped += 1
                    continue

                base_price = float(price_matches[-1])

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
                    skipped += 1
                    continue

                if not price_per_round:
                    price_per_round = round(base_price / total_rounds, 4)

                if not sanity_check_ppr(price_per_round, base_price, total_rounds,
                                        context=f'{RETAILER_SLUG} {caliber_norm}', caliber=caliber_norm):
                    skipped += 1
                    continue

                grain = parse_grain(name)
                case_material = parse_case_material(name)
                bullet_type = parse_bullet_type(name)
                country = parse_country(name)
                manufacturer = parse_brand(name) or "Unknown"
                product_id = product_url.split('/')[-1] if product_url else name[:50]
                if product_id in seen_ids:
                    continue
                seen_ids.add(product_id)

                card_text = product.inner_text()
                card_lower = card_text.lower()
                stock_el = product.query_selector('.in-stock, .availability')
                if stock_el:
                    in_stock = 'in stock' in stock_el.inner_text().lower()
                else:
                    # Fallback: look for OOS copy anywhere on the tile.
                    in_stock = 'out of stock' not in card_lower and \
                               'sold out' not in card_lower
                purchase_limit = parse_purchase_limit(card_text)

                listing = {
                    'retailer_id': retailer_id,
                    'retailer_product_id': product_id,
                    'product_url': product_url,
                    'caliber': caliber_display,
                    'caliber_normalized': caliber_norm,
                    'grain': grain,
                    'bullet_type': bullet_type,
                    'case_material': case_material,
                    'condition_type': 'New',
                    'country_of_origin': country,
                    'manufacturer': manufacturer,
                    'rounds_per_box': total_rounds,
                    'boxes_per_case': 1,
                    'total_rounds': total_rounds,
                    'base_price': base_price,
                    'price_per_round': price_per_round,
                    'purchase_limit': purchase_limit,
                    'last_updated': now_iso(),
                }
                with_stock_fields(listing, in_stock)

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

                saved += 1
                print(f"  Saved [{caliber_norm}]: {name[:55]} | ${base_price} | {price_per_round}/rd")

            except Exception as e:
                skipped += 1
                print(f"  Skipped: {e}")
                continue

        flags.append((handle, empty_first_page))
    return saved, skipped, flags


def scrape():
    print(f"[{datetime.now()}] Starting Lucky Gunner scraper (all calibers)...")
    retailer_id = get_retailer_id()
    if not retailer_id:
        return

    print(f"Retailer ID: {retailer_id}")

    total_saved = 0
    total_skipped = 0
    seen_ids = set()
    empty_handles = []  # list of (caliber_norm, handle) for guardrail

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })

        for caliber_norm in CALIBER_PATHS:
            caliber_display = CALIBERS[caliber_norm]
            saved, skipped, flags = scrape_caliber(page, caliber_norm, caliber_display, retailer_id, seen_ids)
            total_saved += saved
            total_skipped += skipped
            for handle, empty in flags:
                if empty:
                    empty_handles.append((caliber_norm, handle))

        browser.close()

    print(f"\nDone! Saved: {total_saved} | Skipped: {total_skipped}")

    # Storefront-drift guardrail. A single transient empty handle is
    # fine; three or more is a strong signal that Lucky Gunner renamed
    # collection paths and the scraper is silently producing partial
    # data (the exact symptom that hid 5 of 10 calibers from the DB
    # until the 2026-05-09 audit). Exit non-zero so CI runs go red.
    EMPTY_FAIL_THRESHOLD = 3
    if len(empty_handles) >= EMPTY_FAIL_THRESHOLD:
        print(f"\nFAIL: {len(empty_handles)} Lucky Gunner collections returned "
              f"zero products on first page — likely storefront drift:")
        for cal, h in empty_handles:
            print(f"  - {cal}: Lucky Gunner collection {h} returned zero products on first page")
        sys.exit(1)
    elif empty_handles:
        print(f"\nWARN: {len(empty_handles)} Lucky Gunner collection(s) returned "
              f"zero products on first page (transient or worth investigating):")
        for cal, h in empty_handles:
            print(f"  - {cal}: Lucky Gunner collection {h} returned zero products on first page")

if __name__ == '__main__':
    scrape()
