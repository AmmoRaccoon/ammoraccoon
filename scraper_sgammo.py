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

RETAILER_SLUG = "sgammo"
SITE_BASE = "https://www.sgammo.com"

# SGAmmo category URLs per caliber. Values are lists for parity with
# the other CALIBER_PATHS scrapers (TrueShot/LG/BulkAmmo) — SGAmmo
# doesn't currently split any caliber across multiple collections, so
# each list has a single entry, but the per-handle loop in
# scrape_caliber relies on the list shape. Five entries below were
# renamed during a 2026-05-09-or-earlier storefront restructure: the
# prior values silently 404'd until the audit on 2026-05-09 caught
# the absence (5 of 10 configured URLs were dead, exact-match to the
# 5 calibers with implausibly low DB row counts). SGAmmo also changed
# the in-row price markup so that "$X.XX Each" now renders as
# "$ X.XX Each" (literal space between dollar sign and digits) — the
# regex fixes for that live further down in scrape_caliber.
CALIBER_PATHS = {
    '9mm':     ['/catalog/pistol-ammo-sale/9mm-luger-ammo/'],
    '380acp':  ['/catalog/pistol-ammo-sale/380-auto-ammo/'],
    '40sw':    ['/catalog/pistol-ammo-sale/40-cal-ammo/'],
    '38spl':   ['/catalog/pistol-ammo-sale/38-special-ammo/'],
    '357mag':  ['/catalog/pistol-ammo-sale/357-magnum-ammo/'],
    '22lr':    ['/catalog/rimfire-ammo-sale/22-lr-ammo/'],
    '223-556': ['/catalog/rifle-ammo-sale/223-556mm-ammo/'],
    '308win':  ['/catalog/rifle-ammo-sale/308-win-762x51-ammo/'],
    '762x39':  ['/catalog/rifle-ammo-sale/762x39-ammo/'],
    '300blk':  ['/catalog/rifle-ammo-sale/300-aac-blackout-ammo/'],
}

def get_retailer_id():
    result = supabase.table("retailers").select("id").eq("slug", RETAILER_SLUG).execute()
    if not result.data:
        print(f"ERROR: Retailer '{RETAILER_SLUG}' not found in database")
        return None
    return result.data[0]["id"]

def parse_grain(text):
    match = re.search(r'(\d+)[\s-]*gr(?:ain)?', text, re.IGNORECASE)
    return int(match.group(1)) if match else None

def parse_rounds(text):
    patterns = [
        r'(\d[\d,]*)\s*rounds?\s*(?:case|box|pack)?',
        r'(\d[\d,]*)\s*rd',
        r'(\d[\d,]*)\s*count',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
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
        'wolf': 'Russia', 'aguila': 'Mexico',
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
            print(f"  WARN: SGAmmo collection {handle} returned "
                  f"zero products on first page (caliber {caliber_norm}).")
            empty_first_page = True
            flags.append((handle, empty_first_page))
            continue
        time.sleep(8)

        rows = page.query_selector_all('table.sgammo-product-list__table tr')
        print(f"  Found {len(rows)} rows")
        if not rows:
            # Loud, grep-friendly line so the cause is obvious in CI
            # logs even when the run as a whole succeeds.
            print(f"  WARN: SGAmmo collection {handle} returned "
                  f"zero products on first page (caliber {caliber_norm}).")
            empty_first_page = True
            flags.append((handle, empty_first_page))
            continue

        for row in rows:
            try:
                text = row.inner_text().strip()
                if not text or 'Image' in text[:20] or 'Name' in text[:20]:
                    continue

                link_el = row.query_selector('a')
                if not link_el:
                    skipped += 1
                    continue

                name = link_el.inner_text().strip()
                product_url = link_el.get_attribute('href')
                if not product_url:
                    skipped += 1
                    continue

                # \s* between $ and the digits is permissive belt-and-
                # suspenders — Playwright's inner_text() strips the
                # &#36;-entity-then-space rendering down to "$0.28",
                # but if SGAmmo's CSS ever surfaces a literal space we
                # still match. Cheap insurance.
                cpr_match = re.search(r'\(\s*\$\s*(\d+\.\d+)\s+Per\s+Round\)', text, re.IGNORECASE)
                if not cpr_match:
                    skipped += 1
                    continue
                price_per_round = float(cpr_match.group(1))

                price_match = re.search(r'\$\s*(\d+\.?\d*)\s+Each', text)
                if not price_match:
                    skipped += 1
                    continue
                base_price = float(price_match.group(1))

                total_rounds = parse_rounds(name)
                if not total_rounds or total_rounds <= 0:
                    skipped += 1
                    continue

                if not sanity_check_ppr(price_per_round, base_price, total_rounds,
                                        context=f'{RETAILER_SLUG} {caliber_norm}', caliber=caliber_norm):
                    skipped += 1
                    continue

                grain = parse_grain(name)
                case_material = parse_case_material(name)
                bullet_type = parse_bullet_type(name)
                country = parse_country(name)
                manufacturer = parse_brand(name) or "Unknown"
                # Trailing slash on SGAmmo's product URLs would make
                # split('/')[-1] return '' for every row, collapsing
                # all variants onto a single empty-string product_id
                # and silently dedup-skipping every row past the first.
                # rstrip the slash before extracting the slug.
                product_id = product_url.rstrip('/').split('/')[-1]
                if not product_id or product_id in seen_ids:
                    skipped += 1
                    continue
                seen_ids.add(product_id)

                in_stock = '+' in text or 'in stock' in text.lower()
                purchase_limit = parse_purchase_limit(text)

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
    print(f"[{datetime.now()}] Starting SGAmmo scraper (all calibers)...")
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
    # fine; three or more is a strong signal that SGAmmo renamed
    # collection paths and the scraper is silently producing partial
    # data (the exact symptom that hid 5 of 10 calibers from the DB
    # until the 2026-05-09 audit). Exit non-zero so CI runs go red.
    EMPTY_FAIL_THRESHOLD = 3
    if len(empty_handles) >= EMPTY_FAIL_THRESHOLD:
        print(f"\nFAIL: {len(empty_handles)} SGAmmo collections returned "
              f"zero products on first page — likely storefront drift:")
        for cal, h in empty_handles:
            print(f"  - {cal}: SGAmmo collection {h} returned zero products on first page")
        sys.exit(1)
    elif empty_handles:
        print(f"\nWARN: {len(empty_handles)} SGAmmo collection(s) returned "
              f"zero products on first page (transient or worth investigating):")
        for cal, h in empty_handles:
            print(f"  - {cal}: SGAmmo collection {h} returned zero products on first page")

if __name__ == '__main__':
    scrape()
