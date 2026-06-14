import os
import re
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

from scraper_lib import (
    CALIBERS, normalize_caliber, now_iso, with_stock_fields, parse_purchase_limit,
    parse_brand, sanity_check_ppr, parse_bullet_type, mark_retailer_scraped,
    insert_price_history,
    load_caliber_paths, category_redirected, report_empty_first_pages,
)

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "target-sports"
SITE_BASE = "https://www.targetsportsusa.com"

# Per-caliber category URLs now live in caliber_paths/targetsports.json
# (expansion #4 Step-2 migration) — transcribed verbatim, parity-proven
# byte-identical before flip. The 2026-06-12 TSUSA category-renumber fix
# (commit e9d7c98) lives in that config now; the old IDs had 301'd to
# WRONG calibers with HTTP 200 (.223->.44 Rem Mag, .22 LR->.44 Special),
# which the redirect guard below now catches. Values stay LISTS: TSUSA
# splits .223 Rem vs 5.56 NATO and .308 Win vs 7.62x51 NATO into
# separate pages; normalize_caliber buckets both halves into one
# normalized caliber and seen_ids dedups cross-listed SKUs.
# entry['url'] is a drop-in for the old SITE_BASE + path string.
CALIBER_PATHS = load_caliber_paths('targetsports')

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
    patterns = [
        r'(\d[\d,]*)\s*rounds?',
        r'(\d[\d,]*)\s*rds',
        r'(\d[\d,]*)/box',
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
        'wolf': 'Russia', 'aguila': 'Mexico', 'pmc': 'South Korea',
        'geco': 'Germany', 'lapua': 'Finland', 'norma': 'Sweden',
    }
    for keyword, country in mapping.items():
        if keyword in text_lower:
            return country
    return None

def scrape_caliber(page, caliber_norm, caliber_display, retailer_id, seen_ids, empty_handles):
    """Scrape every configured category URL for a caliber (always a list).
    Appends (caliber, url) to empty_handles for any URL whose first page
    rendered zero candidate links."""
    saved = 0
    skipped = 0
    for entry in CALIBER_PATHS[caliber_norm]:
        s, k, empty = scrape_category_page(page, entry, caliber_norm,
                                           caliber_display, retailer_id, seen_ids)
        saved += s
        skipped += k
        if empty:
            empty_handles.append((caliber_norm, entry['url']))
    return saved, skipped


def scrape_category_page(page, entry, caliber_norm, caliber_display, retailer_id, seen_ids):
    url = SITE_BASE + entry['url']
    print(f"\n[{caliber_norm}] Loading: {url}")
    try:
        resp = page.goto(url, wait_until='domcontentloaded', timeout=90000)
    except Exception as e:
        print(f"  goto failed: {e}")
        return 0, 0, True
    if resp and resp.status >= 400:
        print(f"  HTTP {resp.status} - skipping caliber.")
        return 0, 0, True
    # Redirect guard (shared scraper_lib.category_redirected as of the
    # 2026-06-14 caliber-paths migration) — 2026-06 incident: TSUSA
    # renumbered its category IDs and the old URLs 301'd to OTHER
    # calibers' pages with a 200, so the caliber gate silently saved
    # zero for 9 of 10 calibers. A category URL that navigates anywhere
    # but itself is the wrong page; skip loudly so the failure shows up
    # in the run log and feeds the storefront-drift guardrail.
    if category_redirected(url, page.url):
        print(f"  REDIRECTED to {page.url} - category ID likely renumbered "
              f"again; skipping this page instead of scraping wrong ammo.")
        return 0, 0, True
    print("  Waiting for products to load...")
    time.sleep(20)

    products = page.query_selector_all('li a[href*="-p-"]')
    print(f"  Found {len(products)} candidate links")
    if not products:
        return 0, 0, True

    saved = 0
    skipped = 0

    for product in products:
        try:
            product_url = product.get_attribute('href')
            if not product_url:
                continue

            if not product_url.startswith('http'):
                product_url = SITE_BASE + product_url

            text = product.inner_text().strip()
            if not text:
                continue

            name_el = product.query_selector('h2')
            if not name_el:
                skipped += 1
                continue
            name = name_el.inner_text().strip()

            # Strict caliber gate. Target Sports collection pages
            # cross-pollinate (e.g. .44 Rem Mag and .45 GAP appearing
            # on the 223-556 / 300blk pages). normalize_caliber returns
            # (None, None) for off-list cartridges, so we skip those
            # entirely instead of silently bucketing them under the
            # collection's caliber. Mirrors the gate added to
            # scraper_trueshot 2026-05-08.
            _, detected = normalize_caliber(name)
            if detected != caliber_norm:
                skipped += 1
                continue

            text_lower = text.lower()
            in_stock = 'out of stock' not in text_lower and \
                       'sold out' not in text_lower and \
                       'backordered' not in text_lower
            purchase_limit = parse_purchase_limit(text)

            cpr_match = re.search(r'\$(\d+\.\d+)\s*Per\s*Round', text, re.IGNORECASE)
            if not cpr_match:
                skipped += 1
                continue
            price_per_round = float(cpr_match.group(1))

            price_matches = re.findall(r'\$(\d+\.?\d*)', text)
            prices = [float(p) for p in price_matches if float(p) > 1]
            if not prices:
                skipped += 1
                continue
            base_price = min(prices)

            total_rounds = parse_rounds(name)
            if not total_rounds and price_per_round > 0:
                # Derive from base_price / ppr only when the result is
                # a credible box size. When the listing card surfaces
                # only the per-round price, base_price collapses onto
                # price_per_round and the division rounds to 1 — a stub
                # the user can't actually buy. Require >= 2.
                derived = round(base_price / price_per_round)
                if derived >= 2:
                    total_rounds = derived
            if not total_rounds:
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
            product_id = product_url.split('/')[-1].replace('.aspx', '')
            if product_id in seen_ids:
                continue
            seen_ids.add(product_id)

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

            insert_price_history(supabase, {
                'listing_id': result.data[0]['id'],
                'price': base_price,
                'price_per_round': price_per_round,
                'in_stock': in_stock,
            })

            saved += 1
            print(f"  Saved [{caliber_norm}]: {name[:55]} | ${base_price} | {price_per_round}/rd")

        except Exception as e:
            skipped += 1
            print(f"  Skipped: {e}")
            continue

    return saved, skipped, False


def scrape():
    print(f"[{datetime.now()}] Starting Target Sports USA scraper (all calibers)...")
    retailer_id = get_retailer_id()
    if not retailer_id:
        return

    print(f"Retailer ID: {retailer_id}")

    total_saved = 0
    total_skipped = 0
    seen_ids = set()
    empty_handles = []  # (caliber, url) for the storefront-drift guardrail

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })

        for caliber_norm in CALIBER_PATHS:
            caliber_display = CALIBERS[caliber_norm]
            saved, skipped = scrape_caliber(page, caliber_norm, caliber_display, retailer_id, seen_ids, empty_handles)
            total_saved += saved
            total_skipped += skipped

        browser.close()

    # Storefront-drift guardrail + freshness honesty (NEW 2026-06-14,
    # expansion #4 Step-2 — TSUSA had a redirect guard but no empty-fail
    # or had_success before).
    report_empty_first_pages(empty_handles, 'Target Sports USA')
    mark_retailer_scraped(supabase, retailer_id, had_success=(total_saved > 0))
    print(f"\nDone! Saved: {total_saved} | Skipped: {total_skipped}")

if __name__ == '__main__':
    scrape()
