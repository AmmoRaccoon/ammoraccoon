import os
import re
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

from scraper_lib import (
    CALIBERS, now_iso, with_stock_fields, parse_purchase_limit, parse_brand,
    sanity_check_ppr, parse_bullet_type, mark_retailer_scraped, insert_price_history,
    load_caliber_paths, category_redirected, report_empty_first_pages,
)

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "ammunition-depot"
SITE_BASE = "https://www.ammunitiondepot.com"

# Per-caliber category URLs now live in caliber_paths/ammodeport.json
# (expansion #4 Step-2 migration) — transcribed verbatim, parity-proven
# byte-identical. Values are now LISTS of entries (one per caliber today)
# and entry['url'] is a drop-in for the old SITE_BASE + path string.
CALIBER_PATHS = load_caliber_paths('ammodeport')

def get_retailer_id():
    result = supabase.table("retailers").select("id").eq("slug", RETAILER_SLUG).execute()
    return result.data[0]["id"]

def parse_grain(text):
    match = re.search(r'(\d+)\s*gr(?:ain)?', text, re.IGNORECASE)
    return int(match.group(1)) if match else None

def parse_rounds(text):
    patterns = [
        r'(\d[\d,]*)\s*rounds?',
        r'(\d[\d,]*)\s*rd',
        r'(\d[\d,]*)\s*count',
        r'\((\d[\d,]*)\s*rounds?\)',
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
    # Redirect guard (NEW 2026-06-14, expansion #4 Step-2 — Ammunition
    # Depot had none before): a category that 301s to a DIFFERENT page
    # with HTTP 200 skips loudly and counts as an empty handle, feeding
    # the storefront-drift guardrail.
    if category_redirected(url, page.url):
        print(f"  REDIRECTED to {page.url} - skipping (category moved/renamed).")
        return 0, 0, True
    time.sleep(8)

    products = page.query_selector_all('.product-item')
    print(f"  Found {len(products)} products")
    if not products:
        return 0, 0, True

    saved = 0
    skipped = 0

    for product in products:
        try:
            name_el = product.query_selector('a.product-item-link')
            if not name_el:
                skipped += 1
                continue
            name = name_el.inner_text().strip()
            product_url = name_el.get_attribute('href')

            # Magento marks unavailable items with .stock.unavailable or
            # an "Out of Stock" label inside the product tile.
            card_text = product.inner_text()
            oos_el = product.query_selector('.stock.unavailable, .out-of-stock')
            in_stock = oos_el is None and 'out of stock' not in card_text.lower()
            purchase_limit = parse_purchase_limit(card_text)

            price_el = product.query_selector('span.rounds-price')
            if not price_el:
                skipped += 1
                continue

            price_text = price_el.inner_text().strip()
            price_matches = re.findall(r'\$(\d+\.\d+)', price_text)
            if not price_matches:
                skipped += 1
                continue

            cpr = float(price_matches[0])
            total_rounds = parse_rounds(name)
            if not total_rounds or total_rounds <= 0:
                skipped += 1
                continue

            base_price = round(cpr * total_rounds, 2)
            price_per_round = cpr
            if not sanity_check_ppr(price_per_round, base_price, total_rounds,
                                    context=f'{RETAILER_SLUG} {caliber_norm}', caliber=caliber_norm):
                skipped += 1
                continue
            grain = parse_grain(name)
            case_material = parse_case_material(name)
            bullet_type = parse_bullet_type(name)
            country = parse_country(name)
            manufacturer = parse_brand(name) or "Unknown"
            product_id = product_url.split('/')[-1].replace('.html', '') if product_url else name[:50]
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


def scrape_caliber(page, caliber_norm, caliber_display, retailer_id, seen_ids, empty_handles):
    """Scrape every configured category URL for a caliber (always a list).
    Appends (caliber, url) to empty_handles for any URL whose first page
    rendered zero product cards."""
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


def scrape():
    print(f"[{datetime.now()}] Starting Ammunition Depot scraper (all calibers)...")
    retailer_id = get_retailer_id()
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

    # Storefront-drift guardrail + freshness honesty (NEW 2026-06-14).
    # Ammunition Depot is Cloudflare-walled — had_success=(total_saved>0)
    # so a fully-403'd run no longer falsely bumps last_scraped_at.
    report_empty_first_pages(empty_handles, 'Ammunition Depot')
    mark_retailer_scraped(supabase, retailer_id, had_success=(total_saved > 0))
    print(f"\nDone! Saved: {total_saved} | Skipped: {total_skipped}")

if __name__ == '__main__':
    scrape()
