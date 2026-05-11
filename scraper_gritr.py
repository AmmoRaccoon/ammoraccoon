import os
import re
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

from scraper_lib import (
    CALIBERS, now_iso, with_stock_fields, parse_purchase_limit,
    parse_brand, sanity_check_ppr, parse_bullet_type,
    mark_retailer_scraped,
)

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "gritr"
SITE_BASE = "https://www.gritrsports.com"

# Verified 2026-04-25 against the live "Shop by Caliber" nav. Each
# value is a tuple: (path, optional title-filter regex). The title
# filter narrows mixed-caliber category pages — Gritr puts .357 Mag
# inside "other-handgun-calibers" alongside 10mm/.44/etc., and bundles
# all rimfire calibers (.17 HMR, .22 WMR, .22 LR) into a single
# /rimfire-ammo/ page; we drop rows whose title doesn't match.
CALIBER_PATHS = {
    '9mm':     ('/shooting/ammunition/handgun-ammo/9mm-luger-ammo/',     None),
    '380acp':  ('/shooting/ammunition/handgun-ammo/380-auto-ammo/',      None),
    '40sw':    ('/shooting/ammunition/handgun-ammo/40-s-w-ammo/',        None),
    '38spl':   ('/shooting/ammunition/handgun-ammo/38-specials-ammo/',   None),
    '357mag':  ('/shooting/ammunition/handgun-ammo/other-handgun-calibers/',
                re.compile(r'\b357\b|\.357\b', re.IGNORECASE)),
    '22lr':    ('/shooting/ammunition/rimfire-ammo/',
                re.compile(r'\b22\s*(?:LR|long\s*rifle)\b|\.22\s*LR\b|22LR', re.IGNORECASE)),
    '223-556': ('/shooting/ammunition/rifle-ammo/223-ammo/',              None),
    '308win':  ('/shooting/ammunition/rifle-ammo/308-7-62x51-ammo/',      None),
    '762x39':  ('/shooting/ammunition/rifle-ammo/7-62x39-ammo/',          None),
    '300blk':  ('/shooting/ammunition/rifle-ammo/300-aac-blackout/',      None),
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
        r'(\d[\d,]*)\s*rounds?',
        r'(\d[\d,]*)\s*rds?\b',
        r'(\d[\d,]*)\s*count',
        r'(\d[\d,]*)\s*per\s*box',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return int(match.group(1).replace(',', ''))
    return None

def parse_case_material(text):
    text_lower = text.lower()
    steel_brands = ['wolf', 'tula', 'tulammo', 'brown bear', 'silver bear', 'golden bear', 'barnaul']
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
    path, title_filter = CALIBER_PATHS[caliber_norm]
    url = SITE_BASE + path
    print(f"\n[{caliber_norm}] Loading: {url}")
    try:
        resp = page.goto(url, wait_until='domcontentloaded', timeout=90000)
    except Exception as e:
        print(f"  goto failed: {e}")
        return 0, 0
    if resp and resp.status >= 400:
        print(f"  HTTP {resp.status} - skipping caliber.")
        return 0, 0
    # Gritr renders its catalog through Searchspring's Snize widget,
    # which mounts its <li class="snize-product"> cards client-side
    # after the SPA boots. domcontentloaded fires on an empty grid;
    # wait_for_selector below blocks until the cards exist.
    try:
        page.wait_for_selector('li.snize-product', timeout=25000)
    except Exception:
        print(f"  no snize-product cards rendered after 25s - skipping caliber.")
        return 0, 0
    time.sleep(2)

    # Old selectors targeted BigCommerce's stencil theme (article.card,
    # li.product) and matched zero on Gritr's Snize-rendered grid.
    products = page.query_selector_all('li.snize-product')
    print(f"  Found {len(products)} products")
    if not products:
        return 0, 0

    saved = 0
    skipped = 0

    for product in products:
        try:
            # Title link — Snize wraps the whole card in <a class="snize-view-link">
            # whose href is the product URL and whose aria-label/title
            # carries the FULL product name (the .snize-title <span>
            # truncates after 2 lines so reading from there can chop
            # the round-count suffix off long titles).
            link_el = product.query_selector('a.snize-view-link') or product.query_selector('a[href]')
            if not link_el:
                skipped += 1
                continue
            href = link_el.get_attribute('href') or ''
            if not href:
                skipped += 1
                continue
            product_url = href if href.startswith('http') else SITE_BASE + href

            # Skip brand-carousel cards that occasionally render inside
            # the product grid wrapper. They look like products to the
            # price/round regex (promo banner + carousel number) and
            # otherwise get saved with the loop's caliber tag attached.
            if '/brands/' in product_url:
                skipped += 1
                continue

            name = ((link_el.get_attribute('aria-label') or link_el.get_attribute('title') or '').strip())
            if not name:
                title_el = product.query_selector('.snize-title')
                name = (title_el.inner_text().strip() if title_el else '')
            if not name:
                skipped += 1
                continue

            # Title-based caliber filter for the mixed-caliber category
            # pages (357mag → other-handgun-calibers, 22lr → all rimfire).
            if title_filter and not title_filter.search(name):
                skipped += 1
                continue

            # Skip non-ammo trainers that surface in some categories.
            lname = name.lower()
            if any(k in lname for k in ['snap cap', 'snap caps', 'dummy round', 'dummy rounds', 'a-zoom', 'azoom']):
                skipped += 1
                print(f"  Skipped (not real ammo): {name[:55]}")
                continue

            # Out-of-stock detection — Snize omits the snize-in-stock
            # badge and adds snize-product-out-of-stock to the wrapper.
            wrapper_class = product.get_attribute('class') or ''
            in_stock = ('snize-product-out-of-stock' not in wrapper_class)

            # Listing price lives in <span class="snize-price money">.
            price_el = product.query_selector('.snize-price')
            base_price = None
            if price_el:
                price_text = (price_el.inner_text() or '').strip()
                m = re.search(r'\$(\d{1,5}(?:,\d{3})*(?:\.\d{1,2})?)', price_text)
                if m:
                    try:
                        base_price = float(m.group(1).replace(',', ''))
                    except ValueError:
                        base_price = None
            if not base_price or base_price <= 0:
                skipped += 1
                continue

            # Round count comes from the title — Gritr/Snize doesn't
            # render a per-round badge or variant grid on the listing
            # tile. Titles consistently include "50 Round Box",
            # "20 Round" or similar.
            total_rounds = parse_rounds(name)
            if not total_rounds or total_rounds <= 0:
                skipped += 1
                continue

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
            product_id = product_url.rstrip('/').split('/')[-1]
            if not product_id or product_id in seen_ids:
                continue
            seen_ids.add(product_id)

            # in_stock already derived from the wrapper class above.
            # purchase_limit not surfaced on the Gritr tile — leave None.
            purchase_limit = None

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

    return saved, skipped


def scrape():
    print(f"[{datetime.now()}] Starting Gritr Sports scraper (all calibers)...")
    retailer_id = get_retailer_id()
    if not retailer_id:
        return

    print(f"Retailer ID: {retailer_id}")

    total_saved = 0
    total_skipped = 0
    seen_ids = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })

        for caliber_norm in CALIBER_PATHS:
            caliber_display = CALIBERS[caliber_norm]
            saved, skipped = scrape_caliber(page, caliber_norm, caliber_display, retailer_id, seen_ids)
            total_saved += saved
            total_skipped += skipped

        browser.close()

    mark_retailer_scraped(supabase, retailer_id)
    print(f"\nDone! Saved: {total_saved} | Skipped: {total_skipped}")

if __name__ == '__main__':
    scrape()
