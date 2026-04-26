import os
import re
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

from scraper_lib import (
    CALIBERS, now_iso, with_stock_fields, parse_purchase_limit,
    parse_brand, sanity_check_ppr, clean_title,
)

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "velocity"
SITE_BASE = "https://www.velocityammosales.com"

# WooCommerce store (the /collections/ paths threw the original
# implementation — they map to WC product_cat archives, not Shopify
# collections). Verified 2026-04-25 against the live nav.
CALIBER_PATHS = {
    '9mm':     '/collections/9-mm/',
    '380acp':  '/collections/380-acp/',
    '40sw':    '/collections/40-s-w/',
    '38spl':   '/collections/38-special/',
    '357mag':  '/collections/357-magnum/',
    '22lr':    '/collections/22-lr/',
    '223-556': '/collections/223/',
    '308win':  '/collections/308-win/',
    '762x39':  '/collections/7-62x39/',
    '300blk':  '/collections/300-blackout/',
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

def parse_bullet_type(text):
    text_upper = text.upper()
    for bt in ['FMJ', 'JHP', 'HP', 'OTM', 'TMJ', 'SP', 'FP']:
        if bt in text_upper:
            return bt
    if 'HOLLOW POINT' in text_upper:
        return 'JHP'
    if 'FULL METAL' in text_upper:
        return 'FMJ'
    return None

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
    url = SITE_BASE + CALIBER_PATHS[caliber_norm]
    print(f"\n[{caliber_norm}] Loading: {url}")
    try:
        resp = page.goto(url, wait_until='domcontentloaded', timeout=90000)
    except Exception as e:
        print(f"  goto failed: {e}")
        return 0, 0
    if resp and resp.status >= 400:
        print(f"  HTTP {resp.status} - skipping caliber.")
        return 0, 0
    time.sleep(6)

    # WooCommerce product cards live in <li class="product">. The
    # previous Shopify-flavored selectors matched nothing on this site
    # and the anchor fallback was grabbing every /products/ link in the
    # page (header nav, footer, related-products carousel) which is
    # why "found 246 candidates" but only 50 saved.
    products = page.query_selector_all('ul.products li.product, li.product')
    print(f"  Found {len(products)} product candidates")
    if not products:
        return 0, 0

    saved = 0
    skipped = 0

    for product in products:
        try:
            link_el = product.query_selector('a.woocommerce-LoopProduct-link, a.woocommerce-loop-product__link')
            if not link_el:
                link_el = product.query_selector('a[href*="/products/"]')
            if not link_el:
                skipped += 1
                continue

            href = link_el.get_attribute('href') or ''
            if not href or '?add-to-cart' in href:
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

            title_el = product.query_selector('.woocommerce-loop-product__title, h2, h3')
            raw_name = (title_el.inner_text().strip() if title_el
                        else (link_el.get_attribute('aria-label') or link_el.inner_text().strip()))
            # Velocity titles use HTML en-dashes as field separators
            # ("9mm – American Eagle 115gr FMJ") and typographic
            # apostrophes in brand names like "Pow'R-Ball" — clean_title
            # normalizes both to ASCII.
            name = clean_title(raw_name)
            if not name:
                skipped += 1
                continue

            # Real listing price lives in the .price .woocommerce-Price-amount
            # span. The card ALSO renders a "$0.32/rd" badge inside
            # .price-per-round-wrap that appears earlier in the DOM —
            # the previous regex on inner_text picked that up first
            # and stored it as the listing price, which is why every
            # saved row looked impossibly cheap.
            price_el = product.query_selector('.price .woocommerce-Price-amount, span.price .amount')
            base_price = None
            if price_el:
                price_text = (price_el.inner_text() or '').strip()
                m = re.search(r'\$?(\d{1,5}(?:,\d{3})*(?:\.\d{1,2})?)', price_text)
                if m:
                    try:
                        base_price = float(m.group(1).replace(',', ''))
                    except ValueError:
                        base_price = None
            if not base_price or base_price <= 0:
                # WooCommerce omits the .price span entirely on sold-out
                # listings — no current price to record. Surface the
                # reason so the totals are auditable.
                card_lower = (product.inner_text() or '').lower()
                reason = ('out of stock' if 'sold out' in card_lower or 'out of stock' in card_lower
                          else 'no price element')
                print(f"  Skipped ({reason}): {name[:55]}")
                skipped += 1
                continue

            # Cross-check / round-count derivation from the displayed
            # per-round badge when present — e.g. "$0.32/rd" pairs with
            # a $31.99 listing to imply 100 rounds (no need to parse
            # the title). Title fallback covers cards missing the badge.
            total_rounds = None
            cpr_el = product.query_selector('.price-per-round-wrap .badge-label, .price-per-round-wrap')
            if cpr_el:
                cpr_text = (cpr_el.inner_text() or '').strip()
                cpr_match = re.search(r'\$([0-9.]+)\s*/\s*rd', cpr_text, re.IGNORECASE)
                if cpr_match:
                    try:
                        cpr = float(cpr_match.group(1))
                        if cpr > 0:
                            total_rounds = round(base_price / cpr)
                    except ValueError:
                        total_rounds = None
            if not total_rounds:
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
            product_id = product_url.split('/products/')[-1].split('?')[0].rstrip('/')
            if not product_id or product_id in seen_ids:
                continue
            seen_ids.add(product_id)

            # Read card text once for stock + purchase-limit signals.
            card_text = (product.inner_text() or '')
            card_lower = card_text.lower()
            in_stock = ('out of stock' not in card_lower and
                        'sold out' not in card_lower and
                        'unavailable' not in card_lower)
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

    return saved, skipped


def scrape():
    print(f"[{datetime.now()}] Starting Velocity Ammo Sales scraper (all calibers)...")
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

    print(f"\nDone! Saved: {total_saved} | Skipped: {total_skipped}")

if __name__ == '__main__':
    scrape()
