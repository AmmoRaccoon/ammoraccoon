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
)

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "outdoorlimited"
SITE_BASE = "https://www.outdoorlimited.com"

# Outdoor Limited paths — verified 2026-04-25 against the live homepage
# nav. All [type]-ammo/[caliber]-ammo, but the .380 slug is "auto"
# rather than "acp" (the only entry the previous map got wrong).
CALIBER_PATHS = {
    '9mm':     '/handgun-ammo/9mm-ammo/',
    '380acp':  '/handgun-ammo/380-auto-ammo/',
    '40sw':    '/handgun-ammo/40-s-w-ammo/',
    '38spl':   '/handgun-ammo/38-special-ammo/',
    '357mag':  '/handgun-ammo/357-magnum-ammo/',
    '22lr':    '/rimfire-ammo/22-lr-ammo/',
    '223-556': '/rifle-ammo/223-remington-ammo/',
    '308win':  '/rifle-ammo/308-win-ammo/',
    '762x39':  '/rifle-ammo/7-62x39mm-ammo/',
    '300blk':  '/rifle-ammo/300-aac-blackout-ammo/',
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
    # Outdoor Limited renders the product grid client-side — products
    # only mount after a few seconds of JS. domcontentloaded fires on
    # an empty grid, so we explicitly wait for the title links to
    # appear. The wrapper class is .row_inner (.v-product__img and
    # .v-product__title are children, no bare .v-product class
    # exists). 25s is generous; most pages settle in 5-10.
    try:
        page.wait_for_selector('.v-product__title', timeout=25000)
    except Exception:
        print(f"  no .v-product__title cards rendered after 25s - skipping caliber.")
        return 0, 0
    time.sleep(2)

    # Each card is a .row_inner wrapper containing image link, title
    # link, total price, AND a displayed "per round" line — Outdoor
    # Limited surfaces CPR directly, which lets us back-derive an
    # exact round count instead of guessing from the title.
    products = page.query_selector_all('.row_inner')
    print(f"  Found {len(products)} products")
    if not products:
        return 0, 0

    saved = 0
    skipped = 0

    for product in products:
        try:
            # Title + URL — both live on the .v-product__title anchor.
            link_el = product.query_selector('a.v-product__title')
            if not link_el:
                # Older theme variant: title is inside .column_name.
                link_el = product.query_selector('.column_name a')
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

            name = ((link_el.inner_text() or '').strip()
                    or link_el.get_attribute('title') or '').strip()

            # Total price from the dedicated span — sidesteps regex
            # collisions with the "Price per round $0.27" line that
            # also lives in the card. Fall back to a regex on the
            # price-block container if the span moves.
            price_el = product.query_selector('.product_productprice .price')
            if price_el:
                base_text = (price_el.inner_text() or '').strip()
            else:
                price_block = product.query_selector('.product_productprice') or product
                base_text = (price_block.inner_text() or '').strip()
            price_match = re.search(r'\$(\d{1,4}(?:,\d{3})*(?:\.\d{1,2})?)', base_text)
            if not price_match:
                skipped += 1
                continue
            base_price = float(price_match.group(1).replace(',', ''))
            if base_price <= 0:
                skipped += 1
                continue

            # Displayed per-round price → exact round count, no
            # title-parsing required for the common case. Falls back
            # to title parsing when the CPR line is missing.
            cpr_el = product.query_selector('.price_per_round')
            displayed_cpr = None
            if cpr_el:
                cpr_text = (cpr_el.inner_text() or '').strip()
                cpr_match = re.search(r'\$([0-9.]+)', cpr_text)
                if cpr_match:
                    try:
                        displayed_cpr = float(cpr_match.group(1))
                    except ValueError:
                        displayed_cpr = None

            total_rounds = None
            if displayed_cpr and displayed_cpr > 0:
                total_rounds = round(base_price / displayed_cpr)
            if not total_rounds or total_rounds <= 0:
                total_rounds = parse_rounds(name) or parse_rounds(product.inner_text() or '')
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

            # Pull the rendered card text once so we can derive both
            # stock state and purchase-limit hints. .proStk surfaces
            # "5 in stock!" / "out of stock" copy.
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
    print(f"[{datetime.now()}] Starting Outdoor Limited scraper (all calibers)...")
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
