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

RETAILER_SLUG = "recoilgunworks"
SITE_BASE = "https://recoilgunworks.com"

# BigCommerce store. Verified 2026-04-25 by following each redirect
# from the live "Shop by Caliber" nav and probing slug variants. RGW
# does NOT carry .308 Win or .300 BLK as dedicated category pages
# (every plausible slug 404'd) so they're omitted — re-add later if
# RGW expands inventory. Most categories use an "-ammo" suffix; .357
# Magnum and .40 S&W are the inconsistent exceptions.
CALIBER_PATHS = {
    '9mm':     '/ammo/handgun-ammo/9mm/',
    '380acp':  '/ammo/handgun-ammo/380-auto-ammo/',
    '40sw':    '/ammo/handgun-ammo/40-s-w/',
    '38spl':   '/ammo/handgun-ammo/38-special-ammo/',
    '357mag':  '/ammo/handgun-ammo/357-magnum/',
    '22lr':    '/ammo/rimfire-ammo/22lr/',
    '223-556': '/ammo/rifle-ammo/223-5-56/',
    '762x39':  '/ammo/rifle-ammo/7-62x39mm-ammo/',
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
    # Round-count parsing. RGW titles often include manufacturer SKUs
    # in parens like "(5200)" or "(P9HST2)" — those are NOT round
    # counts and the previous regex matched them, divided $13.99 by
    # 5200, then ate the listing on the [0.01, 5.0]/rd sanity gate.
    # Only match patterns that explicitly say rounds/rd/count/box.
    patterns = [
        r'(\d[\d,]*)\s*rounds?',
        r'(\d[\d,]*)\s*rds?\b',
        r'(\d[\d,]*)\s*rd\s*(?:box|case|pack)',
        r'(\d[\d,]*)\s*count',
        r'(\d[\d,]*)\s*per\s*box',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return int(match.group(1).replace(',', ''))
    return None


def fetch_product_round_count(page, product_url):
    """Navigate to a product page and recover the smallest credible
    round count from three signals, in priority order:

      1. Variant radios — RGW's BigCommerce stencil renders these as
         <span class="form-option-variant">50rd Box</span> etc.
         When present, the smallest variant pairs cleanly with the
         lowest price on the category-page range.
      2. Product title <h1> — single-variant products sometimes bake
         the round count into the title (e.g. "Tula Ammo 7.62x39
         122gr FMJ - 40RD").
      3. SKU trailing suffix — many manufacturer SKUs encode count
         as a "-NN" or "-NNN" tail (Sig V-Crown E380A1-365-20 is
         20rd, etc.). Bounded to [5, 5000] to avoid grabbing random
         SKU digits.

    Returns None when no signal yields a credible count; caller
    should then skip the listing rather than guess.
    """
    try:
        resp = page.goto(product_url, wait_until='domcontentloaded', timeout=60000)
    except Exception:
        return None
    if resp and resp.status >= 400:
        return None

    # 1. Variant grid
    labels = page.query_selector_all('.form-option-variant')
    rounds_seen = []
    for label in labels:
        text = (label.inner_text() or '').strip()
        m = re.search(r'(\d[\d,]*)\s*rd', text, re.IGNORECASE)
        if m:
            rounds_seen.append(int(m.group(1).replace(',', '')))
    if rounds_seen:
        return min(rounds_seen)

    # 2. Product title
    h1 = page.query_selector('h1.productView-title, h1')
    if h1:
        n = parse_rounds((h1.inner_text() or ''))
        if n:
            return n

    # 3. SKU tail
    sku_el = page.query_selector('[data-product-sku], .productView-info dd[data-test*="sku" i]')
    sku_text = ''
    if sku_el:
        sku_text = (sku_el.inner_text() or '').strip()
    else:
        # Fall back to grepping the page body for "SKU: …".
        body = (page.locator('body').inner_text() or '')
        m = re.search(r'SKU:\s*([A-Z0-9\-]+)', body, re.IGNORECASE)
        if m:
            sku_text = m.group(1)
    if sku_text:
        m = re.search(r'-(\d{2,4})\s*$', sku_text)
        if m:
            n = int(m.group(1))
            if 5 <= n <= 5000:
                return n
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
    time.sleep(6)

    # Scope to the main product grid — RGW's BigCommerce stencil theme
    # wraps each product in <li class="product"> directly under
    # <ul class="productGrid">. The previous broad selector
    # ('article.card, li.product, .productCard') matched 24 phantom
    # cards from sidebar carousels and the "you might also like" rail.
    products = page.query_selector_all('.productGrid li.product')
    if not products:
        products = page.query_selector_all('ul.productGrid li.product')
    print(f"  Found {len(products)} products")
    if not products:
        return 0, 0

    # Collect the cheap parts (URL, title, lowest-of-range price) from
    # the category page first; visit each product page in a second
    # pass to read the variant grid. Two passes keeps the loop simple
    # and lets us bail early on cards that fail basic shape checks.
    candidates = []
    skipped = 0
    for product in products:
        try:
            # Title + product URL — RGW renders both an h3.card-title
            # anchor and a card-figure__link anchor. The h3 anchor's
            # innerText is the cleanest title; both share the same
            # href.
            link_el = (product.query_selector('h3.card-title a')
                       or product.query_selector('.card-figure__link'))
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

            name = (link_el.inner_text() or '').strip()
            if not name:
                # card-figure__link's text is empty; the visible title
                # only lives on the aria-label / on the h3 anchor.
                name = (link_el.get_attribute('aria-label') or '').split(',')[0].strip()

            # Real prices live in a single span for a single-variant
            # product, OR a hyphenated range like "$13.99 - $274.99"
            # for multi-variant products. Take the LOWEST of the
            # range — that's the smallest pack size, which we'll
            # pair with the smallest variant's round count below.
            price_el = (product.query_selector('[data-product-price-without-tax]')
                        or product.query_selector('.price.price--withoutTax'))
            if not price_el:
                skipped += 1
                continue
            price_text = (price_el.inner_text() or '').strip()
            price_matches = re.findall(r'\$(\d{1,4}(?:,\d{3})*(?:\.\d{1,2})?)', price_text)
            if not price_matches:
                skipped += 1
                continue
            base_price = min(float(p.replace(',', '')) for p in price_matches)
            if base_price <= 0:
                skipped += 1
                continue

            product_id = product_url.rstrip('/').split('/')[-1]
            if not product_id or product_id in seen_ids:
                continue
            seen_ids.add(product_id)
            candidates.append((product_id, product_url, name, base_price))
        except Exception as e:
            skipped += 1
            print(f"  Skipped (card parse): {e}")
            continue

    saved = 0
    for product_id, product_url, name, base_price in candidates:
        try:
            # Round count: RGW doesn't put it in the title, so we have
            # to visit the product page and read the variant radio
            # labels (e.g. "50rd Box", "1000rd Case"). We pair the
            # SMALLEST variant with the LOWEST price from the
            # category-page range — together that's the per-box deal
            # we want to record.
            total_rounds = fetch_product_round_count(page, product_url)
            if not total_rounds:
                # Last-ditch fallback: maybe the round count was in the
                # category-page title. Rare on RGW but cheap to try.
                total_rounds = parse_rounds(name)
            if not total_rounds or total_rounds <= 0:
                skipped += 1
                print(f"  Skipped (no round count): {name[:55]}")
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

            # We're sitting on the product page after the variant
            # fetch, so read stock + purchase-limit hints directly
            # from its body. Category-page card text isn't reliable
            # here because RGW shows price ranges, not stock badges.
            page_text = (page.locator('body').inner_text() or '').lower()
            in_stock = ('out of stock' not in page_text and
                        'sold out' not in page_text and
                        'currently unavailable' not in page_text)
            purchase_limit = parse_purchase_limit(page_text)

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
    print(f"[{datetime.now()}] Starting RecoilGunWorks scraper (all calibers)...")
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
