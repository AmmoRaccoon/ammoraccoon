import os
import re
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

from scraper_lib import (
    insert_price_history,
    CALIBERS, now_iso, with_stock_fields, parse_purchase_limit,
    parse_brand, sanity_check_ppr, parse_bullet_type,
    clean_title, normalize_caliber,
    mark_retailer_scraped,
    load_caliber_paths, category_redirected, report_empty_first_pages,
)

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "gorilla"
SITE_BASE = "https://www.gorillaammo.com"

# Per-caliber category URLs now live in caliber_paths/gorilla.json
# (expansion #4 Step-2 migration) — transcribed verbatim, parity-proven
# byte-identical. WooCommerce single-brand manufacturer: only 5 of the
# 10 tracked calibers are sold (no .40 S&W / .38 Spl / .357 Mag / .22 LR
# / 7.62x39) — those are deliberately absent from the config, not parked.
# Values are now LISTS of entries and entry['url'] is a drop-in for the
# old SITE_BASE + path string.
CALIBER_PATHS = load_caliber_paths('gorilla')

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
        r'(\d[\d,]*)\s*round\s*box',
        r'(\d[\d,]*)\s*round\s*bucket',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return int(match.group(1).replace(',', ''))
    return None

def parse_case_material(text):
    text_lower = text.lower()
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
    return 'USA'  # Gorilla is a US manufacturer.

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
    # Redirect guard (NEW 2026-06-14, expansion #4 Step-2 — Gorilla had
    # none before). WooCommerce duplicate-slug renames are exactly the
    # drift this catches: a category that 301s to a DIFFERENT page with
    # HTTP 200 skips loudly and counts as an empty handle.
    if category_redirected(url, page.url):
        print(f"  REDIRECTED to {page.url} - skipping (category moved/renamed).")
        return 0, 0, True
    time.sleep(6)

    products = page.query_selector_all('li.product, ul.products li')
    if not products:
        products = page.query_selector_all('[class*="product"]')
    print(f"  Found {len(products)} products")
    if not products:
        return 0, 0, True

    saved = 0
    skipped = 0

    for product in products:
        try:
            link_el = product.query_selector('a.woocommerce-LoopProduct-link, h2.woocommerce-loop-product__title, h3 a, h2 a, a[href*="/product/"]')
            if not link_el:
                skipped += 1
                continue

            href = link_el.get_attribute('href') or ''
            title_el = product.query_selector('.woocommerce-loop-product__title, h2, h3')
            raw_name = (title_el.inner_text().strip() if title_el else '') or link_el.inner_text().strip()
            # Gorilla titles include trademark glyphs ("Sierra MatchKing®")
            # and en-dash field separators ("...MatchKing® – 20rd Box")
            # that render as � on Windows terminals. Normalize to ASCII
            # so the listings table reads cleanly across locales.
            TYPOGRAPHIC = str.maketrans({
                '–': '-', '—': '-',
                '‘': "'", '’': "'", '“': '"', '”': '"',
                '®': '', '™': '', '©': '',
                '·': '*', '•': '*', '×': 'x',
            })
            name = raw_name.translate(TYPOGRAPHIC).strip()
            if not name or not href:
                skipped += 1
                continue

            # Re-tag by TITLE, never trust the category page. A category
            # can cross-list a lookalike (an off-list cartridge or a
            # different tracked caliber); normalize_caliber re-derives the
            # real caliber and a title that maps to nothing tracked is
            # dropped (honest blank), never force-tagged by the category.
            cal_disp, cal_norm = normalize_caliber(clean_title(name))
            if not cal_norm:
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

            card_text = product.inner_text()

            price_matches = re.findall(r'\$(\d{1,4}(?:,\d{3})*(?:\.\d{1,2})?)', card_text)
            if len(price_matches) >= 2 and price_matches[0] != price_matches[1]:
                skipped += 1
                continue
            if not price_matches:
                skipped += 1
                continue
            base_price = float(price_matches[0].replace(',', ''))
            if base_price <= 0:
                skipped += 1
                continue

            total_rounds = parse_rounds(name) or parse_rounds(card_text)
            if not total_rounds or total_rounds <= 0:
                skipped += 1
                continue

            price_per_round = round(base_price / total_rounds, 4)
            if not sanity_check_ppr(price_per_round, base_price, total_rounds,
                                    context=f'{RETAILER_SLUG} {cal_norm}', caliber=cal_norm):
                skipped += 1
                continue

            grain = parse_grain(name)
            case_material = parse_case_material(name)
            bullet_type = parse_bullet_type(name)
            country = parse_country(name)
            manufacturer = parse_brand(name) or "Gorilla"  # default to house brand
            product_id = product_url.rstrip('/').split('/')[-1]
            if not product_id or product_id in seen_ids:
                continue
            seen_ids.add(product_id)

            card_lower = card_text.lower()
            in_stock = ('out of stock' not in card_lower and
                        'sold out' not in card_lower and
                        'unavailable' not in card_lower)
            purchase_limit = parse_purchase_limit(card_text)

            listing = {
                'retailer_id': retailer_id,
                'retailer_product_id': product_id,
                'product_url': product_url,
                'caliber': cal_disp,
                'caliber_normalized': cal_norm,
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
    print(f"[{datetime.now()}] Starting Gorilla Ammunition scraper (all calibers)...")
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

    # Storefront-drift guardrail + freshness honesty (NEW 2026-06-14).
    # Gorilla covers only 5 calibers, so the EMPTY_FAIL_THRESHOLD of 3
    # is a wider net here — three empty first-pages out of five is a
    # strong WooCommerce-rename signal.
    report_empty_first_pages(empty_handles, 'Gorilla Ammunition')
    mark_retailer_scraped(supabase, retailer_id, had_success=(total_saved > 0))
    print(f"\nDone! Saved: {total_saved} | Skipped: {total_skipped}")

if __name__ == '__main__':
    scrape()
