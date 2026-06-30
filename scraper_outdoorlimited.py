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

RETAILER_SLUG = "outdoorlimited"
SITE_BASE = "https://www.outdoorlimited.com"

# Per-caliber category URLs now live in caliber_paths/outdoorlimited.json
# (expansion #4 Step-2 migration) — transcribed verbatim from the prior
# inline map and parity-proven byte-identical. load_caliber_paths returns
# {caliber: [entry, ...]} (always a list); each entry's 'url' is a drop-in
# for the old SITE_BASE + path string.
CALIBER_PATHS = load_caliber_paths('outdoorlimited')

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
    # Redirect guard (NEW 2026-06-14, expansion #4 Step-2 — Outdoor Limited
    # had none before): a category that 301s to a DIFFERENT page (the TSUSA
    # renumber trap returns HTTP 200 on the wrong caliber) now skips loudly
    # and counts as an empty handle, feeding the storefront-drift guardrail
    # instead of silently scraping the wrong caliber. First real fire is
    # expected, not alarming.
    if category_redirected(url, page.url):
        print(f"  REDIRECTED to {page.url} - skipping (category moved/renamed).")
        return 0, 0, True
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
        return 0, 0, True
    time.sleep(2)

    # Each card is a .row_inner wrapper containing image link, title
    # link, total price, AND a displayed "per round" line — Outdoor
    # Limited surfaces CPR directly, which lets us back-derive an
    # exact round count instead of guessing from the title.
    products = page.query_selector_all('.row_inner')
    print(f"  Found {len(products)} products")
    if not products:
        return 0, 0, True

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

            # Re-tag by TITLE, never trust the category page. A category
            # can cross-list a lookalike (an off-list cartridge or a
            # different tracked caliber); normalize_caliber re-derives the
            # real caliber and a title that maps to nothing tracked is
            # dropped (honest blank), never force-tagged by the category.
            cal_disp, cal_norm = normalize_caliber(clean_title(name))
            if not cal_norm:
                skipped += 1
                continue

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
                                    context=f'{RETAILER_SLUG} {cal_norm}', caliber=cal_norm):
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
    """Scrape every configured category URL for a caliber (always a list — the
    TSUSA multi-URL lesson). Appends (caliber, url) to empty_handles for any
    URL whose first page rendered zero product cards."""
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
    print(f"[{datetime.now()}] Starting Outdoor Limited scraper (all calibers)...")
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

    # Storefront-drift guardrail + freshness honesty (NEW 2026-06-14): exit
    # non-zero if >= EMPTY_FAIL_THRESHOLD category URLs rendered zero products
    # on first page, and only advance last_scraped_at when something saved.
    report_empty_first_pages(empty_handles, 'Outdoor Limited')
    mark_retailer_scraped(supabase, retailer_id, had_success=(total_saved > 0))
    print(f"\nDone! Saved: {total_saved} | Skipped: {total_skipped}")

if __name__ == '__main__':
    scrape()
