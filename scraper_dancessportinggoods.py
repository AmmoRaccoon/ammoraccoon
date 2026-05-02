import os
import re
import time
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

from scraper_lib import (
    CALIBERS, normalize_caliber, now_iso, with_stock_fields,
    parse_purchase_limit, parse_brand, sanity_check_ppr, clean_title,
)

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "dancessportinggoods"
SITE_BASE = "https://www.dancessportinggoods.com"

# BigCommerce stencil. Dance's only buckets ammo by gun type, not by
# caliber, so each scrape walks all three top-level type pages and
# normalize_caliber() shoulders the per-caliber bucketing from the
# title. Verified 2026-05-02 — handgun + rifle each ~6 pages of 60,
# rimfire ~3 pages.
TYPE_PATHS = [
    '/ammo/handgun-centerfire/',
    '/ammo/rifle-centerfire/',
    '/ammo/rimfire/',
]

# Cap pagination defensively. Six pages was the deepest seen across
# all three categories on probe day; 12 leaves headroom without an
# unbounded crawl if the next link ever loops.
MAX_PAGES = 12


def get_retailer_id():
    result = supabase.table("retailers").select("id").eq("slug", RETAILER_SLUG).execute()
    if not result.data:
        print(f"ERROR: Retailer '{RETAILER_SLUG}' not found in database")
        return None
    return result.data[0]["id"]


def parse_grain(text):
    m = re.search(r'(\d+)[\s-]*gr(?:ain)?\b', text, re.IGNORECASE)
    return int(m.group(1)) if m else None


def parse_rounds(text):
    # Dance's titles follow a strict template: "... - NN Rounds" at the
    # tail. The hyphen-and-Rounds form is the canonical match; the
    # other patterns are fallbacks for edge-cases (battle packs etc).
    patterns = [
        r'(\d[\d,]*)\s*rounds?\b',
        r'(\d[\d,]*)\s*rds?\b',
        r'(\d[\d,]*)\s*[- ]?\s*rd\s*(?:box|case|pack)',
        r'(\d[\d,]*)\s*per\s*box',
        r'(\d[\d,]*)\s*[- ]?\s*count\b',
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return int(m.group(1).replace(',', ''))
    return None


def parse_case_material(text):
    text_lower = text.lower()
    steel_brands = ['wolf', 'tula', 'tulammo', 'brown bear', 'silver bear', 'golden bear', 'barnaul']
    if any(brand in text_lower for brand in steel_brands):
        return 'Steel'
    if 'steel' in text_lower:
        return 'Steel'
    if 'brass' in text_lower:
        return 'Brass'
    if 'aluminum' in text_lower:
        return 'Aluminum'
    if 'nickel' in text_lower:
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
        'wolf': 'Russia', 'aguila': 'Mexico', 'sterling': 'Turkey',
    }
    for keyword, country in mapping.items():
        if keyword in text_lower:
            return country
    return None


def scrape_type_page(page, type_path, retailer_id, seen_ids):
    base = SITE_BASE + type_path
    label = type_path.strip('/').split('/')[-1]
    saved = 0
    skipped = 0

    for page_num in range(1, MAX_PAGES + 1):
        url = base if page_num == 1 else f"{base}?page={page_num}"
        print(f"\n[{label}] Loading page {page_num}: {url}")
        try:
            resp = page.goto(url, wait_until='networkidle', timeout=60000)
        except Exception as e:
            print(f"  goto failed: {e}")
            break
        if resp and resp.status >= 400:
            print(f"  HTTP {resp.status} - stopping category.")
            break
        # Stencil renders the full grid server-side; networkidle is
        # enough. Brief settle for any final image-lazy-load.
        time.sleep(2)

        cards = page.query_selector_all('article.card')
        if not cards:
            print(f"  No cards on page {page_num}, stopping category.")
            break

        new_on_page = 0
        for card in cards:
            try:
                # Title + URL — the h4.card-title's inner <a> is the
                # canonical entry. Stencil also exposes a duplicate
                # link inside .card-figure (image), but the h4 link
                # is always present and carries the full title text.
                link_el = card.query_selector('h4.card-title a')
                if not link_el:
                    skipped += 1
                    continue
                href = link_el.get_attribute('href') or ''
                if not href:
                    skipped += 1
                    continue
                product_url = href if href.startswith('http') else SITE_BASE + href

                # Skip brand-carousel cards if they ever appear in the
                # grid — same defensive filter used by other BC stencils.
                if '/brands/' in product_url:
                    skipped += 1
                    continue

                raw_name = (link_el.inner_text() or '').strip()
                name = clean_title(raw_name)
                if not name:
                    skipped += 1
                    continue

                # Per-caliber bucketing from the title. Dance's mixes
                # all calibers per type page (handgun-centerfire alone
                # carries 32-20 Win, 460 S&W Mag, etc), so anything
                # outside our 10-caliber tracking list is dropped here.
                caliber_display, caliber_norm = normalize_caliber(name)
                if not caliber_norm:
                    skipped += 1
                    continue

                # Stock detection — Dance's renders a .stock-badge with
                # a "Out of stock" message on OOS cards. In-stock cards
                # may show "Only N left in stock" instead, also inside
                # .stock-badge but with different copy. Treat any badge
                # text matching OOS as out, everything else as in stock.
                stock_el = card.query_selector('.stock-badge .stock-message, .stock-badge')
                stock_text = (stock_el.inner_text() if stock_el else '').strip().lower()
                in_stock = 'out of stock' not in stock_text

                # Price — stencil exposes the live price in
                # .price--withoutTax. Sale items also carry a struck
                # .price--non-sale span which we ignore.
                price_el = card.query_selector('.price--withoutTax')
                price_text = (price_el.inner_text() if price_el else '').strip()
                price_match = re.search(r'\$\s*(\d{1,4}(?:,\d{3})*(?:\.\d{1,2})?)', price_text)
                if not price_match:
                    skipped += 1
                    continue
                base_price = float(price_match.group(1).replace(',', ''))
                if base_price <= 0:
                    skipped += 1
                    continue

                total_rounds = parse_rounds(name)
                if not total_rounds or total_rounds <= 0:
                    skipped += 1
                    print(f"  Skipped (no round count): {name[:55]}")
                    continue

                price_per_round = round(base_price / total_rounds, 4)

                if not sanity_check_ppr(price_per_round, base_price, total_rounds,
                                        context=f'{RETAILER_SLUG} {caliber_norm}',
                                        caliber=caliber_norm):
                    skipped += 1
                    continue

                card_text = card.inner_text() or ''
                purchase_limit = parse_purchase_limit(card_text)

                grain = parse_grain(name)
                case_material = parse_case_material(name)
                bullet_type = parse_bullet_type(name)
                country = parse_country(name)
                manufacturer = parse_brand(name) or "Unknown"
                # Slug is the last path segment of the product URL.
                product_id = product_url.rstrip('/').split('/')[-1][:100]
                if not product_id or product_id in seen_ids:
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

                supabase.table('price_history').insert({
                    'listing_id': result.data[0]['id'],
                    'price': base_price,
                    'price_per_round': price_per_round,
                    'in_stock': in_stock,
                }).execute()

                saved += 1
                new_on_page += 1
                print(f"  Saved [{caliber_norm}]: {name[:55]} | ${base_price} | {price_per_round}/rd")

            except Exception as e:
                skipped += 1
                print(f"  Skipped: {e}")
                continue

        if new_on_page == 0:
            # Either the page mirrored an earlier page (dedup against
            # seen_ids) or every card failed selector matching — either
            # way, no point continuing further.
            break

    return saved, skipped


def scrape():
    print(f"[{datetime.now()}] Starting Dance's Sporting Goods scraper (all calibers)...")
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

        for type_path in TYPE_PATHS:
            saved, skipped = scrape_type_page(page, type_path, retailer_id, seen_ids)
            total_saved += saved
            total_skipped += skipped

        browser.close()

    print(f"\nDone! Saved: {total_saved} | Skipped: {total_skipped}")


if __name__ == '__main__':
    scrape()
