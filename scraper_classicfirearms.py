import os
import re
import time
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

from scraper_lib import (
    CALIBERS, now_iso, with_stock_fields, parse_purchase_limit,
    parse_brand, sanity_check_ppr, clean_title, parse_bullet_type,
    mark_retailer_scraped,
)

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "classic-firearms"
SITE_BASE = "https://www.classicfirearms.com"

# Magento store, pure SSR. URLs verified 2026-04-25 by reading <title>
# of each page. 7.62x39 lives at the hyphenated slug; the unhyphenated
# 762x39 variant 404s on this site.
CALIBER_PATHS = {
    '9mm':     '/ammo/handgun-ammo/9mm/',
    '380acp':  '/ammo/handgun-ammo/380-acp/',
    '40sw':    '/ammo/handgun-ammo/40sw/',
    '38spl':   '/ammo/handgun-ammo/38-special/',
    '357mag':  '/ammo/handgun-ammo/357/',
    '22lr':    '/ammo/rimfire-ammo/22lr/',
    '223-556': '/ammo/rifle-ammo/223rem/',
    '308win':  '/ammo/rifle-ammo/308/',
    '762x39':  '/ammo/rifle-ammo/7-62x39/',
    '300blk':  '/ammo/rifle-ammo/300-blackout/',
}

# Cap pagination defensively — 9mm had ~285 facet-counted listings on
# probe day, which at 24/page is ~12 pages. 20 leaves headroom for
# growth without an unbounded crawl if the next link ever loops.
MAX_PAGES = 20


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
    # CF titles use both "100rd Box" / "20 Round Box" / "50 Rounds Per
    # Box" forms. Allow hyphens between number and unit so titles like
    # "20-Round Box" still parse.
    patterns = [
        r'(\d[\d,]*)\s*[- ]?\s*rounds?\b',
        r'(\d[\d,]*)\s*[- ]?\s*rds?\b',
        r'(\d[\d,]*)\s*[- ]?\s*rd\s*(?:box|case|pack)',
        r'(\d[\d,]*)\s*per\s*box',
        r'(\d[\d,]*)\s*[- ]?\s*count\b',
        r'(\d[\d,]*)\s*[- ]?\s*ct\b',
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
    if 'steel case' in text_lower or 'steel-case' in text_lower:
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
        'wolf': 'Russia', 'aguila': 'Mexico', 'sterling': 'Turkey',
    }
    for keyword, country in mapping.items():
        if keyword in text_lower:
            return country
    return None


def extract_price_from_block(price_text):
    """Pull a dollar amount out of a CF price block.

    CF renders the integer dollars and the cents separately:
      <div class="price">$11 <span class="decimal">99</span></div>
    inner_text() of that block flattens to "$11 99" — note the space.
    Standard $11.99 fallback is also accepted in case CF tweaks the
    template. Returns None when no parseable amount is found.
    """
    if not price_text:
        return None
    # "$11 99" form (dollars + decimal span concatenated by inner_text).
    m = re.search(r'\$\s*(\d{1,4}(?:,\d{3})*)\s+(\d{2})\b', price_text)
    if m:
        dollars = int(m.group(1).replace(',', ''))
        cents = int(m.group(2))
        return dollars + cents / 100.0
    # Fallback: standard "$11.99" form.
    m = re.search(r'\$\s*(\d{1,4}(?:,\d{3})*\.\d{1,2})', price_text.replace(',', ''))
    if m:
        return float(m.group(1))
    return None


def scrape_caliber(page, caliber_norm, caliber_display, retailer_id, seen_ids):
    base = SITE_BASE + CALIBER_PATHS[caliber_norm]
    saved = 0
    skipped = 0

    for page_num in range(1, MAX_PAGES + 1):
        url = base if page_num == 1 else f"{base}?p={page_num}"
        print(f"\n[{caliber_norm}] Loading page {page_num}: {url}")
        try:
            resp = page.goto(url, wait_until='domcontentloaded', timeout=60000)
        except Exception as e:
            print(f"  goto failed: {e}")
            break
        if resp and resp.status >= 400:
            print(f"  HTTP {resp.status} - stopping caliber.")
            break
        time.sleep(2)

        # CF wraps every actual product card in <div class="product-card item">.
        # The bare .product-card without .item matches outer grid wrappers.
        cards = page.query_selector_all('div.product-card.item')
        if not cards:
            print(f"  No cards on page {page_num}, stopping caliber.")
            break

        new_on_page = 0
        for card in cards:
            try:
                # Title + URL — both live on the .product-name h2's child <a>.
                # The href is always absolute on CF.
                link_el = card.query_selector('h2.product-name a, .product-name a')
                if not link_el:
                    skipped += 1
                    continue
                href = link_el.get_attribute('href') or ''
                if not href:
                    skipped += 1
                    continue
                product_url = href if href.startswith('http') else SITE_BASE + href

                # Skip brand-carousel cards if they ever appear in the
                # grid — same defensive filter as the BigCommerce-stencil
                # scrapers use.
                if '/brands/' in product_url:
                    skipped += 1
                    continue

                # CF caliber facets include component bullets (-bul- SKU)
                # and unprimed brass — drop them before parsing.
                slug = product_url.rstrip('/').split('/')[-1].lower()
                if '-bul-' in slug or 'unprimed' in slug or slug.startswith('starline-'):
                    skipped += 1
                    continue

                # Prefer the title attribute (always full); fall back to
                # inner text. Run both through clean_title for typographic
                # cleanup before any further parsing.
                raw_name = (link_el.get_attribute('title')
                            or link_el.inner_text() or '').strip()
                name = clean_title(raw_name)
                if not name:
                    skipped += 1
                    continue

                # Stock detection has to come before the price read because
                # CF hides the visible price block on OOS cards and stashes
                # the last known price inside a hidden notify-me modal as
                # <input class="product-data" data-price="174.9900">.
                # We still record OOS rows so the price_history series
                # doesn't gap every time a SKU goes on backorder.
                oos_el = card.query_selector('.out-of-stock-btn, .out-of-stock-signUp')
                in_stock = oos_el is None

                # Price block. CF marks sale items by wrapping the active
                # price in <span class="special-price">; the .old-price
                # span next to it holds the compare-at amount we don't want.
                # Read special-price first when present, then fall back to
                # the bare .price div.
                price_text = ''
                special_el = card.query_selector('.special-price .price, .special-price')
                if special_el:
                    price_text = (special_el.inner_text() or '').strip()
                if not price_text:
                    price_el = card.query_selector('.product-price .price:not(.price-per-round)')
                    if price_el:
                        price_text = (price_el.inner_text() or '').strip()
                base_price = extract_price_from_block(price_text)
                # OOS-card fallback — read the hidden data-price.
                if base_price is None or base_price <= 0:
                    data_el = card.query_selector('.product-data[data-price]')
                    if data_el:
                        try:
                            base_price = float(data_el.get_attribute('data-price'))
                        except (TypeError, ValueError):
                            base_price = None
                if base_price is None or base_price <= 0:
                    skipped += 1
                    print(f"  Skipped (no price): {name[:55]}")
                    continue

                # PPR — CF surfaces it directly as "(0.240 per round)" on
                # in-stock cards. Per Jon's instruction this is the
                # authoritative source when present; OOS cards don't
                # render this widget, so we back-derive from price/rounds
                # below. Round-count fallback also kicks in when the
                # title doesn't expose a count.
                ppr_el = card.query_selector('.price-per-round')
                ppr = None
                if ppr_el:
                    ppr_text = (ppr_el.inner_text() or '').strip()
                    m = re.search(r'\(?\s*\$?\s*(\d+\.\d+)\s*per\s*round\s*\)?', ppr_text, re.IGNORECASE)
                    if m:
                        ppr = float(m.group(1))

                total_rounds = parse_rounds(name)
                if (not total_rounds or total_rounds <= 0) and ppr and ppr > 0:
                    # Back-derive from CF's own per-round figure. Round
                    # to the nearest integer; CF doesn't sell partial
                    # boxes so the result is well-defined.
                    total_rounds = max(1, round(base_price / ppr))
                if not total_rounds or total_rounds <= 0:
                    skipped += 1
                    print(f"  Skipped (no round count): {name[:55]}")
                    continue

                # Final PPR — prefer CF's surfaced value when available
                # (avoids round-trip drift from base_price / total_rounds).
                price_per_round = ppr if ppr and ppr > 0 else round(base_price / total_rounds, 4)

                if not sanity_check_ppr(price_per_round, base_price, total_rounds,
                                        context=f'{RETAILER_SLUG} {caliber_norm}',
                                        caliber=caliber_norm):
                    skipped += 1
                    continue

                # in_stock was determined above (before the price read).
                # Read the card text once for purchase-limit parsing.
                card_text = card.inner_text() or ''
                purchase_limit = parse_purchase_limit(card_text)

                grain = parse_grain(name)
                case_material = parse_case_material(name)
                bullet_type = parse_bullet_type(name)
                country = parse_country(name)
                manufacturer = parse_brand(name) or "Unknown"
                # CF product URLs end in a slug like
                # /sterling-9mm-115gr-fmj-steel-case-ammunition/ — use
                # the slug as the retailer_product_id (stable across runs).
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
    print(f"[{datetime.now()}] Starting Classic Firearms scraper (all calibers)...")
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
