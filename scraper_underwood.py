import os
import re
import time
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

from scraper_lib import (
    CALIBERS, now_iso, with_stock_fields, parse_purchase_limit,
    parse_brand, sanity_check_ppr, clean_title, normalize_caliber, parse_bullet_type as _shared_bullet_type,
    mark_retailer_scraped,
)

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "underwood"
SITE_BASE = "https://underwoodammo.com"

# BigCommerce stencil. Underwood is a manufacturer-direct boutique
# with no per-caliber category URL — instead they slice their lineup
# into three usage-type pages: handgun defensive, rifle target, rifle
# hunting. Verified 2026-04-26: parent URLs without the trailing
# sub-segment 404, so only the three full paths below are valid.
# normalize_caliber() does the bucketing from each title; products
# outside our 10 calibers (10mm Auto, .44 Mag, .500 S&W, etc.) are
# silently skipped.
PARENT_PATHS = [
    '/ammo/handgun-ammo/defensive/',
    '/ammo/rifle-ammo/target/',
    '/ammo/rifle-ammo/hunting/',
]

MAX_PAGES = 8  # Each parent showed <=20 cards on page 1; Underwood's
               # entire lineup is well under 100 SKUs.


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
    # Underwood titles are short — typical form is
    # "9mm Luger +P+ 65gr Xtreme Defender, $19.99" with no explicit
    # round count (their bulk pages append "20 Round Box" / "200
    # Round Bulk Pack"). When the count is missing we default to 20
    # since every Underwood SKU we've seen ships in 20-round boxes
    # unless explicitly labelled otherwise — caller can override.
    patterns = [
        r'(\d[\d,]*)\s*[- ]?\s*rounds?\b',
        r'(\d[\d,]*)\s*[- ]?\s*rds?\b',
        r'(\d[\d,]*)\s*[- ]?\s*rd\s*(?:box|case|pack|bulk)',
        r'(\d[\d,]*)\s*per\s*box',
        r'(\d[\d,]*)\s*[- ]?\s*round\s*(?:box|bulk\s*pack|case)',
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return int(m.group(1).replace(',', ''))
    return None


def parse_case_material(text):
    text_lower = text.lower()
    # Underwood loads brass-case ammunition exclusively (manufacturer
    # spec — every catalog card is brass). Honor any explicit override
    # for completeness, otherwise default to Brass without the steel-
    # brand fallback that other scrapers use.
    if 'aluminum' in text_lower:
        return 'Aluminum'
    if 'nickel' in text_lower:
        return 'Nickel'
    return 'Brass'


def parse_bullet_type(text):
    """Underwood loads several Lehigh Defense Xtreme lines whose bullet
    type isn't named in standard parser tokens. Pre-check those, then
    fall through to the canonical scraper_lib parser.
    """
    upper = (text or '').upper()
    if 'XTREME DEFENDER' in upper or 'XTREME PENETRATOR' in upper:
        # Solid copper fluted — closest analog is hollow point.
        return 'HP'
    if 'XTREME HUNTER' in upper:
        return 'SP'
    return _shared_bullet_type(text)



def parse_country(text):
    # Underwood is US-manufactured (Sparta, IL).
    return 'USA'


def scrape_parent(page, parent_path, retailer_id, seen_ids, counts):
    base = SITE_BASE + parent_path
    saved = 0
    skipped = 0

    for page_num in range(1, MAX_PAGES + 1):
        url = base if page_num == 1 else f"{base}?page={page_num}"
        print(f"\n[{parent_path}] Loading page {page_num}: {url}")
        try:
            resp = page.goto(url, wait_until='domcontentloaded', timeout=60000)
        except Exception as e:
            print(f"  goto failed: {e}")
            break
        if resp and resp.status >= 400:
            print(f"  HTTP {resp.status} — stopping parent.")
            break
        time.sleep(2)

        cards = page.query_selector_all('.productGrid article.card')
        if not cards:
            print(f"  No cards on page {page_num}, stopping parent.")
            break

        new_on_page = 0
        for card in cards:
            try:
                # URL + title come from a.card-figure__link's href and
                # aria-label respectively. The aria-label format is
                # "<title>, $<price>" — a clean joint source for both.
                link_el = card.query_selector('a.card-figure__link, h4.card-title a')
                if not link_el:
                    skipped += 1
                    continue
                href = link_el.get_attribute('href') or ''
                if not href:
                    skipped += 1
                    continue
                product_url = href if href.startswith('http') else SITE_BASE + href

                # Same defensive /brands/ filter as the other BigCommerce
                # scrapers — never observed on Underwood but cheap to keep.
                if '/brands/' in product_url:
                    skipped += 1
                    continue

                aria = link_el.get_attribute('aria-label') or ''
                # Title is everything before the first " $" boundary in
                # the aria-label. Fall back to the h4.card-title text.
                raw_name = ''
                if aria:
                    raw_name = aria.split(', $')[0].strip()
                if not raw_name:
                    title_el = card.query_selector('h4.card-title a, h4.card-title')
                    if title_el:
                        raw_name = (title_el.inner_text() or '').strip()
                name = clean_title(raw_name)
                if not name:
                    skipped += 1
                    continue

                cal_display, cal_norm = normalize_caliber(name)
                if not cal_norm:
                    skipped += 1
                    continue

                # Price — prefer the aria-label-extracted figure (no DOM
                # parsing needed), then fall back to the standard
                # BigCommerce withoutTax span.
                base_price = None
                if aria:
                    m = re.search(r'\$(\d{1,5}(?:,\d{3})*(?:\.\d{1,2})?)', aria)
                    if m:
                        try:
                            base_price = float(m.group(1).replace(',', ''))
                        except ValueError:
                            base_price = None
                if not base_price or base_price <= 0:
                    price_el = (card.query_selector('[data-product-price-without-tax]')
                                or card.query_selector('.card-price .price--withoutTax'))
                    if price_el:
                        ptxt = (price_el.inner_text() or '').strip()
                        m = re.search(r'\$?(\d{1,5}(?:,\d{3})*(?:\.\d{1,2})?)', ptxt)
                        if m:
                            try:
                                base_price = float(m.group(1).replace(',', ''))
                            except ValueError:
                                base_price = None
                if not base_price or base_price <= 0:
                    print(f"  Skipped (no price): {name[:55]}")
                    skipped += 1
                    continue

                # Round count from title. Underwood's standard SKU is a
                # 20-round box; bulk packs are titled explicitly. When
                # neither pattern matches, default to 20.
                total_rounds = parse_rounds(name)
                if not total_rounds:
                    total_rounds = 20

                price_per_round = round(base_price / total_rounds, 4)
                if not sanity_check_ppr(price_per_round, base_price, total_rounds,
                                        context=f'{RETAILER_SLUG} {cal_norm}',
                                        caliber=cal_norm):
                    skipped += 1
                    continue

                # Stock detection — same pattern as the other BC scrapers.
                card_text = (card.inner_text() or '')
                card_lower = card_text.lower()
                in_stock = ('out of stock' not in card_lower
                            and 'sold out' not in card_lower
                            and 'unavailable' not in card_lower)

                grain = parse_grain(name)
                case_material = parse_case_material(name)
                bullet_type = parse_bullet_type(name)
                country = parse_country(name)
                manufacturer = parse_brand(name) or 'Underwood'
                purchase_limit = parse_purchase_limit(card_text)

                # data-test="card-668" exposes the BigCommerce internal
                # product id as a stable retailer_product_id.
                dt = card.get_attribute('data-test') or ''
                m = re.search(r'card-(\d+)', dt)
                product_id = m.group(1) if m else product_url.rstrip('/').split('/')[-1][:100]
                if not product_id or product_id in seen_ids:
                    continue
                seen_ids.add(product_id)

                listing = {
                    'retailer_id': retailer_id,
                    'retailer_product_id': product_id,
                    'product_url': product_url,
                    'caliber': cal_display,
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
                supabase.table('price_history').insert({
                    'listing_id': result.data[0]['id'],
                    'price': base_price,
                    'price_per_round': price_per_round,
                    'in_stock': in_stock,
                }).execute()

                saved += 1
                new_on_page += 1
                counts[cal_norm] = counts.get(cal_norm, 0) + 1
                print(f"  Saved [{cal_norm}]: {name[:55]} | ${base_price} | {price_per_round}/rd")

            except Exception as e:
                skipped += 1
                print(f"  Skipped: {e}")
                continue

        if new_on_page == 0:
            print(f"  Page {page_num} added 0 new — stopping parent.")
            break

    return saved, skipped


def scrape():
    print(f"[{datetime.now()}] Starting Underwood Ammo scraper (all calibers)...")
    retailer_id = get_retailer_id()
    if not retailer_id:
        return
    print(f"Retailer ID: {retailer_id}")

    total_saved = 0
    total_skipped = 0
    seen_ids = set()
    counts = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })

        for parent in PARENT_PATHS:
            saved, skipped = scrape_parent(page, parent, retailer_id, seen_ids, counts)
            total_saved += saved
            total_skipped += skipped

        browser.close()

    mark_retailer_scraped(supabase, retailer_id)
    print(f"\nDone! Saved: {total_saved} | Skipped: {total_skipped}")
    print("Per-caliber counts:")
    for cal in CALIBERS:
        print(f"  {cal}: {counts.get(cal, 0)}")


if __name__ == '__main__':
    scrape()
