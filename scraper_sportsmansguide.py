import os
import random
import re
import time
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth
from supabase import create_client

from scraper_lib import (
    CALIBERS, now_iso, with_stock_fields, parse_purchase_limit,
    parse_brand, sanity_check_ppr, clean_title, normalize_caliber, parse_bullet_type,
    mark_retailer_scraped,
)

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "sportsmansguide"
SITE_BASE = "https://www.sportsmansguide.com"

# Sportsman's Guide is fronted by a Cloudflare-style anti-bot wall
# that fires inconsistently — fresh contexts get a couple of pages
# through before being flagged. Recon 2026-04-26 confirmed the three
# parent productlist URLs work with stealth-Playwright; per-caliber
# sub-slugs exist but probing each one in sequence trips the wall,
# so we crawl the parents and use normalize_caliber() on titles to
# bucket into our 10 tracked calibers (same approach as Firearms
# Depot). 403s are logged and the parent is skipped — the next
# 2-hour cron pass picks up what this run missed.
PARENT_PATHS = [
    ('/productlist/ammo/handgun-pistol-ammo?d=121&c=95',  'handgun'),
    ('/productlist/ammo/rifle-ammo?d=121&c=96',           'rifle'),
    ('/productlist/ammo/rimfire-ammo?d=121&c=417',        'rimfire'),
]

# Defensive cap. Each productlist showed 50 tiles per page on probe
# day — handgun/rifle parents are the largest. 30 pages × 50 = 1500
# tiles per parent which exceeds anything we've observed.
MAX_PAGES = 30


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
    # SG titles read like "Federal Champion, 9mm, FMJ, 115 Grain, 200
    # Rounds" or ".22LR Mini Mag, 36-Grain CPHP, 100 Rounds" — round
    # count is always trailing or at minimum followed by "Rounds".
    patterns = [
        r'(\d[\d,]*)\s*[- ]?\s*rounds?\b',
        r'(\d[\d,]*)\s*[- ]?\s*rds?\b',
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
    if any(b in text_lower for b in steel_brands):
        return 'Steel'
    if 'steel case' in text_lower or 'steel-case' in text_lower:
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
        'igman': 'Bosnia', 'maxxtech': 'Bosnia',
    }
    for keyword, country in mapping.items():
        if keyword in text_lower:
            return country
    return None


def scrape_parent(page, parent_path, label, retailer_id, seen_ids, counts):
    saved = 0
    skipped = 0
    base = SITE_BASE + parent_path

    for page_num in range(1, MAX_PAGES + 1):
        # SG uses ?pg=N for pagination. Append with the right separator
        # whether or not the parent path already has a query string.
        sep = '&' if '?' in parent_path else '?'
        url = base if page_num == 1 else f"{base}{sep}pg={page_num}"
        print(f"\n[{label}] Loading page {page_num}: {url}")

        try:
            resp = page.goto(url, wait_until='domcontentloaded', timeout=60000)
        except Exception as e:
            print(f"  goto failed: {e}")
            break

        # 403s from SG's bot wall — log and stop the parent. The cron
        # retries every 2 hours, so a missed parent today shows up in
        # the next pass.
        if resp and resp.status == 403:
            print(f"  HTTP 403 (bot wall) — skipping rest of {label}.")
            break
        if resp and resp.status >= 400:
            print(f"  HTTP {resp.status} — stopping {label}.")
            break

        # Stealth wait: SG product tiles ship in markup, but a beat
        # for the JS-driven price decoration helps avoid empty-price
        # reads on slow pages.
        try:
            page.wait_for_selector('.product-tile', timeout=8000)
        except Exception:
            print(f"  No .product-tile rendered on page {page_num}, stopping {label}.")
            break

        tiles = page.query_selector_all('.product-tile')
        if not tiles:
            print(f"  No tiles on page {page_num}, stopping {label}.")
            break

        new_on_page = 0
        for tile in tiles:
            try:
                anchor = tile.query_selector('a.anchor-container, a[href*="/product/"]')
                if not anchor:
                    skipped += 1
                    continue
                href = anchor.get_attribute('href') or ''
                if not href:
                    skipped += 1
                    continue
                product_url = href if href.startswith('http') else SITE_BASE + href

                # Skip brand-carousel rails — same defensive filter as
                # the rest of our scrapers.
                if '/brands/' in product_url:
                    skipped += 1
                    continue

                # Title: prefer the .product-name span text, fall back
                # to the anchor's data-attributes / title attribute.
                title_el = tile.query_selector('.product-name span, .product-name')
                raw_name = (title_el.inner_text() if title_el else '').strip()
                if not raw_name:
                    img = tile.query_selector('img.product')
                    if img:
                        raw_name = (img.get_attribute('alt') or img.get_attribute('title') or '').strip()
                name = clean_title(raw_name)
                if not name:
                    skipped += 1
                    continue

                cal_display, cal_norm = normalize_caliber(name)
                if not cal_norm:
                    skipped += 1
                    continue

                # Price: the anchor exposes a clean `cost="66.99"`
                # attribute. Fall back to parsing the visible
                # .regular-price span ("$66.99 /") if cost is missing.
                base_price = None
                cost_attr = anchor.get_attribute('cost')
                if cost_attr:
                    try:
                        base_price = float(cost_attr)
                    except ValueError:
                        base_price = None
                if not base_price or base_price <= 0:
                    price_el = tile.query_selector('.regular-price')
                    if price_el:
                        price_text = (price_el.inner_text() or '').strip()
                        m = re.search(r'\$?(\d{1,5}(?:,\d{3})*(?:\.\d{1,2})?)', price_text)
                        if m:
                            try:
                                base_price = float(m.group(1).replace(',', ''))
                            except ValueError:
                                base_price = None
                if not base_price or base_price <= 0:
                    print(f"  Skipped (no price): {name[:55]}")
                    skipped += 1
                    continue

                # SG pre-calculates PPR ("As low as $0.32 / rd") — we
                # use it to back-derive round count when the title
                # doesn't expose one explicitly.
                ppr_el = tile.query_selector('.plp-ppr')
                ppr = None
                if ppr_el:
                    ppr_text = (ppr_el.inner_text() or '').strip()
                    m = re.search(r'\$?\s*(\d+\.\d+)\s*/\s*rd', ppr_text, re.IGNORECASE)
                    if m:
                        try:
                            ppr = float(m.group(1))
                        except ValueError:
                            ppr = None

                total_rounds = parse_rounds(name)
                if (not total_rounds or total_rounds <= 0) and ppr and ppr > 0:
                    total_rounds = max(1, round(base_price / ppr))
                if not total_rounds or total_rounds <= 0:
                    skipped += 1
                    continue

                # Final PPR — prefer SG's surfaced figure for accuracy
                # (avoids drift from base_price / total_rounds rounding).
                price_per_round = ppr if ppr and ppr > 0 else round(base_price / total_rounds, 4)

                if not sanity_check_ppr(price_per_round, base_price, total_rounds,
                                        context=f'{RETAILER_SLUG} {cal_norm}',
                                        caliber=cal_norm):
                    skipped += 1
                    continue

                # Stock detection. SG tiles render an "Out of Stock"
                # badge in .image-banner when relevant; otherwise a
                # quick scan of the tile's body text.
                tile_text = (tile.inner_text() or '')
                tile_lower = tile_text.lower()
                in_stock = ('out of stock' not in tile_lower
                            and 'sold out' not in tile_lower
                            and 'unavailable' not in tile_lower)

                grain = parse_grain(name)
                case_material = parse_case_material(name)
                bullet_type = parse_bullet_type(name)
                country = parse_country(name)
                manufacturer = parse_brand(name) or "Unknown"
                purchase_limit = parse_purchase_limit(tile_text)

                # SG product IDs: the anchor exposes pid="715111000000"
                # (BigRiver/Algolia internal ID) which is stable across
                # runs. Falls back to the URL's `?a=<adid>` query value.
                product_id = anchor.get_attribute('pid')
                if not product_id:
                    m = re.search(r'\?a=(\d+)', product_url)
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
            # Page rendered tiles but every one was a duplicate or
            # outside our calibers — no point paginating further.
            print(f"  Page {page_num} added 0 new — stopping {label}.")
            break

        # Conservative jittered pacing — SG's wall is rate-aware.
        time.sleep(random.uniform(4.5, 7.5))

    return saved, skipped


def scrape():
    print(f"[{datetime.now()}] Starting Sportsman's Guide scraper (all calibers)...")
    retailer_id = get_retailer_id()
    if not retailer_id:
        return

    print(f"Retailer ID: {retailer_id}")

    total_saved = 0
    total_skipped = 0
    seen_ids = set()
    counts = {}

    stealth = Stealth(navigator_languages_override=('en-US', 'en'))
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-features=IsolateOrigins,site-per-process',
            ],
        )
        # Each parent gets a fresh context — the bot wall flags
        # within a session, so resetting cookies + TLS between
        # parents materially improves our hit rate.
        for parent_path, label in PARENT_PATHS:
            ctx = browser.new_context(
                user_agent=('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                            'AppleWebKit/537.36 (KHTML, like Gecko) '
                            'Chrome/124.0.0.0 Safari/537.36'),
                viewport={'width': 1366, 'height': 768},
                locale='en-US', timezone_id='America/Chicago',
                extra_http_headers={
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Sec-Ch-Ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                    'Sec-Ch-Ua-Mobile': '?0',
                    'Sec-Ch-Ua-Platform': '"Windows"',
                    'Upgrade-Insecure-Requests': '1',
                },
            )
            stealth.apply_stealth_sync(ctx)
            page = ctx.new_page()
            try:
                saved, skipped = scrape_parent(page, parent_path, label, retailer_id, seen_ids, counts)
                total_saved += saved
                total_skipped += skipped
            finally:
                ctx.close()
            # Breathe between parents so SG's rate detector cools off.
            time.sleep(random.uniform(8.0, 14.0))

        browser.close()

    mark_retailer_scraped(supabase, retailer_id)
    print(f"\nDone! Saved: {total_saved} | Skipped: {total_skipped}")
    print("Per-caliber counts:")
    for cal in CALIBERS:
        print(f"  {cal}: {counts.get(cal, 0)}")


if __name__ == '__main__':
    scrape()
