import os
import re
import time
import urllib.request
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

from scraper_lib import (
    CALIBERS, now_iso, with_stock_fields, parse_purchase_limit,
    parse_brand, sanity_check_ppr, clean_title, normalize_caliber,
)

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "krarmory"
SITE_BASE = "https://krarmory.com"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# KR Armory runs a SiteBuilder/legacy ASP.NET stencil — every product
# lives on its own /<slug>.html (or /<slug>_p_<id>.html) page, and
# category pages just list them. Total inventory is small enough
# (~15-20 SKUs as of 2026-04-26) that walking sitemap.xml is faster
# and more complete than crawling each /<X>_c_<id>.html parent.
#
# Caliber tagging is done from each product page's structured
# extrafieldsBlock ("Caliber: 9mm Luger") with normalize_caliber()
# from scraper_lib as a fallback so KR's free-form titles still
# bucket cleanly into the 10 calibers we track.

SITEMAP_URL = f"{SITE_BASE}/sitemap.xml"

# Sitemap URLs that are NOT product pages — category indexes, info
# pages, etc. Skip these to avoid wasting Playwright navigations on
# pages that won't yield a listing.
NON_PRODUCT_SUFFIXES = (
    '/',
    '/ammo.html',
    '/handgun.html',
    '/new-arrivals.html',
    '/SHOTGUN_c_22.html',
    '/SOUND_c_20.html',
    '/SURPLUS_c_21.html',
    '/556_c_23.html',
)


def get_retailer_id():
    result = supabase.table("retailers").select("id").eq("slug", RETAILER_SLUG).execute()
    if not result.data:
        print(f"ERROR: Retailer '{RETAILER_SLUG}' not found in database")
        return None
    return result.data[0]["id"]


def fetch_sitemap_urls():
    """Pull all <loc> URLs from KR Armory's sitemap.xml.

    Returns a deduped list of absolute URLs that look like product
    pages (ending in .html, excluding category/info indexes). Falls
    back to an empty list on any error so the scraper doesn't crash
    a 2-hour cron over a one-off network blip.
    """
    try:
        req = urllib.request.Request(SITEMAP_URL, headers={'User-Agent': USER_AGENT})
        with urllib.request.urlopen(req, timeout=30) as resp:
            xml = resp.read().decode('utf-8', errors='ignore')
    except Exception as e:
        print(f"  sitemap fetch failed: {e}")
        return []

    locs = re.findall(r'<loc>([^<]+)</loc>', xml)
    seen = set()
    out = []
    for u in locs:
        u = u.strip()
        if not u.startswith(SITE_BASE):
            continue
        path = u[len(SITE_BASE):] or '/'
        if path in NON_PRODUCT_SUFFIXES:
            continue
        # Skip top-level category indexes — they match _c_<id>.html.
        if re.search(r'_c_\d+\.html$', path):
            continue
        if not path.endswith('.html'):
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def parse_extra_fields(page):
    """Read the <div class="extra_field"> blocks into a dict keyed by
    the visible label (lowercased, no trailing colon/whitespace).

    Each field looks like:
        <div class="extra_field">
            <strong>Caliber:</strong>
            <span class="info">9mm Luger</span>
        </div>
    """
    fields = {}
    blocks = page.query_selector_all('.extrafieldsBlock .extra_field')
    for block in blocks:
        label_el = block.query_selector('strong')
        value_el = block.query_selector('span.info')
        if not label_el or not value_el:
            continue
        label = (label_el.inner_text() or '').strip().rstrip(':').strip().lower()
        value = (value_el.inner_text() or '').strip()
        if label and value:
            fields[label] = value
    return fields


def extract_price(page):
    """Return the active selling price as a float.

    KR's product template marks sale items with both a struck-through
    .original-price and an active .sale-price; non-sale items render
    a single [itemprop="price"] span. Prefer sale-price when present
    (the lower of the two, the actual checkout amount), then
    itemprop, then a final regex sweep over the pricing block.
    """
    sale_el = page.query_selector('.sale-price#price, span.sale-price')
    if sale_el:
        text = (sale_el.inner_text() or '').strip()
        m = re.search(r'\$?\s*(\d{1,5}(?:,\d{3})*(?:\.\d{1,2})?)', text)
        if m:
            try:
                return float(m.group(1).replace(',', ''))
            except ValueError:
                pass

    item_el = page.query_selector('[itemprop="price"]')
    if item_el:
        # itemprop=price may live on a meta tag (content="...") or a
        # span (inner_text). Try content first.
        content = item_el.get_attribute('content')
        if content:
            try:
                return float(content)
            except ValueError:
                pass
        text = (item_el.inner_text() or '').strip()
        m = re.search(r'\$?\s*(\d{1,5}(?:,\d{3})*(?:\.\d{1,2})?)', text)
        if m:
            try:
                return float(m.group(1).replace(',', ''))
            except ValueError:
                pass

    # Last-ditch: scrape the pricing block.
    pricing_el = page.query_selector('.pricing, .saleprice, .regular-price')
    if pricing_el:
        text = (pricing_el.inner_text() or '').strip()
        amounts = re.findall(r'\$\s*(\d{1,5}(?:,\d{3})*(?:\.\d{1,2})?)', text)
        if amounts:
            try:
                # Take the smallest — sale price is always <= original.
                return min(float(a.replace(',', '')) for a in amounts)
            except ValueError:
                pass
    return None


def parse_grain_value(text):
    """Parse a grain integer from a string like '115' or '115 gr'."""
    if not text:
        return None
    m = re.search(r'(\d+)', text)
    return int(m.group(1)) if m else None


def parse_rounds_value(text):
    """Parse a round count from a string like '500' or '1000 rounds'."""
    if not text:
        return None
    m = re.search(r'(\d[\d,]*)', text)
    return int(m.group(1).replace(',', '')) if m else None


def parse_grain_from_title(text):
    m = re.search(r'(\d+)[\s-]*gr(?:ain)?\b', text, re.IGNORECASE)
    return int(m.group(1)) if m else None


def parse_rounds_from_title(text):
    patterns = [
        r'(\d[\d,]*)\s*[- ]?\s*rounds?\b',
        r'(\d[\d,]*)\s*[- ]?\s*rds?\b',
        r'(\d[\d,]*)\s*[- ]?\s*rd\s*(?:box|case|pack)',
        r'(\d[\d,]*)\s*per\s*box',
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return int(m.group(1).replace(',', ''))
    return None


def parse_case_material(text, casing_field=None):
    if casing_field:
        cl = casing_field.lower()
        for m in ('brass', 'steel', 'aluminum', 'nickel'):
            if m in cl:
                return m.capitalize()
    text_lower = text.lower()
    steel_brands = ['wolf', 'tula', 'tulammo', 'brown bear', 'silver bear', 'golden bear', 'barnaul']
    if any(b in text_lower for b in steel_brands):
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
        'igman': 'Bosnia', 'maxxtech': 'Bosnia',
    }
    for keyword, country in mapping.items():
        if keyword in text_lower:
            return country
    return None


def scrape_product(page, product_url, retailer_id, seen_ids, counts):
    """Visit a single product page and upsert it into Supabase.

    Returns (saved_int, skipped_int) — 0/0 when the page wasn't a
    product, or 1/0 on save, or 0/1 on any skip path.
    """
    try:
        resp = page.goto(product_url, wait_until='domcontentloaded', timeout=60000)
    except Exception as e:
        print(f"  goto failed: {e}")
        return 0, 1
    if resp and resp.status >= 400:
        return 0, 1

    # KR's product pages may need a beat for the .extrafieldsBlock to
    # render. Wait for it (or the main pricing block) before reading.
    try:
        page.wait_for_selector('.extrafieldsBlock, .pricing', timeout=8000)
    except Exception:
        # Not a product page — sitemap occasionally lists info pages
        # that slip past the suffix filter.
        return 0, 0

    # Title comes from h1.product-name (preferred) or any h1 on the page.
    title_el = page.query_selector('h1.product-name, h1[itemprop="name"], h1')
    raw_title = (title_el.inner_text() or '').strip() if title_el else ''
    name = clean_title(raw_title)
    if not name:
        return 0, 1

    # Skip /brands/ promotional pages on the off-chance any sneak in.
    if '/brands/' in product_url:
        return 0, 1

    fields = parse_extra_fields(page)
    caliber_text = fields.get('caliber', '')

    # Prefer the structured caliber field; fall back to title-based
    # detection so products with empty/missing extra fields still
    # bucket correctly.
    cal_display, cal_norm = (None, None)
    if caliber_text:
        cal_display, cal_norm = normalize_caliber(caliber_text)
    if not cal_norm:
        cal_display, cal_norm = normalize_caliber(name)
    if not cal_norm:
        # Outside our 10 calibers (e.g. .45 ACP, 12 GA, 7.62x54R, 8mm).
        return 0, 1

    base_price = extract_price(page)
    if not base_price or base_price <= 0:
        print(f"  Skipped (no price): {name[:55]}")
        return 0, 1

    # Round count: prefer the structured "Rounds Per" field, then
    # title parsing as fallback.
    total_rounds = parse_rounds_value(fields.get('rounds per', '')) \
        or parse_rounds_from_title(name)
    if not total_rounds or total_rounds <= 0:
        print(f"  Skipped (no round count): {name[:55]}")
        return 0, 1

    price_per_round = round(base_price / total_rounds, 4)
    if not sanity_check_ppr(price_per_round, base_price, total_rounds,
                            context=f'{RETAILER_SLUG} {cal_norm}',
                            caliber=cal_norm):
        return 0, 1

    grain = parse_grain_value(fields.get('grain', '')) or parse_grain_from_title(name)
    case_material = parse_case_material(name, fields.get('casing'))
    bullet_type = parse_bullet_type(name)
    country = parse_country(name)
    manufacturer = parse_brand(name) or "Unknown"

    # Stock from #availability span.
    avail_el = page.query_selector('#availability')
    avail_text = (avail_el.inner_text() or '').strip().lower() if avail_el else ''
    in_stock = bool(avail_text) and 'out of stock' not in avail_text \
        and 'sold out' not in avail_text and 'unavailable' not in avail_text \
        and 'discontinued' not in avail_text
    # Empty availability — fall back to body-text scan.
    if not avail_text:
        body_lower = (page.inner_text('body') or '').lower()
        in_stock = 'out of stock' not in body_lower and 'sold out' not in body_lower

    purchase_limit = parse_purchase_limit(page.inner_text('body') or '')

    # retailer_product_id: prefer the SKU exposed at #product_id, fall
    # back to the URL slug or `_p_<id>` suffix.
    sku_el = page.query_selector('#product_id, [itemprop="sku"]')
    product_id = None
    if sku_el:
        product_id = (sku_el.inner_text() or '').strip()
    if not product_id:
        m = re.search(r'_p_(\d+)\.html$', product_url)
        product_id = m.group(1) if m else product_url.rstrip('/').split('/')[-1].replace('.html', '')[:100]
    if not product_id or product_id in seen_ids:
        return 0, 0
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

    counts[cal_norm] = counts.get(cal_norm, 0) + 1
    print(f"  Saved [{cal_norm}]: {name[:55]} | ${base_price} | {price_per_round}/rd")
    return 1, 0


def scrape():
    print(f"[{datetime.now()}] Starting KR Armory scraper (all calibers)...")
    retailer_id = get_retailer_id()
    if not retailer_id:
        return

    print(f"Retailer ID: {retailer_id}")

    urls = fetch_sitemap_urls()
    print(f"Found {len(urls)} candidate product URLs in sitemap")

    total_saved = 0
    total_skipped = 0
    seen_ids = set()
    counts = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_extra_http_headers({'User-Agent': USER_AGENT})

        for url in urls:
            saved, skipped = scrape_product(page, url, retailer_id, seen_ids, counts)
            total_saved += saved
            total_skipped += skipped
            time.sleep(0.5)

        browser.close()

    print(f"\nDone! Saved: {total_saved} | Skipped: {total_skipped}")
    print("Per-caliber counts:")
    for cal in CALIBERS:
        print(f"  {cal}: {counts.get(cal, 0)}")


if __name__ == '__main__':
    scrape()
