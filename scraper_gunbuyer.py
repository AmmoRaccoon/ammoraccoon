import os
import re
import time
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

from scraper_lib import (
    CALIBERS, now_iso, with_stock_fields, parse_purchase_limit,
    parse_brand_with_url, sanity_check_ppr, clean_title,
    parse_bullet_type_with_url_fallback,
    mark_retailer_scraped,
)

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "gunbuyer"
SITE_BASE = "https://www.gunbuyer.com"

# Magento storefront. Handgun calibers have clean per-caliber subcat
# pages; the rifle parent doesn't subdivide so we drive the layered-nav
# `?caliber=N` facet IDs (verified 2026-05-02 against the live filter
# sidebar). Combo facets (e.g. "223 Remington/5.56x45mm NATO") and the
# pure NATO facet are listed separately because Gunbuyer indexes some
# SKUs only on one, and the seen_ids dedup cleans up any overlap.
CALIBER_URLS = {
    '9mm':     ['/ammunition/handgun/9mm.html'],
    '380acp':  ['/ammunition/handgun/380-acp.html'],
    '40sw':    ['/ammunition/handgun/40-s-w.html'],
    '38spl':   ['/ammunition/handgun/38-special.html'],
    '357mag':  ['/ammunition/handgun.html?caliber=760'],
    '22lr':    ['/ammunition/rimfire/22-lr.html'],
    '223-556': ['/ammunition/rifle.html?caliber=675',   # 223 Remington
                '/ammunition/rifle.html?caliber=680',   # 223/5.56 combo
                '/ammunition/rifle.html?caliber=827'],  # 5.56x45mm NATO
    '308win':  ['/ammunition/rifle.html?caliber=737',   # 308 Winchester
                '/ammunition/rifle.html?caliber=738'],  # 308/7.62x51 NATO
    '762x39':  ['/ammunition/rifle.html?caliber=854'],
    '300blk':  ['/ammunition/rifle.html?caliber=720'],
}

# Cap pagination defensively. Most caliber facets surface 50-200 items
# at 15/page = 4-14 pages. 25 leaves headroom for the 192-item 9mm
# bucket without an unbounded crawl if the next link ever loops.
MAX_PAGES = 25

USER_AGENT = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
              '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')


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
    # Gunbuyer titles are uppercase and use round-count forms like
    # "50 ROUND BOX", "20RD", "1000RDS", "100 CT". Many SKUs also
    # carry warehouse notation like "20/10" or "50/20" — that's
    # rounds-per-box / boxes-per-case where the SKU still sells ONE
    # box (verified 2026-05-02 by spot-checking $/rd against the
    # displayed total price). So the leading number of the slash form
    # is the per-box count and we treat the second number as case
    # cardinality only, never multiplying.
    patterns = [
        r'(\d[\d,]*)\s*round?s?\b',
        r'(\d[\d,]*)\s*rds?\b',
        r'(\d[\d,]*)\s*[- ]?\s*rd\s*(?:box|case|pack)',
        r'(\d[\d,]*)\s*per\s*box',
        r'(\d[\d,]*)\s*[- ]?\s*ct\b',
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return int(m.group(1).replace(',', ''))
    # "20/10" warehouse notation — first number is rounds in the box
    # the customer is buying. Bound the per-box count to the plausible
    # 5-1000 range so a year ("2024/2025") or model ID can't sneak in.
    m = re.search(r'\b(\d{1,4})\s*/\s*\d{1,4}\b', text)
    if m:
        per_box = int(m.group(1))
        if 5 <= per_box <= 1000:
            return per_box
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


def parse_country(text):
    text_lower = text.lower()
    mapping = {
        'federal': 'USA', 'winchester': 'USA', 'remington': 'USA',
        'cci': 'USA', 'speer': 'USA', 'hornady': 'USA',
        'blazer': 'USA', 'fiocchi': 'USA', 'american eagle': 'USA',
        'magtech': 'Brazil', 'cbc': 'Brazil',
        'ppu': 'Serbia', 'prvi partizan': 'Serbia',
        'sellier': 'Czech Republic', 's&b': 'Czech Republic',
        'tula': 'Russia', 'wolf': 'Russia',
        'aguila': 'Mexico', 'sterling': 'Turkey',
    }
    for keyword, country in mapping.items():
        if keyword in text_lower:
            return country
    return None


def _open_page(browser, url):
    """Open `url` in a fresh browser context and return (context, page, response).

    A fresh context per request is required because Cloudflare 403s
    Gunbuyer's layered-nav URLs after 1-2 hits within a single session
    (verified 2026-05-02). Caller is responsible for context.close().
    """
    context = browser.new_context(user_agent=USER_AGENT)
    page = context.new_page()
    resp = page.goto(url, wait_until='domcontentloaded', timeout=60000)
    return context, page, resp


def scrape_facet(browser, caliber_norm, caliber_display, facet_path, retailer_id, seen_ids):
    """Scrape one facet URL for one caliber.

    Each pagination page runs in its own browser context to sidestep
    Cloudflare's session-based rate limiting on the layered-nav
    (`?caliber=`) URLs. Recon 2026-05-02 confirmed pages 1-4 all return
    200 OK when each is loaded in a fresh context, while reusing a
    context for page 2 reliably returns 403.
    """
    base = SITE_BASE + facet_path
    saved = 0
    skipped = 0
    sep = '&' if '?' in facet_path else '?'

    for page_num in range(1, MAX_PAGES + 1):
        url = base if page_num == 1 else f"{base}{sep}p={page_num}"
        print(f"\n[{caliber_norm}] Loading page {page_num}: {url}")
        try:
            context, page, resp = _open_page(browser, url)
        except Exception as e:
            print(f"  goto failed: {e}")
            break
        try:
            if resp and resp.status >= 400:
                print(f"  HTTP {resp.status} - stopping facet.")
                break
            # Title text on .product-item-link populates a few seconds
            # after DOMContentLoaded — wait explicitly so we don't grab
            # empty strings.
            try:
                page.wait_for_selector(
                    '.products.list.items.product-items > li .product-item-link',
                    timeout=15000,
                )
            except Exception:
                # Empty page or selector drift — bail this facet.
                print(f"  no product-item-link rendered after 15s, stopping facet.")
                break

            cards = page.query_selector_all('.products.list.items.product-items > li')
            if not cards:
                print(f"  No cards on page {page_num}, stopping facet.")
                break

            new_on_page = 0
            for card in cards:
                try:
                    link_el = card.query_selector('.product-item-link')
                    if not link_el:
                        skipped += 1
                        continue
                    href = link_el.get_attribute('href') or ''
                    if not href:
                        skipped += 1
                        continue
                    product_url = href if href.startswith('http') else SITE_BASE + href

                    raw_name = (link_el.inner_text() or '').strip()
                    name = clean_title(raw_name)
                    if not name:
                        skipped += 1
                        continue

                    # Stock — Magento exposes OOS via either a
                    # .stock.unavailable span or by suppressing the
                    # Add-to-Cart button. Treat the explicit unavailable
                    # marker as authoritative; absence of "Add to Cart"
                    # text in the card is the fallback signal.
                    oos_el = card.query_selector('.stock.unavailable, .out-of-stock')
                    if oos_el is not None:
                        in_stock = False
                    else:
                        card_text = (card.inner_text() or '').lower()
                        in_stock = 'add to cart' in card_text or 'in stock' in card_text

                    # Price — Magento's [data-price-amount] attribute
                    # is the cleanest source: numeric, no glyphs, no
                    # split-decimal parsing. Final-price-wrapper is the
                    # active price even when a sale is in effect.
                    price_el = card.query_selector(
                        '[data-price-type="finalPrice"] [data-price-amount], '
                        '.price-final_price [data-price-amount], '
                        '[data-price-amount]'
                    )
                    base_price = None
                    if price_el:
                        amt = price_el.get_attribute('data-price-amount')
                        if amt:
                            try:
                                base_price = float(amt)
                            except ValueError:
                                base_price = None
                    if base_price is None or base_price <= 0:
                        # Fallback to inner_text() $-regex.
                        ptext = (price_el.inner_text() if price_el else
                                 (card.inner_text() or ''))
                        m = re.search(r'\$\s*(\d{1,4}(?:,\d{3})*\.\d{1,2})', ptext)
                        if m:
                            base_price = float(m.group(1).replace(',', ''))
                    if base_price is None or base_price <= 0:
                        skipped += 1
                        print(f"  Skipped (no price): {name[:55]}")
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

                    purchase_limit = parse_purchase_limit(card.inner_text() or '')
                    grain = parse_grain(name)
                    case_material = parse_case_material(name)
                    # Gunbuyer titles abbreviate to SKU codes
                    # ("WIN X193150 5.56 55 BOX 150RD") that frequently
                    # omit the bullet-type token even when the URL slug
                    # exposes it ("...-fmj-..."). Use the slug fallback
                    # so the audit's 53 in-stock NULLs get caught.
                    bullet_type = parse_bullet_type_with_url_fallback(name, product_url)
                    country = parse_country(name)
                    manufacturer = parse_brand_with_url(name, product_url) or "Unknown"
                    # Slug = filename minus the .html suffix; stable
                    # across runs and unique per SKU on Magento.
                    product_id = product_url.rstrip('/').split('/')[-1].replace('.html', '')[:100]
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
                # Either the page mirrored an earlier page (dedup
                # against seen_ids across facets) or every card failed
                # selector matching — either way, no point continuing.
                break
        finally:
            context.close()

        # Brief pause between contexts. Not a stealth mechanism — just
        # politeness so Gunbuyer's WAF isn't drowning in fresh sessions.
        time.sleep(2)

    return saved, skipped


def scrape():
    print(f"[{datetime.now()}] Starting Gunbuyer scraper (all calibers)...")
    retailer_id = get_retailer_id()
    if not retailer_id:
        return

    print(f"Retailer ID: {retailer_id}")

    total_saved = 0
    total_skipped = 0
    seen_ids = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        for caliber_norm, paths in CALIBER_URLS.items():
            caliber_display = CALIBERS[caliber_norm]
            for facet_path in paths:
                saved, skipped = scrape_facet(
                    browser, caliber_norm, caliber_display,
                    facet_path, retailer_id, seen_ids,
                )
                total_saved += saved
                total_skipped += skipped
                # Brief gap between contexts to give Cloudflare time
                # to forget us. Not a stealth mechanism — just basic
                # politeness.
                time.sleep(3)
        browser.close()

    mark_retailer_scraped(supabase, retailer_id)
    print(f"\nDone! Saved: {total_saved} | Skipped: {total_skipped}")


if __name__ == '__main__':
    scrape()
