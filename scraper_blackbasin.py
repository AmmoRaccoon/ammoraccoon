import os
import re
import sys
import urllib.request
import urllib.error
import json
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client

from scraper_lib import (
    CALIBERS, now_iso, with_stock_fields, parse_purchase_limit,
    parse_brand, sanity_check_ppr, clean_title, parse_bullet_type,
)

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "blackbasin"
SITE_BASE = "https://blackbasin.com"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Black Basin migrated to Shopify on/around 2026-05-08; the previous
# BigCommerce URLs (/handgun-ammo/9mm/, /rifle-ammo/.223-remington/, …)
# now all return 404 and the scraper had been silently writing zero
# rows for nine days. Shopify exposes a per-collection JSON feed at
# /collections/<handle>/products.json with full variant detail (round
# count in option1, price/sku/available per variant) — far cleaner
# than DOM-scraping a theme.
#
# .223 and 5.56 live in two separate Shopify collections on Black
# Basin even though we bucket them together as `223-556` in CALIBERS.
# We hit both URLs and dedup by retailer_product_id (variant SKU) so a
# Federal XM193 listed in both collections doesn't double-count.
CALIBER_PATHS = {
    '9mm':     ['/collections/9mm-ammo'],
    '380acp':  ['/collections/380-auto-ammo'],
    '40sw':    ['/collections/40-sw-ammo'],
    '38spl':   ['/collections/38-special-ammo'],
    '357mag':  ['/collections/357-mag-ammo'],
    '22lr':    ['/collections/22-lr-ammo'],
    '223-556': ['/collections/223-remington-ammo', '/collections/556x45-nato-ammo'],
    '308win':  ['/collections/308-ammo'],
    '762x39':  ['/collections/762x39mm-ammo'],
    '300blk':  ['/collections/300-aac-blackout-ammo'],
}


def get_retailer_id():
    result = supabase.table("retailers").select("id").eq("slug", RETAILER_SLUG).execute()
    if not result.data:
        print(f"ERROR: Retailer '{RETAILER_SLUG}' not found in database")
        return None
    return result.data[0]["id"]


def parse_grain(text):
    m = re.search(r'(\d+)[\s-]*gr(?:ain)?\b', text, re.IGNORECASE)
    return int(m.group(1)) if m else None


def parse_rounds_from_variant(variant):
    """Return the round count for a Shopify variant.

    On Black Basin, option1 is the round count as a bare numeric
    string ('50', '100', '1000'). Falls back to the variant title
    (typically identical to option1 on single-option products) and
    finally to the SKU suffix after the last dash (Black Basin SKUs
    look like `754908500086-50` where `-50` is the pack qty).
    """
    opt1 = variant.get('option1')
    if opt1 is not None:
        m = re.fullmatch(r'\s*(\d{1,5})\s*', str(opt1))
        if m:
            n = int(m.group(1))
            if 10 <= n <= 10000:
                return n

    t = variant.get('title')
    if t:
        m = re.search(r'\b(\d{1,5})\b', str(t))
        if m:
            n = int(m.group(1))
            if 10 <= n <= 10000:
                return n

    sku = variant.get('sku') or ''
    if '-' in sku:
        suffix = sku.rsplit('-', 1)[-1]
        m = re.fullmatch(r'(\d{1,5})', suffix)
        if m:
            n = int(m.group(1))
            if 10 <= n <= 10000:
                return n
    return None


def parse_case_material(text):
    text_lower = text.lower()
    steel_brands = ['wolf', 'tula', 'tulammo', 'brown bear',
                    'silver bear', 'golden bear', 'barnaul']
    if any(b in text_lower for b in steel_brands):
        return 'Steel'
    if 'steel' in text_lower:
        return 'Steel'
    if 'aluminum' in text_lower:
        return 'Aluminum'
    if 'nickel' in text_lower:
        return 'Nickel'
    if 'brass' in text_lower:
        return 'Brass'
    return 'Brass'


def parse_country(text):
    text_lower = text.lower()
    mapping = {
        'federal': 'USA', 'winchester': 'USA', 'remington': 'USA',
        'cci': 'USA', 'speer': 'USA', 'hornady': 'USA',
        'blazer': 'USA', 'fiocchi': 'Italy', 'american eagle': 'USA',
        'magtech': 'Brazil', 'cbc': 'Brazil',
        'ppu': 'Serbia', 'prvi partizan': 'Serbia',
        'sellier': 'Czech Republic', 'tula': 'Russia',
        'wolf': 'Russia', 'aguila': 'Mexico',
    }
    for k, v in mapping.items():
        if k in text_lower:
            return v
    return None


def fetch_collection_page(path, page=1, limit=250):
    """GET a single page of /<path>/products.json. Returns (products,
    http_status). Caller distinguishes 200/404/other by inspecting status."""
    url = f"{SITE_BASE}{path}/products.json?limit={limit}&page={page}"
    req = urllib.request.Request(url, headers={
        'User-Agent': USER_AGENT,
        'Accept': 'application/json',
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
        return data.get('products', []) or [], resp.status
    except urllib.error.HTTPError as e:
        return [], e.code
    except Exception as e:
        print(f"  fetch failed for {url}: {e}")
        return [], None


def fetch_collection(path):
    """Paginate through a collection's products.json feed. Returns
    (all_products, ok). ok is False when the first page didn't 200 —
    used by the loud-failure check at the end of scrape()."""
    all_products = []
    page = 1
    while True:
        chunk, status = fetch_collection_page(path, page=page)
        if page == 1 and status != 200:
            return [], False
        if not chunk:
            break
        all_products.extend(chunk)
        if len(chunk) < 250:
            break
        page += 1
        if page > 20:  # safety stop — no caliber currently has >5k products
            break
    return all_products, True


def scrape_caliber(caliber_norm, caliber_display, retailer_id, seen_ids, counts):
    """Scrape every URL mapped to a caliber. Returns
    (saved, skipped, fetched_any) — fetched_any flips True the moment
    a single URL for this caliber loads, so callers can tell "no rows"
    from "site is gone."
    """
    paths = CALIBER_PATHS[caliber_norm]
    saved_total = 0
    skipped_total = 0
    fetched_any = False

    for path in paths:
        print(f"\n[{caliber_norm}] GET {path}/products.json")
        products, ok = fetch_collection(path)
        if not ok:
            print(f"  FAILED to load {path}")
            continue
        fetched_any = True
        print(f"  {len(products)} products in feed")

        for prod in products:
            try:
                title = clean_title(prod.get('title') or '')
                handle = (prod.get('handle') or '').strip()
                if not title or not handle:
                    skipped_total += 1
                    continue

                product_url = f"{SITE_BASE}/products/{handle}"

                variants = prod.get('variants') or []
                if not variants:
                    skipped_total += 1
                    continue

                # Black Basin sells the same SKU at multiple pack sizes
                # via Shopify variants. Pick the smallest priced variant
                # — the cheapest absolute price most users see first —
                # and use its `available` flag for the listing's stock.
                priced = []
                for v in variants:
                    n = parse_rounds_from_variant(v)
                    try:
                        p = float(v.get('price') or 0)
                    except (TypeError, ValueError):
                        p = 0
                    if n and p > 0:
                        priced.append((n, p, v))

                if not priced:
                    skipped_total += 1
                    continue

                priced.sort(key=lambda t: t[0])
                total_rounds, base_price, v_chosen = priced[0]

                price_per_round = round(base_price / total_rounds, 4)
                if not sanity_check_ppr(price_per_round, base_price, total_rounds,
                                        context=f'{RETAILER_SLUG} {caliber_norm}',
                                        caliber=caliber_norm):
                    skipped_total += 1
                    continue

                in_stock = bool(v_chosen.get('available'))
                grain = parse_grain(title)
                case_material = parse_case_material(title)
                bullet_type = parse_bullet_type(title)
                country = parse_country(title)
                # Vendor on every Black Basin product is the storefront
                # name "Black Basin" — useless for manufacturer. Always
                # parse from the title.
                manufacturer = parse_brand(title) or 'Unknown'
                purchase_limit = parse_purchase_limit(title)

                product_id = (v_chosen.get('sku') or '').strip() or handle
                product_id = product_id[:100]
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

                saved_total += 1
                counts[caliber_norm] = counts.get(caliber_norm, 0) + 1
                print(f"  Saved [{caliber_norm}]: {title[:55]} | ${base_price} | {price_per_round}/rd")

            except Exception as e:
                skipped_total += 1
                print(f"  Skipped: {e}")
                continue

    return saved_total, skipped_total, fetched_any


def scrape():
    print(f"[{datetime.now()}] Starting Black Basin scraper (Shopify JSON)...")
    retailer_id = get_retailer_id()
    if not retailer_id:
        return 1
    print(f"Retailer ID: {retailer_id}")

    total_saved = 0
    total_skipped = 0
    seen_ids = set()
    counts = {}
    successful_calibers = 0

    for caliber_norm in CALIBER_PATHS:
        caliber_display = CALIBERS[caliber_norm]
        saved, skipped, fetched_any = scrape_caliber(
            caliber_norm, caliber_display, retailer_id, seen_ids, counts
        )
        total_saved += saved
        total_skipped += skipped
        if fetched_any:
            successful_calibers += 1

    print(f"\nDone! Saved: {total_saved} | Skipped: {total_skipped}")
    print("Per-caliber counts:")
    for cal in CALIBER_PATHS:
        print(f"  {cal}: {counts.get(cal, 0)}")

    # Loud-failure check — every caliber URL 404'd / failed to load.
    # Returning a non-zero exit code makes the GitHub Actions step go
    # red and triggers the health-check email so silent staleness can
    # never repeat the May 2026 incident, where a wholesale Shopify
    # migration broke every URL but the scraper still printed
    # "Saved: 0" and exited 0 for nine consecutive days.
    if successful_calibers == 0:
        print("\nFATAL: every Black Basin caliber URL failed to load. "
              "Site may have moved or migrated again — manual check needed.")
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(scrape())
