import json
import os
import re
import urllib.request
from datetime import datetime, timezone
from supabase import create_client

from scraper_lib import CALIBERS, normalize_caliber, now_iso, with_stock_fields

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
RETAILER_ID = 15
SITE_BASE = "https://www.trueshotammo.com"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
)

# Trueshot Shopify collection handles per caliber.
COLLECTION_HANDLES = {
    '9mm':     'ammunition-pistol-ammo-9mm',
    '380acp':  'ammunition-pistol-ammo-380-auto',
    '40sw':    'ammunition-pistol-ammo-40-s-w',
    '38spl':   'ammunition-pistol-ammo-38-special',
    '357mag':  'ammunition-pistol-ammo-357-magnum',
    '22lr':    'ammunition-rimfire-ammo-22-lr',
    '223-556': 'ammunition-rifle-ammo-223-rem-556-nato',
    '308win':  'ammunition-rifle-ammo-308-win-762-nato',
    '762x39':  'ammunition-rifle-ammo-762x39',
    '300blk':  'ammunition-rifle-ammo-300-blackout',
}

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=45) as resp:
        return json.loads(resp.read())

def parse_variant_rounds(variant_title, product_title=""):
    m = re.search(r'(\d+)', variant_title)
    if m:
        return int(m.group(1))
    primary = re.search(r'(\d+)\s*rounds?\s*of\b', product_title, re.IGNORECASE)
    if primary:
        return int(primary.group(1))
    box = re.search(r'(\d+)\s*(?:rd|rds)\s*box', product_title, re.IGNORECASE)
    if box:
        return int(box.group(1))
    return None

def parse_grain(text):
    m = re.search(r'(\d+)\s*(?:grain|gr)\b', text, re.IGNORECASE)
    return int(m.group(1)) if m else None

def parse_case_material(text):
    t = text.lower()
    steel_brands = ['wolf', 'tula', 'tulammo', 'brown bear', 'silver bear', 'barnaul', 'red army']
    if 'steel case' in t or 'steel-case' in t:
        return 'Steel'
    if any(b in t for b in steel_brands):
        return 'Steel'
    if 'aluminum' in t or 'aluminium' in t:
        return 'Aluminum'
    if 'brass' in t:
        return 'Brass'
    if 'nickel' in t:
        return 'Nickel'
    if 'polymer' in t:
        return 'Polymer'
    return 'Brass'

def parse_bullet_type(text):
    u = text.upper()
    if 'JHP' in u or 'HOLLOW POINT' in u or 'BJHP' in u:
        return 'JHP'
    if 'TMJ' in u:
        return 'TMJ'
    if 'FMJ' in u or 'FULL METAL JACKET' in u:
        return 'FMJ'
    if 'LRN' in u or 'LEAD ROUND' in u:
        return 'LRN'
    if 'JSP' in u:
        return 'JSP'
    if 'FRANGIBLE' in u:
        return 'Frangible'
    if 'FTX' in u or 'FLEXLOCK' in u or 'XTP' in u or 'HONEYBADGER' in u:
        return 'JHP'
    if 'INCENDIARY' in u:
        return 'Incendiary'
    if 'BLANK' in u:
        return 'Blank'
    if 'HP' in u:
        return 'JHP'
    return 'FMJ'

def parse_condition(text):
    t = text.lower()
    if 'reman' in t or 'remanufactured' in t:
        return 'Remanufactured'
    return 'New'


def scrape_caliber(caliber_norm, caliber_display, seen_ids):
    handle = COLLECTION_HANDLES[caliber_norm]
    rows = []
    page_num = 1
    while True:
        url = f"{SITE_BASE}/collections/{handle}/products.json?page={page_num}&limit=250"
        print(f"\n[{caliber_norm}] page {page_num}: {url}")
        try:
            data = fetch_json(url)
        except Exception as e:
            print(f"  Fetch failed: {e}")
            break

        products = data.get("products", [])
        if not products:
            print(f"  No products on page {page_num}, stopping caliber.")
            break

        for p in products:
            title = p.get("title", "")
            handle_p = p.get("handle", "")
            vendor = (p.get("vendor") or "").strip() or None
            if not handle_p:
                continue

            # Defensive: confirm caliber matches title - skip cross-listed items.
            _, detected = normalize_caliber(title)
            if detected and detected != caliber_norm:
                continue

            grain = parse_grain(title)
            case_material = parse_case_material(title)
            bullet_type = parse_bullet_type(title)
            condition = parse_condition(title)

            for v in p.get("variants", []):
                try:
                    variant_title = v.get("title", "") or ""
                    rounds = parse_variant_rounds(variant_title, title)
                    if not rounds or rounds < 1:
                        continue

                    price_raw = v.get("price")
                    if price_raw is None:
                        continue
                    price = float(price_raw)
                    if price <= 0:
                        continue

                    available = bool(v.get("available", False))
                    variant_id = v.get("id")
                    link = f"{SITE_BASE}/products/{handle_p}?variant={variant_id}" if variant_id else f"{SITE_BASE}/products/{handle_p}"
                    product_id = (f"{handle_p}-{variant_id}" if variant_id else handle_p)[:100]
                    if product_id in seen_ids:
                        continue
                    seen_ids.add(product_id)
                    ppr = round(price / rounds, 4)

                    row = {
                        'retailer_id': RETAILER_ID,
                        'retailer_product_id': product_id,
                        'caliber': caliber_display,
                        'caliber_normalized': caliber_norm,
                        'product_url': link,
                        'base_price': round(price, 2),
                        'price_per_round': ppr,
                        'rounds_per_box': rounds,
                        'total_rounds': rounds,
                        'manufacturer': vendor,
                        'grain': grain,
                        'bullet_type': bullet_type,
                        'case_material': case_material,
                        'condition_type': condition,
                        'last_updated': now_iso(),
                    }
                    with_stock_fields(row, available)
                    rows.append(row)
                    print(f"  [ok] {title[:50]} | {rounds}rd | ${price} | {ppr:.2f}/rd | {'in' if available else 'OUT'}")
                except Exception as e:
                    print(f"  Error on variant: {e}")
                    continue

        page_num += 1
        if page_num > 10:
            break
    return rows


def scrape():
    all_rows = []
    seen_ids = set()
    for caliber_norm in COLLECTION_HANDLES:
        caliber_display = CALIBERS[caliber_norm]
        rows = scrape_caliber(caliber_norm, caliber_display, seen_ids)
        all_rows.extend(rows)

    print(f"\nTotal scraped: {len(all_rows)}")

    if not all_rows:
        print("Nothing to upsert.")
        return

    now = now_iso()
    for row in all_rows:
        try:
            result = supabase.table('listings').upsert(
                row,
                on_conflict='retailer_id,retailer_product_id'
            ).execute()

            if result.data:
                listing_id = result.data[0]['id']
                supabase.table('price_history').insert({
                    'listing_id': listing_id,
                    'price': row['base_price'],
                    'price_per_round': row['price_per_round'],
                    'in_stock': row['in_stock'],
                    'recorded_at': now,
                }).execute()

        except Exception as e:
            print(f"  DB error for {row.get('manufacturer','?')}: {e}")

    print("Done.")

if __name__ == "__main__":
    scrape()
