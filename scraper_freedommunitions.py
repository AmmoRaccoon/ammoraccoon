import json
import os
import re
import urllib.request
from datetime import datetime, timezone
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
RETAILER_ID = 18
BASE_URL = "https://www.freedommunitions.com"
COLLECTION_HANDLE = "pistol-9mm"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=45) as resp:
        return json.loads(resp.read())

def parse_rounds(text, variant_title=""):
    # Freedom Munitions titles: "... 50 rounds, ..." / "... 50 Rounds ..."
    primary = re.search(r'(\d[\d,]*)\s*rounds?\b', text, re.IGNORECASE)
    if primary:
        return int(primary.group(1).replace(',', ''))
    # Fallback: numeric variant title
    if variant_title and variant_title.lower() != 'default title':
        m = re.search(r'(\d+)', variant_title)
        if m:
            return int(m.group(1))
    return None

def parse_grain(text):
    m = re.search(r'(\d+)\s*(?:grain|gr)\.?\b', text, re.IGNORECASE)
    return int(m.group(1)) if m else None

def parse_case_material(text):
    t = text.lower()
    steel_brands = ['wolf', 'tula', 'tulammo', 'brown bear', 'silver bear', 'barnaul', 'red army']
    if 'steel case' in t or 'steel-case' in t:
        return 'Steel'
    if any(b in t for b in steel_brands):
        return 'Steel'
    if 'steel' in t:
        return 'Steel'
    if 'aluminum' in t or 'aluminium' in t:
        return 'Aluminum'
    if 'nickel' in t:
        return 'Nickel'
    if 'brass' in t:
        return 'Brass'
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
    if 'ROUND NOSE' in u or ' RN' in u or u.endswith('RN') or '(RN)' in u:
        return 'FMJ'  # Round Nose is typically FMJ-RN
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

def is_nine_mm(text):
    t = text.lower()
    return '9mm' in t or '9 mm' in t or '9x19' in t or 'luger' in t

def scrape():
    all_rows = []
    seen_ids = set()
    page_num = 1
    while True:
        url = f"{BASE_URL}/collections/{COLLECTION_HANDLE}/products.json?page={page_num}&limit=250"
        print(f"Fetching page {page_num}: {url}")
        try:
            data = fetch_json(url)
        except Exception as e:
            print(f"  Fetch failed: {e}")
            break

        products = data.get("products", [])
        if not products:
            print(f"  No products on page {page_num}, stopping.")
            break

        for p in products:
            title = p.get("title", "")
            handle = p.get("handle", "")
            vendor = (p.get("vendor") or "").strip() or None
            if not handle or not is_nine_mm(title):
                continue

            grain = parse_grain(title)
            case_material = parse_case_material(title)
            bullet_type = parse_bullet_type(title)
            condition = parse_condition(title)

            for v in p.get("variants", []):
                try:
                    variant_title = v.get("title", "") or ""
                    rounds = parse_rounds(title, variant_title)
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
                    link = f"{BASE_URL}/products/{handle}"
                    product_id = (f"{handle}-{variant_id}" if variant_id else handle)[:100]
                    if product_id in seen_ids:
                        continue
                    seen_ids.add(product_id)
                    ppr = round(price / rounds * 100, 4)

                    row = {
                        'retailer_id': RETAILER_ID,
                        'retailer_product_id': product_id,
                        'caliber': '9mm',
                        'caliber_normalized': '9mm',
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
                        'in_stock': available,
                        'last_updated': datetime.now(timezone.utc).isoformat(),
                    }
                    all_rows.append(row)
                    print(f"  [ok] {title[:55]} | {rounds}rd | ${price} | {ppr:.2f}c/rd | {'in' if available else 'OUT'}")
                except Exception as e:
                    print(f"  Error on variant: {e}")
                    continue

        page_num += 1

    print(f"\nTotal scraped: {len(all_rows)}")

    if not all_rows:
        print("Nothing to upsert.")
        return

    now = datetime.now(timezone.utc).isoformat()
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
