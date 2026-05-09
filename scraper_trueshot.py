import json
import os
import re
import sys
import urllib.request
from datetime import datetime, timezone
from supabase import create_client

from scraper_lib import CALIBERS, normalize_caliber, now_iso, with_stock_fields, parse_purchase_limit, parse_brand, sanity_check_ppr, parse_bullet_type

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
RETAILER_ID = 15
SITE_BASE = "https://www.trueshotammo.com"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
)

# Trueshot Shopify collection handles per caliber. Values are lists
# because some calibers resolve to more than one collection — TrueShot
# split .223 and 5.56 into separate collections during their 2026-05-09
# storefront restructure, so 223-556 now crawls both and merges results
# (Shopify product handles are global, so seen_ids dedups any overlap).
# The five non-list-only handles below were also renamed in that
# restructure: the prior values silently 404'd and produced empty pages
# until the audit on 2026-05-09 caught the absence.
COLLECTION_HANDLES = {
    '9mm':     ['ammunition-pistol-ammo-9mm'],
    '380acp':  ['ammunition-pistol-ammo-380-auto'],
    '40sw':    ['ammunition-pistol-ammo-40-sw'],
    '38spl':   ['ammunition-pistol-ammo-38-special'],
    '357mag':  ['ammunition-pistol-ammo-357-magnum'],
    '22lr':    ['ammunition-rimfire-ammo-22-long-rifle'],
    '223-556': ['ammunition-rifle-ammo-223-rem',
                'ammunition-rifle-ammo-5-56x45mm'],
    '308win':  ['ammunition-rifle-ammo-308-win'],
    '762x39':  ['ammunition-rifle-ammo-7-62x39'],
    '300blk':  ['ammunition-rifle-ammo-300-blackout'],
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


def parse_condition(text):
    t = text.lower()
    if 'reman' in t or 'remanufactured' in t:
        return 'Remanufactured'
    return 'New'


def scrape_caliber(caliber_norm, caliber_display, seen_ids):
    """Scrape every configured collection for a caliber.

    Returns (rows, flags) where flags is a list of (handle,
    empty_first_page) tuples. The orchestrator in scrape() uses the
    flags to fire the storefront-drift guardrail when too many
    collections silently return zero products.
    """
    rows = []
    flags = []
    for handle in COLLECTION_HANDLES[caliber_norm]:
        empty_first_page = False
        page_num = 1
        while True:
            url = f"{SITE_BASE}/collections/{handle}/products.json?page={page_num}&limit=250"
            print(f"\n[{caliber_norm}/{handle}] page {page_num}: {url}")
            try:
                data = fetch_json(url)
            except Exception as e:
                print(f"  Fetch failed: {e}")
                break

            products = data.get("products", [])
            if not products:
                if page_num == 1:
                    empty_first_page = True
                    # Loud, grep-friendly line so the cause is obvious
                    # in CI logs even if the run as a whole succeeds.
                    print(f"  WARN: TrueShot collection {handle} returned "
                          f"zero products on first page (caliber {caliber_norm}).")
                else:
                    print(f"  No products on page {page_num}, stopping handle.")
                break

            for p in products:
                title = p.get("title", "")
                handle_p = p.get("handle", "")
                vendor_raw = (p.get("vendor") or "").strip() or None
                vendor = parse_brand(title) or parse_brand(vendor_raw or '') or vendor_raw or 'Unknown'
                if not handle_p:
                    continue

                # Strict: require positive in-list detection from the title.
                # Off-list calibers (.45 ACP, .38 Super, 10mm, etc.) make
                # normalize_caliber return None — we want to skip those, not
                # silently bucket them under the collection's caliber.
                _, detected = normalize_caliber(title)
                if detected != caliber_norm:
                    continue

                grain = parse_grain(title)
                case_material = parse_case_material(title)
                bullet_type = parse_bullet_type(title)
                condition = parse_condition(title)
                # Shopify exposes tags and body_html; check both for limit copy.
                tag_text = ' '.join(p.get('tags') or [])
                body_html = p.get('body_html') or ''
                purchase_limit = parse_purchase_limit(title) or \
                                 parse_purchase_limit(tag_text) or \
                                 parse_purchase_limit(body_html)

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
                        if not sanity_check_ppr(ppr, price, rounds, context=title[:60], caliber=caliber_norm):
                            continue

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
                            'purchase_limit': purchase_limit,
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
        flags.append((handle, empty_first_page))
    return rows, flags


def scrape():
    all_rows = []
    seen_ids = set()
    empty_handles = []  # list of (caliber_norm, handle) for guardrail
    for caliber_norm in COLLECTION_HANDLES:
        caliber_display = CALIBERS[caliber_norm]
        rows, flags = scrape_caliber(caliber_norm, caliber_display, seen_ids)
        all_rows.extend(rows)
        for handle, empty in flags:
            if empty:
                empty_handles.append((caliber_norm, handle))

    print(f"\nTotal scraped: {len(all_rows)}")

    # Storefront-drift guardrail. A single transient empty collection
    # is fine; three or more is a strong signal that TrueShot renamed
    # collection handles and the scraper is silently producing partial
    # data (the exact symptom that hid 5 of 10 calibers from the DB
    # until the 2026-05-09 audit). Exit non-zero so CI runs go red.
    EMPTY_FAIL_THRESHOLD = 3
    if len(empty_handles) >= EMPTY_FAIL_THRESHOLD:
        print(f"\nFAIL: {len(empty_handles)} TrueShot collections returned "
              f"zero products on first page — likely storefront drift:")
        for cal, h in empty_handles:
            print(f"  - {cal}: TrueShot collection {h} returned zero products on first page")
        sys.exit(1)
    elif empty_handles:
        print(f"\nWARN: {len(empty_handles)} TrueShot collection(s) returned "
              f"zero products on first page (transient or worth investigating):")
        for cal, h in empty_handles:
            print(f"  - {cal}: TrueShot collection {h} returned zero products on first page")

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
