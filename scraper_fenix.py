import os
import re
import sys
import urllib.request
import json
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client

from scraper_lib import (
    insert_price_history,
    CALIBERS, now_iso, with_stock_fields, parse_purchase_limit,
    parse_brand, sanity_check_ppr, clean_title, parse_bullet_type,
    normalize_caliber,
    mark_retailer_scraped,
    load_caliber_paths,
)

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

RETAILER_SLUG = "fenix"
SITE_BASE = "https://fenixammunition.com"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Per-caliber Shopify collection paths now live in caliber_paths/fenix.json
# (expansion #4 Step-2 migration) — migrated from the inline dict, parity-
# proven byte-identical (fetched URL). Per the blackbasin precedent the
# config stores the HTML category path '/collections/<handle>' and the
# scraper appends '/products.json' at runtime (the old literal embedded the
# suffix). REQUESTS-BASED (Shopify /products.json via urllib — no browser),
# so loader-only + no shared Playwright guards; a NEW blackbasin-style
# loud-failure guard is added in scrape() below (fenix previously had none).
# Fenix is a Michigan competition / defensive manufacturer that does not
# produce rimfire (.22 LR), revolver rounds (.38 Spl, .357 Mag), .308 Win,
# or 7.62x39, so those calibers are absent by design (would 404).
CALIBER_PATHS = load_caliber_paths('fenix')


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
    # Fenix titles use "(250 ct.)" / "(50 ct.)" — a `ct` pattern needs
    # to come first because parse_rounds in other scrapers doesn't
    # handle Fenix's count-shorthand. Standard rounds/rds patterns
    # follow as fallbacks.
    patterns = [
        r'\(\s*(\d[\d,]*)\s*ct\.?\s*\)',
        r'(\d[\d,]*)\s*ct\.?\s*\)',
        r'(\d[\d,]*)\s*[- ]?\s*rounds?\b',
        r'(\d[\d,]*)\s*[- ]?\s*rds?\b',
        r'(\d[\d,]*)\s*[- ]?\s*rd\s*(?:box|case|pack|count)',
        r'(\d[\d,]*)\s*per\s*box',
        r'(\d[\d,]*)\s*[- ]?\s*count\b',
        r'box\s*of\s*(\d[\d,]*)',
        r'case\s*of\s*(\d[\d,]*)',
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return int(m.group(1).replace(',', ''))
    return None


def parse_case_material(text):
    text_lower = text.lower()
    if 'aluminum' in text_lower:
        return 'Aluminum'
    if 'nickel' in text_lower:
        return 'Nickel'
    if 'steel' in text_lower:
        return 'Steel'
    return 'Brass'  # Fenix runs reloaded / new brass exclusively.




def parse_country(text):
    return 'USA'  # Novi, MI.


def fetch_products_json(path):
    """GET <path>/products.json as JSON. Returns (products, ok). ok is
    False when the request failed to load (404/timeout/etc.) — the
    loud-failure guard in scrape() uses it so a wholesale Shopify
    migration that 404s every collection fails the run loudly instead of
    silently saving zero (fenix previously had NO such guard). The config
    stores the '/collections/<handle>' path; the '/products.json' suffix
    is appended here at runtime (blackbasin precedent)."""
    url = f"{SITE_BASE}{path}/products.json"
    req = urllib.request.Request(url, headers={
        'User-Agent': USER_AGENT,
        'Accept': 'application/json',
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
        return data.get('products', []) or [], True
    except Exception as e:
        print(f"  fetch failed for {url}: {e}")
        return [], False


def scrape_caliber(path, caliber_norm, caliber_display, retailer_id, seen_ids, counts):
    print(f"\n[{caliber_norm}] GET {path}/products.json")
    products, ok = fetch_products_json(path)
    if not ok:
        print(f"  FAILED to load {path}")
        return 0, 0, False
    print(f"  {len(products)} products in feed")
    saved = 0
    skipped = 0

    for prod in products:
        try:
            title = clean_title(prod.get('title') or '')
            handle = (prod.get('handle') or '').strip()
            vendor = (prod.get('vendor') or '').strip()
            if not title or not handle:
                skipped += 1
                continue

            # Re-tag by TITLE, never trust the category page. A category
            # can cross-list a lookalike (an off-list cartridge or a
            # different tracked caliber); normalize_caliber re-derives the
            # real caliber and a title that maps to nothing tracked is
            # dropped (honest blank), never force-tagged by the category.
            # title is already clean_title'd above (blackbasin precedent).
            cal_disp, cal_norm = normalize_caliber(title)
            if not cal_norm:
                skipped += 1
                continue

            product_url = f"{SITE_BASE}/products/{handle}"

            # /brands/ guard kept for parity with the rest of the suite —
            # never observed on Shopify product slugs but cheap to keep.
            if '/brands/' in product_url:
                skipped += 1
                continue

            variants = prod.get('variants') or []
            if not variants:
                skipped += 1
                continue
            v0 = variants[0]
            try:
                base_price = float(v0.get('price') or 0)
            except (TypeError, ValueError):
                base_price = 0
            if base_price <= 0:
                skipped += 1
                continue

            total_rounds = parse_rounds(title)
            if not total_rounds or total_rounds <= 0:
                # Fenix's "Custom Bag Run" / non-ammo configurator
                # products have no count in the title and a placeholder
                # price — silently skip.
                skipped += 1
                continue

            price_per_round = round(base_price / total_rounds, 4)
            if not sanity_check_ppr(price_per_round, base_price, total_rounds,
                                    context=f'{RETAILER_SLUG} {cal_norm}',
                                    caliber=cal_norm):
                skipped += 1
                continue

            # Stock — Shopify variants expose `available` directly. Use
            # the first variant's flag (Fenix never multi-pack-variants
            # within one product as of probe day).
            in_stock = bool(v0.get('available'))

            grain = parse_grain(title)
            case_material = parse_case_material(title)
            bullet_type = parse_bullet_type(title)
            country = parse_country(title)
            manufacturer = parse_brand(title) or vendor or 'Fenix'
            purchase_limit = parse_purchase_limit(title)

            # Fenix's variant SKU is stable and short ("FA9115250") —
            # use it as retailer_product_id when present, otherwise
            # fall back to the handle.
            product_id = (v0.get('sku') or '').strip() or handle
            product_id = product_id[:100]
            if not product_id or product_id in seen_ids:
                continue
            seen_ids.add(product_id)

            listing = {
                'retailer_id': retailer_id,
                'retailer_product_id': product_id,
                'product_url': product_url,
                'caliber': cal_disp,
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
            insert_price_history(supabase, {
                'listing_id': result.data[0]['id'],
                'price': base_price,
                'price_per_round': price_per_round,
                'in_stock': in_stock,
            })

            saved += 1
            counts[caliber_norm] = counts.get(caliber_norm, 0) + 1
            print(f"  Saved [{caliber_norm}]: {title[:55]} | ${base_price} | {price_per_round}/rd")

        except Exception as e:
            skipped += 1
            print(f"  Skipped: {e}")
            continue

    return saved, skipped, True


def scrape():
    print(f"[{datetime.now()}] Starting Fenix Ammunition scraper (calibers Fenix produces)...")
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
        caliber_ok = False
        for entry in CALIBER_PATHS[caliber_norm]:
            saved, skipped, ok = scrape_caliber(entry['url'], caliber_norm,
                                                caliber_display, retailer_id,
                                                seen_ids, counts)
            total_saved += saved
            total_skipped += skipped
            if ok:
                caliber_ok = True
        if caliber_ok:
            successful_calibers += 1

    print(f"\nDone! Saved: {total_saved} | Skipped: {total_skipped}")
    print("Per-caliber counts:")
    for cal in CALIBERS:
        print(f"  {cal}: {counts.get(cal, 0)}")

    # Loud-failure guard (NEW 2026-06-14, expansion #4 Step-2 — fenix had
    # NO drift guard at all before this; this is an ADD, not a swap, per
    # Jon's call). If every Fenix collection URL failed to load, a
    # wholesale Shopify migration has broken the storefront: exit non-zero
    # so CI goes red and last_scraped_at is NOT bumped (mirrors blackbasin).
    if successful_calibers == 0:
        print("\nFATAL: every Fenix caliber URL failed to load. "
              "Site may have moved or migrated again — manual check needed.")
        return 1

    mark_retailer_scraped(supabase, retailer_id)
    return 0


if __name__ == '__main__':
    sys.exit(scrape())
