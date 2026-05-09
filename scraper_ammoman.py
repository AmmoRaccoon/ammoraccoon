"""scraper_ammoman.py — AmmoMan.com ammunition scraper.

Magento 2 storefront. Plain `requests` returns full SSR'd HTML with a
clean Product JSON-LD block on every in-stock PDP. No anti-bot in front.

Discovery strategy: walk /sitemap.xml (a flat XML, ~4500 URLs), filter
to product PDPs (slug contains "-grain-") whose slug normalize_caliber()'s
to one of our 10 tracked calibers — yields ~1800 in-scope PDPs. Skip
out-of-scope calibers (45 ACP, 6.5 Creedmoor, 12-gauge, etc.) without
fetching their pages.

Per-PDP data comes from the Product JSON-LD <script> block:
  - name (e.g. "9MM LUGER FIOCCHI 147 GRAIN JHP (1000 ROUNDS)")
  - mpn (manufacturer SKU)
  - brand.name
  - offers.price (string)
  - offers.availability ("http://schema.org/InStock" | "OutOfStock")
  - offers.url (canonical PDP URL)
  - offers.inventoryLevel (int — captured but not yet stored)

Three traps the parser must NOT fall into (full recon in
scripts/ammoman_unit_mapping.md, web repo):

  - ~1-2% of OOS / discontinued PDPs OMIT the Product JSON-LD block. v1
    skips those (logged as `[NO-JSON-LD]`); they reappear when restocked.
  - Page text "29 in Stock" is JS-rendered. The authoritative stock
    signal is `offers.availability` in the JSON-LD.
  - The Magento `mpn` is shared across pack-count variants of the same
    SKU line (e.g. 53955 covers both 50-round and 1000-round packs).
    Use the URL slug as `retailer_product_id` so variants don't collide
    on the unique constraint (retailer_id, retailer_product_id).

Robots.txt has no Crawl-delay; v1 uses 5s between requests — half the
Powder Valley posture (which has explicit 10s) since AmmoMan is more
heavily resourced.
"""

import argparse
import html as htmllib
import json
import os
import re
import sys
import time
from datetime import datetime
from typing import Optional

import requests
from dotenv import load_dotenv
from supabase import create_client

from scraper_lib import (
    normalize_caliber, now_iso, with_stock_fields,
    parse_purchase_limit, parse_brand, sanity_check_ppr, clean_title,
    parse_bullet_type,
)

load_dotenv()

RETAILER_SLUG = "ammoman"
SITE_BASE = "https://www.ammoman.com"
SITEMAP_URL = f"{SITE_BASE}/sitemap.xml"

CRAWL_DELAY_SEC = 5
USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
)
REQUEST_TIMEOUT = 30


# ---------- HTTP ----------

def fetch(url: str) -> str:
    r = requests.get(
        url,
        headers={
            'User-Agent': USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'identity',
        },
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    return r.text


# ---------- Sitemap walking ----------

_LOC_RE = re.compile(r'<loc>([^<]+)</loc>', re.IGNORECASE)


def discover_product_urls() -> list[tuple[str, str]]:
    """Fetch sitemap, return [(url, caliber_normalized)] for in-scope PDPs.

    Filters in two passes:
      1. URL slug must contain "-grain-" (product, not category).
      2. Slug-as-text must normalize_caliber() to one of our 10 tracked
         calibers — anything else (45 ACP, 6.5 Creedmoor, 12-gauge, ...)
         is dropped without spending a PDP fetch.
    """
    print(f'[sitemap] fetching {SITEMAP_URL}')
    xml = fetch(SITEMAP_URL)
    urls = _LOC_RE.findall(xml)
    print(f'[sitemap] {len(urls)} total URLs')

    products = [u for u in urls if '-grain-' in u]
    print(f'[sitemap] {len(products)} product URLs (have "-grain-")')

    in_scope: list[tuple[str, str]] = []
    for u in products:
        slug = u.rsplit('/', 1)[-1].replace('-', ' ')
        _, cal_norm = normalize_caliber(slug)
        if cal_norm:
            in_scope.append((u, cal_norm))
    print(f'[sitemap] {len(in_scope)} match our 10 tracked calibers')
    return in_scope


# ---------- PDP parsing ----------

_LDJSON_RE = re.compile(
    r'<script[^>]*type=[\'"]application/ld\+json[\'"][^>]*>(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)
_ROUNDS_RE = re.compile(r'\((\d[\d,]*)\s+ROUNDS?\)', re.IGNORECASE)


def extract_product_jsonld(html: str) -> Optional[dict]:
    """Return the @type=Product block from a PDP, or None.

    AmmoMan PDPs ship three JSON-LD blocks: WebSite, BreadcrumbList,
    Product. OOS/discontinued PDPs omit the Product block entirely
    (Trap #1) — caller treats None as "skip this URL".
    """
    for raw in _LDJSON_RE.findall(html):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict) and data.get('@type') == 'Product':
            return data
    return None


def parse_grain(text: str) -> Optional[int]:
    m = re.search(r'(\d+)\s*gr(?:ain)?\b', text, re.IGNORECASE)
    return int(m.group(1)) if m else None


def parse_rounds(text: str) -> Optional[int]:
    """AmmoMan title format: '... (1000 ROUNDS)' — consistent across catalog."""
    m = _ROUNDS_RE.search(text)
    if not m:
        return None
    try:
        return int(m.group(1).replace(',', ''))
    except ValueError:
        return None


def parse_case_material(text: str) -> str:
    t = text.lower()
    steel_brands = ('wolf', 'tula', 'tulammo', 'brown bear', 'silver bear',
                    'golden bear', 'barnaul')
    if any(b in t for b in steel_brands) or 'steel case' in t \
            or 'steel-case' in t or ' steel ' in f' {t} ':
        return 'Steel'
    if 'brass' in t:
        return 'Brass'
    if 'aluminum' in t:
        return 'Aluminum'
    if 'nickel' in t:
        return 'Nickel'
    return 'Brass'


def parse_country(text: str) -> Optional[str]:
    t = text.lower()
    mapping = {
        'federal': 'USA', 'winchester': 'USA', 'remington': 'USA',
        'cci': 'USA', 'speer': 'USA', 'hornady': 'USA',
        'blazer': 'USA', 'fiocchi': 'USA', 'american eagle': 'USA',
        'black hills': 'USA',
        'magtech': 'Brazil', 'cbc': 'Brazil',
        'ppu': 'Serbia', 'prvi partizan': 'Serbia',
        'sellier': 'Czech Republic', 'tula': 'Russia',
        'wolf': 'Russia', 'aguila': 'Mexico', 'sterling': 'Turkey',
    }
    for needle, country in mapping.items():
        if needle in t:
            return country
    return None


def parse_pdp(url: str, html: str) -> Optional[dict]:
    """Return a parsed listing dict, or None to skip this PDP."""
    pd = extract_product_jsonld(html)
    if not pd:
        return None

    name_raw = pd.get('name') or ''
    name = clean_title(htmllib.unescape(name_raw))
    if not name:
        return None

    offers = pd.get('offers') or {}
    if isinstance(offers, list):
        # Schema allows a list; AmmoMan uses a single offer in practice.
        offers = offers[0] if offers else {}
    avail = (offers.get('availability') or '').lower()
    in_stock = 'instock' in avail.replace(' ', '')

    price_raw = offers.get('price')
    try:
        base_price = float(str(price_raw).replace(',', ''))
    except (TypeError, ValueError):
        return None
    if base_price <= 0:
        return None

    total_rounds = parse_rounds(name)
    if not total_rounds or total_rounds <= 0:
        return None
    price_per_round = round(base_price / total_rounds, 4)

    mpn = pd.get('mpn') or ''
    brand_obj = pd.get('brand') or {}
    brand_jsonld = brand_obj.get('name') if isinstance(brand_obj, dict) else None

    # retailer_product_id = URL slug (last segment). The mpn is shared
    # across pack-count variants of the same SKU line — using the slug
    # keeps each variant uniquely keyed.
    slug_id = url.rsplit('/', 1)[-1]

    return {
        'pid': slug_id,
        'mpn': mpn,
        'url': url,
        'title': name,
        'in_stock': in_stock,
        'base_price': base_price,
        'price_per_round': price_per_round,
        'total_rounds': total_rounds,
        'brand_jsonld': brand_jsonld,
    }


# ---------- Entrypoint ----------

def main() -> int:
    ap = argparse.ArgumentParser(description='Scrape AmmoMan.com ammunition listings.')
    ap.add_argument('--dry-run', action='store_true',
                    help='Parse and print only; no DB writes.')
    ap.add_argument('--limit-products', type=int, default=None,
                    help='Cap total products processed (dev/test).')
    ap.add_argument('--crawl-delay', type=float, default=CRAWL_DELAY_SEC,
                    help=f'Seconds between PDP fetches (default {CRAWL_DELAY_SEC}).')
    args = ap.parse_args()

    supabase = None
    retailer_id: int = 0
    if not args.dry_run:
        supabase = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])
        rr = supabase.table('retailers').select('id').eq('slug', RETAILER_SLUG).execute()
        if not rr.data:
            print(f"ERROR: Retailer '{RETAILER_SLUG}' not found in retailers table")
            return 1
        retailer_id = rr.data[0]['id']

    print(f'[{datetime.now().isoformat()}] AmmoMan scraper starting '
          f'(mode={"DRY RUN" if args.dry_run else "LIVE"}, '
          f'crawl_delay={args.crawl_delay}s, retailer_id={retailer_id})')

    try:
        urls = discover_product_urls()
    except Exception as e:
        print(f'[FETCH-ERR] sitemap fetch failed: {e}')
        return 1

    if args.limit_products is not None:
        urls = urls[:args.limit_products]
        print(f'[sitemap] capped to {len(urls)} for this run')

    saved_total = 0
    skipped_total = 0
    no_jsonld_total = 0
    errors: list[str] = []
    seen_pids: set[str] = set()

    for i, (url, cal_hint) in enumerate(urls, 1):
        if i > 1:
            time.sleep(args.crawl_delay)

        try:
            html = fetch(url)
        except Exception as e:
            msg = f'[{cal_hint}] fetch failed for {url}: {e}'
            print(f'  [FETCH-ERR] {msg}')
            errors.append(msg)
            continue

        try:
            row = parse_pdp(url, html)
        except Exception as e:
            msg = f'[{cal_hint}] parse failed for {url}: {e}'
            print(f'  [PARSE-ERR] {msg}')
            errors.append(msg)
            skipped_total += 1
            continue

        if row is None:
            # Distinguish no-JSON-LD (Trap #1) from generic skip — those
            # are expected (OOS/discontinued PDPs) so don't pollute the
            # error list. Just count and log quietly.
            if not extract_product_jsonld(html):
                no_jsonld_total += 1
                print(f'  [NO-JSON-LD] {url[len(SITE_BASE):]}')
            else:
                skipped_total += 1
            continue

        # Re-derive caliber from the product NAME (more authoritative than
        # the URL slug) and confirm it still matches the discovery hint.
        cal_disp, cal_norm = normalize_caliber(row['title'])
        if not cal_norm:
            skipped_total += 1
            continue

        if not sanity_check_ppr(
            row['price_per_round'], row['base_price'], row['total_rounds'],
            context=f'{RETAILER_SLUG} {cal_norm}', caliber=cal_norm,
        ):
            skipped_total += 1
            continue

        if row['pid'] in seen_pids:
            continue
        seen_pids.add(row['pid'])

        grain = parse_grain(row['title'])
        bullet_type = parse_bullet_type(row['title'])
        case_material = parse_case_material(row['title'])
        country = parse_country(row['title'])
        # Prefer brand from the title (parse_brand normalizes to the
        # canonical short form). The JSON-LD brand string runs through
        # parse_brand too before being trusted verbatim — without it
        # JSON-LD values like "Sierra Bullets" / "Hornady Manufacturing"
        # / "Federal Premium Ammunition" leak into the manufacturer
        # column un-normalized. Final fallback is the raw JSON-LD
        # string for cases parse_brand doesn't know.
        manufacturer = (parse_brand(row['title'])
                        or parse_brand(row['brand_jsonld'])
                        or row['brand_jsonld']
                        or 'Unknown')
        purchase_limit = parse_purchase_limit(html)

        listing = {
            'retailer_id': retailer_id,
            'retailer_product_id': row['pid'],
            'product_url': row['url'],
            'caliber': cal_disp,
            'caliber_normalized': cal_norm,
            'grain': grain,
            'bullet_type': bullet_type,
            'case_material': case_material,
            'condition_type': 'New',
            'country_of_origin': country,
            'manufacturer': manufacturer,
            'rounds_per_box': row['total_rounds'],
            'boxes_per_case': 1,
            'total_rounds': row['total_rounds'],
            'base_price': row['base_price'],
            'price_per_round': row['price_per_round'],
            'purchase_limit': purchase_limit,
            'last_updated': now_iso(),
        }
        with_stock_fields(listing, row['in_stock'])

        if args.dry_run:
            print(
                f"  [DRY {i:>4}/{len(urls)}] cal={cal_norm:<8} "
                f"${row['base_price']:>7.2f} ({row['price_per_round']}/rd) "
                f"{'IN ' if row['in_stock'] else 'OUT'} mpn={row['mpn']:<10} "
                f"{row['title'][:55]}"
            )
            saved_total += 1
        else:
            try:
                result = supabase.table('listings').upsert(
                    listing, on_conflict='retailer_id,retailer_product_id',
                ).execute()
                supabase.table('price_history').insert({
                    'listing_id': result.data[0]['id'],
                    'price': row['base_price'],
                    'price_per_round': row['price_per_round'],
                    'in_stock': row['in_stock'],
                }).execute()
                saved_total += 1
                print(
                    f"  [{i:>4}/{len(urls)}] cal={cal_norm:<8} "
                    f"${row['base_price']:>7.2f} ({row['price_per_round']}/rd) "
                    f"{'IN ' if row['in_stock'] else 'OUT'} mpn={row['mpn']:<10} "
                    f"{row['title'][:55]}"
                )
            except Exception as e:
                msg = f'[{cal_norm}] {row["pid"]}: upsert failed: {e}'
                print(f'  [DB-ERR] {msg}')
                errors.append(msg)

    mode = 'DRY RUN' if args.dry_run else 'LIVE'
    print(f'\n=== TOTALS ({mode}) ===')
    print(f'  saved={saved_total}  skipped={skipped_total}  '
          f'no-jsonld={no_jsonld_total}  errors={len(errors)}')
    if errors:
        print(f'\n=== {len(errors)} ERROR(S) ===')
        for e in errors[:30]:
            print(f'  {e}')
        if len(errors) > 30:
            print(f'  ... and {len(errors) - 30} more')

    return 0 if not errors else 1


if __name__ == '__main__':
    sys.exit(main())
