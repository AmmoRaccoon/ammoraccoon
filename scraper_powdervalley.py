"""scraper_powdervalley.py — Powder Valley reloading-component scraper.

Scrapes powder, primer, bullet, brass component listings from
powdervalley.com. Writes one row per variant SKU to the `components`
table (see migrations/009_components.sql).

Site shape (WordPress + WooCommerce + Yoast):
  Categories: /product-category/reloading-supplies/reloading-components/{type}/
  Pagination: append /page/N/   (1-indexed; 404 past the last page)
  Products:   /product/{slug}/

Why no Playwright: Powder Valley is fronted by Cloudflare in CDN/cache mode,
not challenge mode. A plain `requests` GET with a real UA returns rendered
HTML including the JSON-LD product graph. Verified during recon.

Per-variant data sources (in order of preference):
  1. JSON-LD ProductGroup.hasVariant[]  → sku, offers.priceSpecification.price.
     Powder also exposes `size`. Bullet/brass do NOT — pack count must come
     from the variant's URL query (?attribute_pa_quantity=N) or name suffix
     (" - 100"), since schema.org has no `quantity` property.
  2. data-product_variations JSON on the WC variations form → per-variant
     is_in_stock. Used to override the page-level .stock div for variant
     pages where some sizes are stocked and others are not.
  3. WC additional-info <table> (Quantity / Size rows) → pack count for
     single-SKU products that aren't part of a ProductGroup.

Unit conventions (per scripts/powdervalley_unit_mapping.md in the web repo):
  powder  → pa_size values are POUNDS  → package_unit='lbs'
  primer  → no variants, fixed-count box → package_unit='pieces'
  bullet  → pa_quantity values are PIECES → package_unit='pieces'
  brass   → pa_quantity values are PIECES → package_unit='pieces'

Three traps the parser must NOT fall into (from the spike doc):
  - Don't read pack size from the additional-info "Weight" row — that's
    shipping weight, not pack size.
  - Don't read pack size from the URL slug — slugs lie (e.g.
    starline-224-valkyrie-brass-50 is actually 100ct).
  - Don't assume ProductGroup; some products are plain @type:Product.

Robots.txt: 10s crawl-delay across all User-agents. Respected via
time.sleep(CRAWL_DELAY_SEC) after every request. Disallowed /pa_* attribute
URLs are never visited — variants come from the canonical product page.

Required env: SUPABASE_URL, SUPABASE_KEY (skipped under --dry-run).

Usage:
  python scraper_powdervalley.py --dry-run --limit-products 3   # quick smoke
  python scraper_powdervalley.py --dry-run --category powder    # one category
  python scraper_powdervalley.py                                # full live run
"""

import argparse
import html as htmllib
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

RETAILER_SLUG = 'powdervalley'
SITE_BASE = 'https://www.powdervalley.com'
USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
)
CRAWL_DELAY_SEC = 10  # robots.txt: User-agent: *  Crawl-delay: 10
PAGE_HARD_CAP = 50    # safety net so a runaway loop can't walk forever

CATEGORY_PATHS = {
    'powder': '/product-category/reloading-supplies/reloading-components/smokeless-powder/',
    'primer': '/product-category/reloading-supplies/reloading-components/primers/',
    'bullet': '/product-category/reloading-supplies/reloading-components/bullets/',
    'brass':  '/product-category/reloading-supplies/reloading-components/brass/',
}

# (category) → unit string written to components.package_unit.
CATEGORY_UNIT = {
    'powder': 'lbs',
    'primer': 'pieces',
    'bullet': 'pieces',
    'brass':  'pieces',
}

# (category) → minimum product URL count expected from a clean full discovery.
# Sized well below historical baselines so normal site churn doesn't false-positive,
# but above zero so a Cloudflare block on page 1 (which currently breaks pagination
# silently — see the 2026-05-05 bullet incident) can't masquerade as a green run.
# Bypassed when --limit-pages or --limit-products is set, since those flags imply
# a dev/test run that intentionally truncates discovery.
CATEGORY_MIN_DISCOVERED = {
    'powder': 50,
    'primer': 20,
    'bullet': 200,
    'brass':  10,
}


@dataclass
class ComponentRow:
    retailer_slug: str
    category: str
    variant_sku: str
    product_name: str
    package_size: float
    package_unit: str
    price: float
    in_stock: bool
    source_url: str
    parent_sku: Optional[str] = None
    brand: Optional[str] = None
    manufacturer: Optional[str] = None
    caliber: Optional[str] = None
    grain: Optional[int] = None


# ---------- HTTP ----------

def fetch(url: str) -> str:
    r = requests.get(url, headers={'User-Agent': USER_AGENT}, timeout=30)
    r.raise_for_status()
    return r.text


# ---------- JSON-LD ----------

def find_product_jsonld(soup: BeautifulSoup) -> Optional[dict]:
    """Return the @type:ProductGroup or @type:Product node from any
    JSON-LD block on the page. Powder Valley nests it inside a @graph
    array on the second of two blocks."""
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            payload = json.loads(script.string or '{}')
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        graph = payload.get('@graph') if isinstance(payload, dict) else None
        items = graph if graph else (payload if isinstance(payload, list) else [payload])
        for item in items:
            if not isinstance(item, dict):
                continue
            t = item.get('@type')
            if t in ('ProductGroup', 'Product'):
                return item
    return None


def _price_from_offers(offers) -> Optional[float]:
    """Pull a price float from a JSON-LD offers blob.

    Powder Valley nests under offers.priceSpecification[].price but tolerate
    direct offers.price too (spec-compliant fallback for plain Offers)."""
    if not offers:
        return None
    if isinstance(offers, list):
        for o in offers:
            v = _price_from_offers(o)
            if v is not None:
                return v
        return None
    if not isinstance(offers, dict):
        return None
    for key in ('price', 'lowPrice'):
        if key in offers:
            try:
                return float(offers[key])
            except (TypeError, ValueError):
                pass
    spec = offers.get('priceSpecification')
    if isinstance(spec, list):
        for s in spec:
            if isinstance(s, dict) and 'price' in s:
                try:
                    return float(s['price'])
                except (TypeError, ValueError):
                    continue
    elif isinstance(spec, dict) and 'price' in spec:
        try:
            return float(spec['price'])
        except (TypeError, ValueError):
            pass
    return None


_VARIANT_SUFFIX_RE = re.compile(r'\s-\s(\S+)\s*$')
_VARIANT_URL_RE = re.compile(r'attribute_pa_(?:size|quantity|count|weight)=([^&#]+)')
_OZ_SLUG_RE = re.compile(r'^(\d+(?:\.\d+)?)-?oz$', re.IGNORECASE)


def _parse_size_slug(raw, category: str) -> Optional[float]:
    """pa_size/pa_quantity slug to numeric package_size in the canonical
    unit for the category. Powder also accepts '<N>-oz' slugs and converts
    to lbs (Trail Boss / Clays family / Titewad / Nitro 100 / Ramshot
    Competition ship in oz containers, not pound jugs); all other slugs
    must be bare numerics."""
    if raw is None:
        return None
    s = str(raw).strip().lower()
    if not s:
        return None
    if category == 'powder':
        m = _OZ_SLUG_RE.match(s)
        if m:
            return round(float(m.group(1)) / 16.0, 4)
    try:
        return float(s)
    except ValueError:
        return None


def _variant_size(variant: dict, category: str) -> Optional[float]:
    """Pack count for one variant. Powder uses JSON-LD `size`; bullet/brass
    have no size field so fall back to the URL query parameter, then to the
    name suffix (" - 100", " - 1000")."""
    v = _parse_size_slug(variant.get('size'), category)
    if v is not None:
        return v
    url = variant.get('url') or ''
    m = _VARIANT_URL_RE.search(url)
    if m:
        v = _parse_size_slug(m.group(1), category)
        if v is not None:
            return v
    name = variant.get('name') or ''
    m = _VARIANT_SUFFIX_RE.search(name)
    if m:
        v = _parse_size_slug(m.group(1), category)
        if v is not None:
            return v
    return None


# ---------- HTML ----------

def info_table_value(soup: BeautifulSoup, label: str) -> Optional[str]:
    """Read a value from the WooCommerce additional-info table by row label.

    Tries the canonical wc table classes first, then any 2-cell tr as a
    theme-portability fallback."""
    target = label.lower()
    for tr in soup.select('table.woocommerce-product-attributes tr, table.shop_attributes tr'):
        cells = tr.find_all(['th', 'td'])
        if len(cells) >= 2 and cells[0].get_text(strip=True).lower() == target:
            return cells[1].get_text(' ', strip=True)
    for tr in soup.find_all('tr'):
        cells = tr.find_all(['th', 'td'])
        if len(cells) == 2 and cells[0].get_text(strip=True).lower() == target:
            return cells[1].get_text(' ', strip=True)
    return None


def parse_page_stock(soup: BeautifulSoup) -> bool:
    """Page-level in-stock from .stock CSS class (text capitalization is
    inconsistent across themes)."""
    el = soup.select_one('.stock')
    if not el:
        # No stock element at all — treat as out of stock to avoid lying.
        return False
    classes = ' '.join(el.get('class', []))
    if 'out-of-stock' in classes:
        return False
    if 'in-stock' in classes or 'available-on-backorder' in classes:
        return True
    text = el.get_text(strip=True).lower()
    return 'in stock' in text


def parse_variant_stock_map(html: str) -> dict[str, bool]:
    """Per-variant in-stock from the WC variations form's data-product_variations
    JSON. Returns {sku: in_stock}. Empty dict if the form is missing."""
    m = re.search(r'data-product_variations="([^"]+)"', html)
    if not m:
        return {}
    raw = htmllib.unescape(m.group(1))
    try:
        variations = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    out: dict[str, bool] = {}
    for v in variations or []:
        sku = v.get('sku')
        if not sku:
            continue
        out[str(sku)] = bool(v.get('is_in_stock'))
    return out


_TITLE_QTY_PATTERNS = [
    re.compile(r'box of (\d+)', re.IGNORECASE),
    re.compile(r'pack of (\d+)', re.IGNORECASE),
    re.compile(r'\((\d+)\)\s*$'),
    re.compile(r'(\d+)\s*count\b', re.IGNORECASE),
    re.compile(r'(\d+)\s*ct\b', re.IGNORECASE),
]


def parse_qty_from_title(title: str) -> Optional[int]:
    for pat in _TITLE_QTY_PATTERNS:
        m = pat.search(title)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                continue
    return None


_GRAIN_RE = re.compile(r'(\d+)\s*Grain', re.IGNORECASE)


def parse_grain_from_title(title: str) -> Optional[int]:
    m = _GRAIN_RE.search(title)
    return int(m.group(1)) if m else None


# ---------- Product page → rows ----------

def parse_product_page(html: str, url: str, category: str) -> list[ComponentRow]:
    soup = BeautifulSoup(html, 'html.parser')
    product = find_product_jsonld(soup)
    if not product:
        raise ValueError('no Product/ProductGroup JSON-LD on page')

    name = (product.get('name') or '').strip()
    parent_sku = product.get('sku') or product.get('productGroupID')
    brand_obj = product.get('brand')
    brand = brand_obj.get('name') if isinstance(brand_obj, dict) else None
    mfr_obj = product.get('manufacturer')
    manufacturer = mfr_obj.get('name') if isinstance(mfr_obj, dict) else None

    page_in_stock = parse_page_stock(soup)
    variant_stock = parse_variant_stock_map(html)

    caliber = info_table_value(soup, 'Caliber') if category in ('bullet', 'brass') else None
    grain = parse_grain_from_title(name) if category == 'bullet' else None

    unit = CATEGORY_UNIT[category]
    rows: list[ComponentRow] = []

    variants = product.get('hasVariant')
    if isinstance(variants, list) and variants:
        for v in variants:
            v_sku = v.get('sku') or v.get('productID')
            if not v_sku:
                continue
            size = _variant_size(v, category)
            if size is None:
                # Skip; we won't write a row we can't price-per-unit.
                continue
            price = _price_from_offers(v.get('offers'))
            if price is None:
                continue
            v_sku_str = str(v_sku)
            # Per-variant stock if available; otherwise fall back to page-level.
            in_stock = variant_stock.get(v_sku_str, page_in_stock)
            rows.append(ComponentRow(
                retailer_slug=RETAILER_SLUG,
                category=category,
                parent_sku=str(parent_sku) if parent_sku else None,
                variant_sku=v_sku_str,
                product_name=name,
                brand=brand,
                manufacturer=manufacturer,
                package_size=size,
                package_unit=unit,
                price=price,
                in_stock=in_stock,
                caliber=caliber,
                grain=grain,
                source_url=url,
            ))
        return rows

    # Plain Product (single SKU).
    if not parent_sku:
        raise ValueError('single-SKU product missing sku')

    qty_label = 'Size' if category == 'powder' else 'Quantity'
    qty_raw = info_table_value(soup, qty_label)
    size: Optional[float] = None
    if qty_raw:
        m = re.search(r'(\d+(?:\.\d+)?)', qty_raw)
        if m:
            size = float(m.group(1))
    if size is None:
        # Title fallback: "Box of 1000", "(1000)", "50 Count"
        q = parse_qty_from_title(name)
        if q is not None:
            size = float(q)
    if size is None:
        raise ValueError(f'could not determine package_size (info {qty_label!r}={qty_raw!r}, title={name!r})')

    price = _price_from_offers(product.get('offers'))
    if price is None:
        raise ValueError('could not determine price')

    rows.append(ComponentRow(
        retailer_slug=RETAILER_SLUG,
        category=category,
        parent_sku=str(parent_sku),
        variant_sku=str(parent_sku),
        product_name=name,
        brand=brand,
        manufacturer=manufacturer,
        package_size=size,
        package_unit=unit,
        price=price,
        in_stock=page_in_stock,
        caliber=caliber,
        grain=grain,
        source_url=url,
    ))
    return rows


# ---------- Discovery ----------

_PRODUCT_HREF_RE = re.compile(r'href="(https?://www\.powdervalley\.com/product/[^"?#]+/)"')


def discover_product_urls(
    category: str,
    limit_pages: Optional[int] = None,
) -> tuple[list[str], Optional[str]]:
    """Walk /page/N/ pagination. Returns (urls, discovery_error).

    discovery_error is None on clean completion — that includes 404 (legitimate
    end of pagination) and "no new URLs" (the last page wrapped cleanly). It is
    a non-empty string when a discovery-time fetch failed in a way the caller
    should treat as a job failure: a non-404 HTTP status (Cloudflare block,
    server error) or a network exception. Per-product fetch errors are tracked
    separately by the caller and don't surface here.
    """
    base = SITE_BASE + CATEGORY_PATHS[category]
    found: list[str] = []
    seen: set[str] = set()
    page_num = 1
    discovery_error: Optional[str] = None
    while True:
        if limit_pages is not None and page_num > limit_pages:
            print(f'  [discover {category}] limit-pages reached ({limit_pages})')
            break
        if page_num > PAGE_HARD_CAP:
            print(f'  [discover {category}] hard cap reached ({PAGE_HARD_CAP})')
            break
        url = base if page_num == 1 else f'{base}page/{page_num}/'
        print(f'  [discover {category}] page {page_num}: {url}')
        try:
            html = fetch(url)
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status == 404:
                print(f'    404 — end of pagination')
                break
            discovery_error = f'HTTP {status} on page {page_num} ({url})'
            print(f'    {discovery_error} — stopping')
            break
        except Exception as e:
            discovery_error = f'fetch error on page {page_num} ({url}): {e}'
            print(f'    {discovery_error} — stopping')
            break
        finally:
            time.sleep(CRAWL_DELAY_SEC)

        new_urls = []
        for m in _PRODUCT_HREF_RE.findall(html):
            if 'gift-card' in m:
                continue
            if m in seen:
                continue
            seen.add(m)
            new_urls.append(m)
        if not new_urls:
            print(f'    no new product URLs — stopping')
            break
        print(f'    +{len(new_urls)} (running {len(found) + len(new_urls)})')
        found.extend(new_urls)
        page_num += 1
    return found, discovery_error


# ---------- DB ----------

def upsert_row(supabase, row: ComponentRow) -> None:
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        'retailer_slug': row.retailer_slug,
        'category': row.category,
        'parent_sku': row.parent_sku,
        'variant_sku': row.variant_sku,
        'product_name': row.product_name,
        'brand': row.brand,
        'manufacturer': row.manufacturer,
        'package_size': row.package_size,
        'package_unit': row.package_unit,
        'price': row.price,
        'in_stock': row.in_stock,
        'caliber': row.caliber,
        'grain': row.grain,
        'source_url': row.source_url,
        'last_seen_at': now,
        'last_updated': now,
    }
    if row.in_stock:
        payload['last_seen_in_stock'] = now
    supabase.table('components').upsert(
        payload,
        on_conflict='retailer_slug,variant_sku',
    ).execute()


# ---------- Entrypoint ----------

def main() -> int:
    ap = argparse.ArgumentParser(description='Scrape Powder Valley reloading components.')
    ap.add_argument('--dry-run', action='store_true',
                    help='Parse and print only; no DB writes.')
    ap.add_argument('--category', choices=list(CATEGORY_PATHS.keys()) + ['all'], default='all')
    ap.add_argument('--limit-pages', type=int, default=None,
                    help='Cap discovery pages per category (dev/test).')
    ap.add_argument('--limit-products', type=int, default=None,
                    help='Cap product fetches per category (dev/test).')
    args = ap.parse_args()

    supabase = None
    if not args.dry_run:
        supabase = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])

    categories = list(CATEGORY_PATHS.keys()) if args.category == 'all' else [args.category]

    totals = {c: {'products': 0, 'variants': 0, 'in_stock_variants': 0, 'errors': 0,
                  'discovered': 0, 'discovery_status': 'OK'}
              for c in categories}
    errors: list[str] = []
    discovery_failures: list[str] = []
    # Test-mode flags bypass the discovery floor — they intentionally truncate.
    enforce_floor = args.limit_pages is None and args.limit_products is None

    for cat in categories:
        print(f'\n=== {cat.upper()} ===')
        urls, discovery_error = discover_product_urls(cat, limit_pages=args.limit_pages)
        print(f'  discovered {len(urls)} product URL(s)')
        totals[cat]['discovered'] = len(urls)
        if discovery_error:
            totals[cat]['discovery_status'] = f'ERROR: {discovery_error}'
            discovery_failures.append(f'[{cat}] {discovery_error}')
        elif enforce_floor:
            floor = CATEGORY_MIN_DISCOVERED.get(cat, 0)
            if len(urls) < floor:
                shortfall = (
                    f'discovered {len(urls)} URL(s); minimum expected {floor} '
                    f'(silent block, pagination break, or category drift?)'
                )
                totals[cat]['discovery_status'] = f'BELOW FLOOR: {shortfall}'
                discovery_failures.append(f'[{cat}] {shortfall}')
                print(f'  [DISCOVERY-FLOOR] {shortfall}')
        if args.limit_products is not None:
            urls = urls[:args.limit_products]
            print(f'  capped to {len(urls)} for this run')

        for i, url in enumerate(urls, 1):
            slug = url.rstrip('/').rsplit('/', 1)[-1]
            try:
                html = fetch(url)
            except Exception as e:
                msg = f'[{cat}] {slug}: fetch failed: {e}'
                print(f'  [FETCH-ERR] {msg}')
                errors.append(msg)
                totals[cat]['errors'] += 1
                time.sleep(CRAWL_DELAY_SEC)
                continue

            try:
                rows = parse_product_page(html, url, cat)
            except Exception as e:
                msg = f'[{cat}] {slug}: parse failed: {e}'
                print(f'  [PARSE-ERR] {msg}')
                errors.append(msg)
                totals[cat]['errors'] += 1
                time.sleep(CRAWL_DELAY_SEC)
                continue

            stocked = sum(1 for r in rows if r.in_stock)
            stock_str = f'{stocked}/{len(rows)} stocked'
            print(f'  [{i}/{len(urls)}] {slug} — {len(rows)} variant(s), {stock_str}')

            for r in rows:
                if args.dry_run:
                    print(f'      sku={r.variant_sku:<14s} {r.package_size:>6g} {r.package_unit:<6s} '
                          f'${r.price:>7.2f}  {"IN " if r.in_stock else "OUT"}  '
                          f'brand={r.brand!r}')
                else:
                    try:
                        upsert_row(supabase, r)
                    except Exception as e:
                        msg = f'[{cat}] {r.variant_sku}: upsert failed: {e}'
                        print(f'      [DB-ERR] {msg}')
                        errors.append(msg)
                        totals[cat]['errors'] += 1

            totals[cat]['products'] += 1
            totals[cat]['variants'] += len(rows)
            totals[cat]['in_stock_variants'] += stocked
            time.sleep(CRAWL_DELAY_SEC)

    mode = 'DRY RUN' if args.dry_run else 'LIVE'
    print(f'\n=== TOTALS ({mode}) ===')
    for cat, t in totals.items():
        print(f'  {cat:7s}  discovered={t["discovered"]:4d}  products={t["products"]:4d}  '
              f'variants={t["variants"]:4d}  stocked={t["in_stock_variants"]:4d}  '
              f'errors={t["errors"]}  discovery={t["discovery_status"]}')
    if discovery_failures:
        print(f'\n=== {len(discovery_failures)} DISCOVERY FAILURE(S) ===')
        for f in discovery_failures:
            print(f'  {f}')
    if errors:
        print(f'\n=== {len(errors)} ERROR(S) ===')
        for e in errors[:30]:
            print(f'  {e}')
        if len(errors) > 30:
            print(f'  ... and {len(errors) - 30} more')

    return 0 if (not errors and not discovery_failures) else 1


if __name__ == '__main__':
    sys.exit(main())
