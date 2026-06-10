"""scraper_preppergunshop.py — Prepper Gun Shop (preppergunshop.com) scraper.

Magento 2 storefront (Roughrider Arms LLC). Plain `requests` returns full
SSR'd HTML; no Cloudflare challenge observed from residential OR GitHub
Actions IPs as of the 2026-06-06 recon (re-verify the Actions side before
wiring into cron — the Wideners failure mode).

Discovery strategy: crawl the /ammunition/ anchor category, which
aggregates all four ammo branches (handgun-ammunition 630 + rifle-
ammunition 640 + rimfire 28 + shotgun 301 ≈ root 1,628 as of recon).
This is the FULL ammo catalog — paginate ?p=N to the end (no rotating
window; the stale-tail lesson). 60 cards/page, ~28 pages; the page after
the last returns zero cards, which is the stop signal.

Two recon traps the crawler must respect:
  - Card titles arrive HTML-escaped (".40 S&amp;W"). html.unescape()
    BEFORE normalize_caliber, or the entire .40 S&W bucket vanishes
    (36 products went untracked in the first recon pass).
  - The default category sort drifts while you crawl (products shift
    pages between fetches; ~11 of 1,628 were silently missed). Pin the
    sort with ?product_list_order=name&product_list_dir=asc — verified
    stable across consecutive fetches.

Per-PDP data comes from the Product JSON-LD block via the shared reader
in scraper_lib (extract_product_offer / availability_to_in_stock — same
code path scraper_recheck.py trusts):
  - offers.price (numeric)
  - offers.availability ("http://schema.org/InStock" | "OutOfStock")
  - JSON-LD is present on BOTH in-stock and OOS PDPs (verified), so OOS
    listings are tracked honestly rather than dropped.
  - No mpn/sku/brand in the JSON-LD — brand parses from the title/slug.

retailer_product_id = URL slug (last path segment), consistent with
ammoman/bulkammo. Titles carry the sale unit LAST when both a box and a
case count appear ("50rd Box 2000rd Case" sells the 2000rd case at
$163.79 — verified), so parse_rounds checks case/pack counts before box
counts, mirroring scraper_bulkammo. sanity_check_ppr gates the rest.

robots.txt: no Crawl-delay; /ammunition/ is allowed (the disallowed
/catalog/ and /catalogsearch/ paths are not used here).

Registration: retailers row slug='preppergunshop' (see
scripts/seed_preppergunshop.py) — the scraper resolves retailer_id from
the slug at runtime like scraper_ammoman, so no hardcoded id.
"""

import argparse
import html as htmllib
import os
import re
import sys
import time
from datetime import datetime
from typing import Optional

import requests

from scraper_lib import (
    insert_price_history,
    normalize_caliber, now_iso, with_stock_fields,
    parse_purchase_limit, parse_brand_with_url, sanity_check_ppr,
    clean_title, parse_bullet_type_with_url_fallback,
    extract_product_offer, availability_to_in_stock,
    mark_retailer_scraped,
)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dry-run needs no env; live mode will fail loudly on missing vars

RETAILER_SLUG = "preppergunshop"
SITE_BASE = "https://www.preppergunshop.com"
CATEGORY_PATH = "/ammunition/"
# Pinned sort — the live default re-orders mid-crawl and drops products.
SORT_PARAMS = "product_list_order=name&product_list_dir=asc"
MAX_PAGES = 60  # ~28 pages live; headroom for catalog growth, stops runaways

CRAWL_DELAY_SEC = 2.0   # between PDP fetches (robots has no Crawl-delay)
PAGE_DELAY_SEC = 1.0    # between category pages
USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
)
REQUEST_TIMEOUT = 30

_CARD_RE = re.compile(
    r'class="product-item-link"\s+href="([^"]+)"\s*>([^<]+)</a>')


# ---------- HTTP ----------

def fetch(url: str) -> str:
    r = requests.get(
        url,
        headers={
            'User-Agent': USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        },
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    return r.text


# ---------- Discovery (category crawl, paginate to the end) ----------

def discover_products(page_delay: float = PAGE_DELAY_SEC) -> list[tuple[str, str]]:
    """Crawl /ammunition/ to the last page. Returns [(url, title)] for the
    ENTIRE ammo catalog (caliber filtering happens in the caller).

    Stops when a page yields zero cards or zero new slugs. Page 1 coming
    back empty is storefront drift / a wall — caller must treat that as
    a hard failure, not an empty catalog.
    """
    seen: dict[str, tuple[str, str]] = {}
    page = 1
    while page <= MAX_PAGES:
        url = f'{SITE_BASE}{CATEGORY_PATH}?{SORT_PARAMS}&p={page}'
        html = fetch(url)
        cards = _CARD_RE.findall(html)
        new = 0
        for href, raw_title in cards:
            slug = href.rstrip('/').rsplit('/', 1)[-1]
            if slug in seen:
                continue
            title = clean_title(htmllib.unescape(raw_title))
            seen[slug] = (href, title)
            new += 1
        print(f'[category] p={page}: {len(cards)} cards, {new} new '
              f'({len(seen)} total)')
        if not cards or new == 0:
            break
        page += 1
        time.sleep(page_delay)
    return [(href, title) for href, title in seen.values()]


# ---------- Per-product parsing ----------

def parse_rounds(title: str) -> Optional[int]:
    """Round count from a PGS title. Case/pack counts outrank box counts
    because dual-count titles ("50rd Box 2000rd Case") sell the LAST,
    larger unit — verified against live pricing during recon."""
    t = title or ''
    case = re.search(r'([\d,]+)\s*(?:rd|rds|rounds?)[\s-]*(?:case|bulk\s*pack|pack\b)',
                     t, re.IGNORECASE)
    if case:
        return int(case.group(1).replace(',', ''))
    multi = re.search(r'(\d+)\s*(?:rd|rds|rounds?)\s*(?:box|loose)?\s*x\s*(\d+)',
                      t, re.IGNORECASE)
    if multi:
        return int(multi.group(1)) * int(multi.group(2))
    box = re.search(r'([\d,]+)\s*(?:rd|rds|rounds?)[\s-]*box', t, re.IGNORECASE)
    if box:
        return int(box.group(1).replace(',', ''))
    # "20/bx", "50/BX", "20/box" — slash-count format on a handful of
    # PGS titles (Aguila/Tula/RWS). The bx|box suffix is required, so
    # shotshell fractions like "2-3/4" can't false-match.
    slash_box = re.search(r'(\d+)\s*/\s*(?:bx|box)\b', t, re.IGNORECASE)
    if slash_box:
        return int(slash_box.group(1))
    rounds = re.search(r'([\d,]+)\s*rounds?\b', t, re.IGNORECASE)
    if rounds:
        return int(rounds.group(1).replace(',', ''))
    rd = re.search(r'([\d,]+)\s*rds?\b', t, re.IGNORECASE)
    if rd:
        return int(rd.group(1).replace(',', ''))
    return None


def parse_grain(title: str) -> Optional[int]:
    m = re.search(r'(\d+)\s*(?:grain|gr)\b', title, re.IGNORECASE)
    return int(m.group(1)) if m else None


def parse_case_material(title: str) -> str:
    t = title.lower()
    steel_brands = ('wolf', 'tula', 'tulammo', 'brown bear', 'silver bear',
                    'golden bear', 'barnaul', 'red army', 'sterling')
    if any(b in t for b in steel_brands) or 'steel' in t:
        return 'Steel'
    if 'aluminum' in t or 'aluminium' in t:
        return 'Aluminum'
    if 'nickel' in t:
        return 'Nickel'
    if 'polymer' in t:
        return 'Polymer'
    return 'Brass'


def parse_condition(title: str) -> str:
    if 'reman' in title.lower():
        return 'Remanufactured'
    return 'New'


def parse_pdp(url: str, title: str, html: str) -> tuple[Optional[dict], str]:
    """Parse one PDP into (listing-precursor dict, reason).

    Returns (None, reason) when the product must be HELD — i.e. we cannot
    honestly store it: no JSON-LD offer, ambiguous availability, no
    price, no round count, or implausible price-per-round.
    """
    offer = extract_product_offer(html)
    if not isinstance(offer, dict):
        return None, 'no-jsonld-offer'

    in_stock = availability_to_in_stock(offer.get('availability'))
    if in_stock is None:
        return None, f'ambiguous-availability:{offer.get("availability")}'

    try:
        base_price = float(str(offer.get('price')).replace(',', '').replace('$', ''))
    except (TypeError, ValueError):
        return None, 'no-price'
    if base_price <= 0:
        return None, 'zero-price'

    total_rounds = parse_rounds(title)
    if not total_rounds or total_rounds < 1:
        return None, 'no-round-count'

    ppr = round(base_price / total_rounds, 4)

    return {
        'url': url,
        'title': title,
        'in_stock': in_stock,
        'base_price': round(base_price, 2),
        'price_per_round': ppr,
        'total_rounds': total_rounds,
        'raw_offer': offer,  # popped before DB write; kept for spot-checks
    }, 'ok'


def build_listing(row: dict, retailer_id: int, html: str) -> dict:
    """Assemble the listings-table dict from a parsed PDP row."""
    title, url = row['title'], row['url']
    cal_disp, cal_norm = normalize_caliber(title)
    listing = {
        'retailer_id': retailer_id,
        'retailer_product_id': url.rstrip('/').rsplit('/', 1)[-1],
        'product_url': url,
        'caliber': cal_disp,
        'caliber_normalized': cal_norm,
        'grain': parse_grain(title),
        'bullet_type': parse_bullet_type_with_url_fallback(title, url),
        'case_material': parse_case_material(title),
        'condition_type': parse_condition(title),
        'manufacturer': (parse_brand_with_url(title, url) or 'Unknown'),
        'rounds_per_box': row['total_rounds'],
        'boxes_per_case': 1,
        'total_rounds': row['total_rounds'],
        'base_price': row['base_price'],
        'price_per_round': row['price_per_round'],
        'purchase_limit': parse_purchase_limit(html),
        'last_updated': now_iso(),
    }
    with_stock_fields(listing, row['in_stock'])
    return listing


# ---------- Entrypoint ----------

def main() -> int:
    ap = argparse.ArgumentParser(description='Scrape Prepper Gun Shop ammunition listings.')
    ap.add_argument('--dry-run', action='store_true',
                    help='Parse and print only; no DB connection, no writes.')
    ap.add_argument('--limit-products', type=int, default=None,
                    help='Cap tracked-caliber PDPs processed (dev/test).')
    ap.add_argument('--crawl-delay', type=float, default=CRAWL_DELAY_SEC,
                    help=f'Seconds between PDP fetches (default {CRAWL_DELAY_SEC}).')
    args = ap.parse_args()

    supabase = None
    retailer_id = 0
    if not args.dry_run:
        from supabase import create_client
        supabase = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])
        rr = supabase.table('retailers').select('id').eq('slug', RETAILER_SLUG).execute()
        if not rr.data:
            print(f"ERROR: Retailer '{RETAILER_SLUG}' not found in retailers table")
            return 1
        retailer_id = rr.data[0]['id']

    print(f'[{datetime.now().isoformat()}] Prepper Gun Shop scraper starting '
          f'(mode={"DRY RUN" if args.dry_run else "LIVE"}, '
          f'crawl_delay={args.crawl_delay}s, retailer_id={retailer_id})')

    try:
        catalog = discover_products()
    except Exception as e:
        print(f'[FETCH-ERR] category crawl failed: {e}')
        return 1

    # Storefront-drift / wall guardrail: an empty page 1 means the
    # category moved or we are blocked — never "the catalog is empty".
    if not catalog:
        print('FAIL: /ammunition/ returned zero products on page 1 — '
              'storefront drift or anti-bot wall. No writes.')
        return 1

    tracked = []
    for url, title in catalog:
        _, cal_norm = normalize_caliber(title)
        if cal_norm:
            tracked.append((url, title, cal_norm))
    print(f'[discovery] {len(catalog)} products in catalog, '
          f'{len(tracked)} in tracked calibers')

    if args.limit_products is not None:
        tracked = tracked[:args.limit_products]
        print(f'[discovery] capped to {len(tracked)} for this run')

    saved_total = 0
    clean_total = 0      # all of: price, stock, caliber, grain, bullet, rounds
    partial_total = 0    # ingestable but grain or bullet_type is NULL
    held: dict[str, int] = {}
    errors: list[str] = []
    seen_pids: set[str] = set()
    per_caliber: dict[str, int] = {}

    for i, (url, title, cal_norm) in enumerate(tracked, 1):
        if i > 1:
            time.sleep(args.crawl_delay)

        try:
            html = fetch(url)
        except Exception as e:
            msg = f'[{cal_norm}] fetch failed for {url}: {e}'
            print(f'  [FETCH-ERR] {msg}')
            errors.append(msg)
            continue

        try:
            row, reason = parse_pdp(url, title, html)
        except Exception as e:
            msg = f'[{cal_norm}] parse failed for {url}: {e}'
            print(f'  [PARSE-ERR] {msg}')
            errors.append(msg)
            continue

        if row is None:
            held[reason] = held.get(reason, 0) + 1
            print(f'  [HELD:{reason}] {title[:70]}')
            continue

        if not sanity_check_ppr(
            row['price_per_round'], row['base_price'], row['total_rounds'],
            context=f'{RETAILER_SLUG} {title[:50]}', caliber=cal_norm,
        ):
            held['sanity-fail'] = held.get('sanity-fail', 0) + 1
            continue

        pid = url.rstrip('/').rsplit('/', 1)[-1]
        if pid in seen_pids:
            continue
        seen_pids.add(pid)

        row.pop('raw_offer', None)
        listing = build_listing(row, retailer_id, html)

        if listing['grain'] is not None and listing['bullet_type'] is not None:
            clean_total += 1
        else:
            partial_total += 1
        per_caliber[cal_norm] = per_caliber.get(cal_norm, 0) + 1

        if args.dry_run:
            print(
                f"  [DRY {i:>4}/{len(tracked)}] cal={cal_norm:<8} "
                f"${row['base_price']:>8.2f} ({row['price_per_round']}/rd) "
                f"{'IN ' if row['in_stock'] else 'OUT'} "
                f"{listing['total_rounds']:>5}rd gr={listing['grain'] or '-':<4} "
                f"bt={listing['bullet_type'] or '-':<5} "
                f"mfr={listing['manufacturer'][:12]:<12} {title[:48]}"
            )
            saved_total += 1
        else:
            try:
                result = supabase.table('listings').upsert(
                    listing, on_conflict='retailer_id,retailer_product_id',
                ).execute()
                insert_price_history(supabase, {
                    'listing_id': result.data[0]['id'],
                    'price': row['base_price'],
                    'price_per_round': row['price_per_round'],
                    'in_stock': row['in_stock'],
                })
                saved_total += 1
                print(
                    f"  [{i:>4}/{len(tracked)}] cal={cal_norm:<8} "
                    f"${row['base_price']:>8.2f} ({row['price_per_round']}/rd) "
                    f"{'IN ' if row['in_stock'] else 'OUT'} {title[:55]}"
                )
            except Exception as e:
                msg = f'[{cal_norm}] {pid}: upsert failed: {e}'
                print(f'  [DB-ERR] {msg}')
                errors.append(msg)

    if not args.dry_run:
        mark_retailer_scraped(supabase, retailer_id, had_success=saved_total > 0)

    mode = 'DRY RUN' if args.dry_run else 'LIVE'
    held_total = sum(held.values())
    print(f'\n=== TOTALS ({mode}) ===')
    print(f'  catalog={len(catalog)}  tracked={len(tracked)}  '
          f'saved={saved_total} (clean={clean_total} partial={partial_total})  '
          f'held={held_total}  errors={len(errors)}')
    if held:
        print('  held breakdown:')
        for reason, n in sorted(held.items(), key=lambda kv: -kv[1]):
            print(f'    {reason}: {n}')
    if per_caliber:
        print('  per caliber:')
        for cal, n in sorted(per_caliber.items(), key=lambda kv: -kv[1]):
            print(f'    {cal}: {n}')
    if errors:
        print(f'\n=== {len(errors)} ERROR(S) ===')
        for e in errors[:30]:
            print(f'  {e}')
        if len(errors) > 30:
            print(f'  ... and {len(errors) - 30} more')

    return 0 if not errors else 1


if __name__ == '__main__':
    sys.exit(main())
