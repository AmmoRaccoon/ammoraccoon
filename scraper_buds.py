"""scraper_buds.py — Bud's Gun Shop ammunition scraper.

Custom PHP / osCommerce-derived storefront fronted by Cloudflare in
CDN/cache mode (no JS challenge). Plain `requests` returns SSR'd HTML
with rich schema.org Microdata on every product card — name, url,
price, gtin, availability all exposed inline. No PDP fetches needed.

Discovery walks the all-ammo search view:
  /search.php/type/ammo                   (page 1)
  /search.php/type/ammo/page/2 ... /page/N

Stops when a page returns 0 product cards (page 29 in current data —
27 full pages × 36 cards + 1 partial page = 1000-result hard ceiling).

Three traps the parser must NOT fall into (full recon in
scripts/buds_unit_mapping.md, web repo):

  - The site-wide search caps at 1000 results; v1 lives with the cap.
    v2 candidate is to walk per-caliber filter views and union.
  - Bud's exposed caliber filter list only covers 9 calibers (missing
    380acp, 762x39, 300blk, 357mag from our 10). We walk the all-ammo
    view and filter via normalize_caliber(title) so the missing 4 are
    still picked up when they show up in the unfiltered listing.
  - Page 1 is /search.php/type/ammo (no /page/1 suffix). /page/1 returns
    a 301 redirect to the root.

Robots.txt has no Crawl-delay; v1 uses 5s between page fetches —
matches the AmmoMan posture for unconstrained sites.
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
from dotenv import load_dotenv
from supabase import create_client

from scraper_lib import (
    normalize_caliber, now_iso, with_stock_fields,
    parse_purchase_limit, parse_brand, sanity_check_ppr, clean_title,
    parse_bullet_type,
)

load_dotenv()

RETAILER_SLUG = "buds"
SITE_BASE = "https://www.budsgunshop.com"
LISTING_BASE = f"{SITE_BASE}/search.php/type/ammo"

CRAWL_DELAY_SEC = 5
PAGE_HARD_CAP = 50
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


# ---------- Per-card parsing ----------

# Card opens with a div carrying both itemtype Product and data-pid.
# The lookahead-split below uses this exact prefix to bound each card.
_CARD_OPEN_RE = re.compile(
    r'<div\s+itemscope\s+itemtype=["\']https?://schema\.org/Product["\']\s+data-pid=["\'](\d+)["\']',
    re.IGNORECASE,
)
_NAME_RE = re.compile(
    r'<span\s+itemprop=["\']name["\'][^>]*>(.+?)</span>', re.DOTALL | re.IGNORECASE,
)
_URL_RE = re.compile(
    r'<a\s+itemprop=["\']url["\'][^>]*href=["\']([^"\']+)["\']', re.IGNORECASE,
)
_PRICE_RE = re.compile(
    r'<meta\s+itemprop=["\']price["\']\s+content=["\']([\d.]+)["\']', re.IGNORECASE,
)
_AVAIL_RE = re.compile(
    r'itemprop=["\']availability["\'][^>]*href=["\']([^"\']+)["\']', re.IGNORECASE,
)
_PPR_RE = re.compile(
    r'title=["\']\$?([\d.]+)\s*Price\s*Per\s*Round["\']', re.IGNORECASE,
)
_GTIN_RE = re.compile(
    r'<meta\s+itemprop=["\']gtin["\']\s+content=["\']([^"\']+)["\']', re.IGNORECASE,
)


def parse_grain(text: str) -> Optional[int]:
    m = re.search(r'(\d+)\s*gr(?:ain)?\b', text, re.IGNORECASE)
    return int(m.group(1)) if m else None


def parse_rounds(text: str) -> Optional[int]:
    """Bud's titles usually carry the round count: '50rd Box', '100 Round Box',
    '20rd box', '1000 round'. Order patterns from most-specific to least so a
    title with both '20rd box' and a stray digit elsewhere matches the box
    count, not the digit."""
    for pat in (
        r'(\d[\d,]*)\s*[- ]?\s*rd\s*box\b',
        r'(\d[\d,]*)\s*[- ]?\s*round\s*box\b',
        r'(\d[\d,]*)\s*[- ]?\s*rounds?\s*per\s*box\b',
        r'(\d[\d,]*)\s*[- ]?\s*rd\b',
        r'(\d[\d,]*)\s*[- ]?\s*rounds?\b',
    ):
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                return int(m.group(1).replace(',', ''))
            except ValueError:
                continue
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


def parse_card(card_html: str, pid: str) -> Optional[dict]:
    """Return a parsed listing dict for one Bud's card, or None to skip."""
    name_m = _NAME_RE.search(card_html)
    url_m = _URL_RE.search(card_html)
    price_m = _PRICE_RE.search(card_html)
    if not (name_m and url_m and price_m):
        return None

    title = clean_title(htmllib.unescape(re.sub(r'\s+', ' ', name_m.group(1)).strip()))
    if not title:
        return None

    try:
        base_price = float(price_m.group(1))
    except ValueError:
        return None
    if base_price <= 0:
        return None

    avail_m = _AVAIL_RE.search(card_html)
    avail = (avail_m.group(1) if avail_m else '').lower()
    in_stock = 'instock' in avail.replace(' ', '').replace('/', '')

    # Total-rounds resolution: prefer the title-parsed count when present
    # because the displayed PPR is rounded to cents on the card. A 100-round
    # box at $10.49 displays "$0.10/rd"; back-derived rounds = 10.49/0.10
    # = 104.9 → 105 (off by 5%). Only fall back to PPR back-derivation
    # when the title omits the count.
    ppr_m = _PPR_RE.search(card_html)
    title_rounds = parse_rounds(title)
    if title_rounds and title_rounds > 0:
        total_rounds = title_rounds
        price_per_round = round(base_price / total_rounds, 4)
    elif ppr_m:
        try:
            display_ppr = float(ppr_m.group(1))
        except ValueError:
            return None
        if display_ppr <= 0:
            return None
        total_rounds = max(1, round(base_price / display_ppr))
        # Recompute PPR from the integer rounds for precision.
        price_per_round = round(base_price / total_rounds, 4)
    else:
        return None

    rel_url = url_m.group(1)
    full_url = rel_url if rel_url.startswith('http') else f'{SITE_BASE}/{rel_url.lstrip("/")}'

    return {
        'pid': pid,
        'url': full_url,
        'title': title,
        'in_stock': in_stock,
        'base_price': base_price,
        'price_per_round': price_per_round,
        'total_rounds': total_rounds,
    }


def chunk_cards(html: str) -> list[tuple[str, str]]:
    """Split a listing page into [(pid, card_html)] tuples.

    Splits on a lookahead at the card wrapper open so each chunk starts
    with one wrapper and ends just before the next — per-card field
    extraction can't bleed into a neighbor's HTML.
    """
    parts = re.split(
        r'(?=<div\s+itemscope\s+itemtype=["\']https?://schema\.org/Product["\']\s+data-pid=)',
        html,
        flags=re.IGNORECASE,
    )
    out: list[tuple[str, str]] = []
    for p in parts[1:]:
        m = _CARD_OPEN_RE.match(p)
        if m:
            out.append((m.group(1), p))
    return out


# ---------- Entrypoint ----------

def page_url(n: int) -> str:
    """Page 1 is the root URL — /page/1 returns a 301 to the root."""
    return LISTING_BASE if n == 1 else f'{LISTING_BASE}/page/{n}'


def main() -> int:
    ap = argparse.ArgumentParser(description="Scrape Bud's Gun Shop ammunition listings.")
    ap.add_argument('--dry-run', action='store_true',
                    help='Parse and print only; no DB writes.')
    ap.add_argument('--limit-products', type=int, default=None,
                    help='Cap total products processed (dev/test).')
    ap.add_argument('--limit-pages', type=int, default=None,
                    help='Cap pages walked (dev/test).')
    ap.add_argument('--crawl-delay', type=float, default=CRAWL_DELAY_SEC,
                    help=f'Seconds between page fetches (default {CRAWL_DELAY_SEC}).')
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

    print(f'[{datetime.now().isoformat()}] Bud\'s Gun Shop scraper starting '
          f'(mode={"DRY RUN" if args.dry_run else "LIVE"}, '
          f'crawl_delay={args.crawl_delay}s, retailer_id={retailer_id})')

    saved_total = 0
    skipped_total = 0
    errors: list[str] = []
    seen_pids: set[str] = set()

    for page_num in range(1, PAGE_HARD_CAP + 1):
        if args.limit_pages is not None and page_num > args.limit_pages:
            break
        if page_num > 1:
            time.sleep(args.crawl_delay)

        url = page_url(page_num)
        print(f'\n=== PAGE {page_num} === {url}')
        try:
            html = fetch(url)
        except Exception as e:
            msg = f'page {page_num} fetch failed: {e}'
            print(f'  [FETCH-ERR] {msg}')
            errors.append(msg)
            continue

        cards = chunk_cards(html)
        print(f'  {len(cards)} card(s) on page {page_num}')
        if not cards:
            # Past the last page — Bud's returns 200 with the empty
            # search-list shell. Stop walking.
            print('  (no cards — end of paginated set)')
            break

        for pid, card_html in cards:
            if args.limit_products is not None and saved_total >= args.limit_products:
                break
            if pid in seen_pids:
                continue

            try:
                row = parse_card(card_html, pid)
            except Exception as e:
                msg = f'page {page_num} pid {pid}: parse failed: {e}'
                print(f'  [PARSE-ERR] {msg}')
                errors.append(msg)
                skipped_total += 1
                continue
            if not row:
                skipped_total += 1
                continue

            cal_disp, cal_norm = normalize_caliber(row['title'])
            if not cal_norm:
                # Off-list caliber (45 ACP, 12 ga, 5.7x28, etc.) — quiet skip.
                skipped_total += 1
                continue

            if not sanity_check_ppr(
                row['price_per_round'], row['base_price'], row['total_rounds'],
                context=f'{RETAILER_SLUG} {cal_norm}', caliber=cal_norm,
            ):
                skipped_total += 1
                continue

            seen_pids.add(row['pid'])

            grain = parse_grain(row['title'])
            bullet_type = parse_bullet_type(row['title'])
            case_material = parse_case_material(row['title'])
            country = parse_country(row['title'])
            manufacturer = parse_brand(row['title']) or 'Unknown'
            purchase_limit = parse_purchase_limit(card_html)

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
                    f"  [DRY] pid={row['pid']:>7} cal={cal_norm:<8} "
                    f"${row['base_price']:>7.2f} ({row['price_per_round']}/rd) "
                    f"{'IN ' if row['in_stock'] else 'OUT'} {row['title'][:55]}"
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
                        f"  Saved pid={row['pid']:>7} cal={cal_norm:<8} "
                        f"${row['base_price']:>7.2f} ({row['price_per_round']}/rd) "
                        f"{'IN ' if row['in_stock'] else 'OUT'} {row['title'][:55]}"
                    )
                except Exception as e:
                    msg = f'page {page_num} {row["pid"]}: upsert failed: {e}'
                    print(f'  [DB-ERR] {msg}')
                    errors.append(msg)

        if args.limit_products is not None and saved_total >= args.limit_products:
            print(f'  hit --limit-products {args.limit_products}; stopping')
            break

    mode = 'DRY RUN' if args.dry_run else 'LIVE'
    print(f'\n=== TOTALS ({mode}) ===')
    print(f'  saved={saved_total}  skipped={skipped_total}  errors={len(errors)}')
    if errors:
        print(f'\n=== {len(errors)} ERROR(S) ===')
        for e in errors[:30]:
            print(f'  {e}')
        if len(errors) > 30:
            print(f'  ... and {len(errors) - 30} more')

    return 0 if not errors else 1


if __name__ == '__main__':
    sys.exit(main())
