"""scraper_buds.py — Bud's Gun Shop ammunition scraper.

Custom PHP / osCommerce-derived storefront fronted by Cloudflare in
CDN/cache mode (no JS challenge). Plain `requests` returns SSR'd HTML
with rich schema.org Microdata on every product card — name, url,
price, gtin, availability all exposed inline. No PDP fetches needed.

Discovery (v2 — 2026-05-09): walks each of the 6 per-caliber filter
views Bud's exposes plus the all-ammo view, dedupes by data-pid.

The all-ammo view caps at 1000 results and the broader catalog is
heavily skewed toward 12-gauge (≥1000 SKUs in that one off-list
caliber alone), which used to crowd our 10 tracked calibers out of
the cap. Walking the per-caliber filter views first reaches the
~1,200 SKUs Bud's actually carries across the 6 calibers it exposes
filters for; the all-ammo backstop catches the 4 calibers with no
exposed filter (380acp, 762x39, 300blk, 357mag).

Three traps the parser must NOT fall into (full recon in
scripts/buds_unit_mapping.md, web repo):

  - The site-wide search and each caliber filter view both cap at
    1000 results. v2 mitigates by unioning multiple filter views.
  - Bud's exposed caliber filter list only covers 9 calibers (missing
    380acp, 762x39, 300blk, 357mag from our 10). The all-ammo walk
    is retained as a backstop so the missing 4 are still picked up.
  - Page 1 is /search.php/type/ammo (no /page/1 suffix). /page/1 returns
    a 301 redirect to the root. Same special case applies to each
    /caliber/<slug> root.

Robots.txt has no Crawl-delay; v1 used 5s between page fetches —
v2 retains that posture for parity with the AmmoMan scraper.
"""

import argparse
import html as htmllib
import os
import re
import sys
import time
from collections import Counter
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

# Per-caliber filter slugs Bud's exposes in the all-ammo view's facet
# panel (verified live 2026-05-09 by parsing the page-1 facet links).
# Only 6 of our 10 tracked calibers have exposed filter URLs — the
# remaining 4 (380acp, 762x39, 300blk, 357mag) are picked up by the
# all-ammo backstop walk.
#
# Slugs are reproduced exactly as Bud's encodes them in the href —
# `+` for spaces, `%26` for `&`, `%28`/`%29` for parentheses. The
# numeric prefix (e.g. `22903-`, `10000601-`) is Bud's category id;
# omitting or changing it yields a 404. If Bud's reorganizes their
# taxonomy these will break — the loud-failure gate in main() detects
# the case where every root 404s.
CALIBER_FILTER_URLS = {
    '9mm':     f'{LISTING_BASE}/caliber/22903-9mm',
    '223-556': f'{LISTING_BASE}/caliber/10000601-.223+remington+5.56+nato',
    '308win':  f'{LISTING_BASE}/caliber/10000660-308+winchester+%287.62+nato%29',
    '22lr':    f'{LISTING_BASE}/caliber/10000844-22+long+rifle',
    '40sw':    f'{LISTING_BASE}/caliber/10000719-40+smith+%26+wesson',
    '38spl':   f'{LISTING_BASE}/caliber/10000566-38+special',
}

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


# ---------- Walk one listing root (all-ammo OR a per-caliber filter view) ----------

def walk_listing_root(
    *, root_url, source_label, args, supabase, retailer_id,
    seen_pids, errors, per_source_counts, dedup_hits,
):
    """Walk a single Bud's listing root URL via /page/N pagination.

    Page 1 lives at the root URL (no /page/1 suffix); pages 2..N use
    `<root>/page/N`. Stops when a page returns 0 cards or PAGE_HARD_CAP
    is hit. Returns (saved, skipped, fetched_any) — fetched_any is True
    iff at least one page on this root returned 200.
    """
    saved = 0
    skipped = 0
    fetched_any = False

    for page_num in range(1, PAGE_HARD_CAP + 1):
        if args.limit_pages is not None and page_num > args.limit_pages:
            break
        if page_num > 1:
            time.sleep(args.crawl_delay)

        url = root_url if page_num == 1 else f'{root_url}/page/{page_num}'
        print(f'\n=== {source_label} PAGE {page_num} === {url}')
        try:
            html = fetch(url)
        except Exception as e:
            msg = f'{source_label} page {page_num} fetch failed: {e}'
            print(f'  [FETCH-ERR] {msg}')
            errors.append(msg)
            continue

        fetched_any = True
        cards = chunk_cards(html)
        print(f'  {len(cards)} card(s) on page {page_num}')
        if not cards:
            print('  (no cards — end of paginated set)')
            break

        for pid, card_html in cards:
            if args.limit_products is not None and saved >= args.limit_products:
                break
            if pid in seen_pids:
                # Cross-strategy dedup hit — pids from a per-caliber
                # filter walk already in seen_pids show up again in
                # the all-ammo backstop. Tracked so the dry-run report
                # can quantify how much overlap exists between the two
                # discovery strategies.
                dedup_hits[0] += 1
                continue

            try:
                row = parse_card(card_html, pid)
            except Exception as e:
                msg = f'{source_label} page {page_num} pid {pid}: parse failed: {e}'
                print(f'  [PARSE-ERR] {msg}')
                errors.append(msg)
                skipped += 1
                continue
            if not row:
                skipped += 1
                continue

            cal_disp, cal_norm = normalize_caliber(row['title'])
            if not cal_norm:
                # Off-list caliber (45 ACP, 12 ga, 5.7x28, etc.) — quiet skip.
                skipped += 1
                continue

            if not sanity_check_ppr(
                row['price_per_round'], row['base_price'], row['total_rounds'],
                context=f'{RETAILER_SLUG} {cal_norm}', caliber=cal_norm,
            ):
                skipped += 1
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
                saved += 1
                per_source_counts.setdefault(source_label, Counter())[cal_norm] += 1
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
                    saved += 1
                    per_source_counts.setdefault(source_label, Counter())[cal_norm] += 1
                    print(
                        f"  Saved pid={row['pid']:>7} cal={cal_norm:<8} "
                        f"${row['base_price']:>7.2f} ({row['price_per_round']}/rd) "
                        f"{'IN ' if row['in_stock'] else 'OUT'} {row['title'][:55]}"
                    )
                except Exception as e:
                    msg = f'{source_label} page {page_num} {row["pid"]}: upsert failed: {e}'
                    print(f'  [DB-ERR] {msg}')
                    errors.append(msg)

        if args.limit_products is not None and saved >= args.limit_products:
            print(f'  hit --limit-products {args.limit_products}; stopping')
            break

    return saved, skipped, fetched_any


# ---------- Entrypoint ----------

def main() -> int:
    ap = argparse.ArgumentParser(description="Scrape Bud's Gun Shop ammunition listings.")
    ap.add_argument('--dry-run', action='store_true',
                    help='Parse and print only; no DB writes.')
    ap.add_argument('--limit-products', type=int, default=None,
                    help='Cap total products processed (dev/test).')
    ap.add_argument('--limit-pages', type=int, default=None,
                    help='Cap pages walked per listing root (dev/test).')
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
    started_at = time.time()

    saved_total = 0
    skipped_total = 0
    errors: list[str] = []
    seen_pids: set[str] = set()
    per_source_counts: dict[str, Counter] = {}
    # Mutable single-element list so walk_listing_root() can increment
    # the cross-strategy dedup-hit counter without needing a wrapper class.
    dedup_hits = [0]
    successful_roots = 0
    total_roots = len(CALIBER_FILTER_URLS) + 1  # +1 for the all-ammo backstop

    # Walk per-caliber filter views first. Six of our 10 calibers are
    # exposed via filter URLs; ordering them first means the all-ammo
    # backstop's dedup_hits counter quantifies how much its top-1000
    # overlap with the (smaller, focused) caliber views.
    for cal_norm, filter_url in CALIBER_FILTER_URLS.items():
        s, sk, ok = walk_listing_root(
            root_url=filter_url,
            source_label=f'filter[{cal_norm}]',
            args=args, supabase=supabase, retailer_id=retailer_id,
            seen_pids=seen_pids, errors=errors,
            per_source_counts=per_source_counts, dedup_hits=dedup_hits,
        )
        saved_total += s
        skipped_total += sk
        if ok:
            successful_roots += 1
        if args.limit_products is not None and saved_total >= args.limit_products:
            break

    # All-ammo backstop. Picks up the 4 calibers Bud's exposes no
    # filter URL for (380acp, 357mag, 762x39, 300blk) and any in-scope
    # SKUs that fall outside the per-caliber views.
    if args.limit_products is None or saved_total < args.limit_products:
        s, sk, ok = walk_listing_root(
            root_url=LISTING_BASE,
            source_label='all_ammo',
            args=args, supabase=supabase, retailer_id=retailer_id,
            seen_pids=seen_pids, errors=errors,
            per_source_counts=per_source_counts, dedup_hits=dedup_hits,
        )
        saved_total += s
        skipped_total += sk
        if ok:
            successful_roots += 1

    elapsed = time.time() - started_at
    mode = 'DRY RUN' if args.dry_run else 'LIVE'
    print(f'\n=== TOTALS ({mode}) ===')
    print(f'  saved={saved_total}  skipped={skipped_total}  errors={len(errors)}')
    print(f'  dedup_hits (pid already seen by an earlier root): {dedup_hits[0]}')
    print(f'  URL roots loaded: {successful_roots} / {total_roots}')
    print(f'  elapsed: {elapsed:.1f}s')

    print('\nPer-source per-caliber save counts:')
    for source in sorted(per_source_counts):
        counts = per_source_counts[source]
        print(f'  {source}:')
        for cal, n in counts.most_common():
            print(f'    {cal:10s} {n}')

    if errors:
        print(f'\n=== {len(errors)} ERROR(S) ===')
        for e in errors[:30]:
            print(f'  {e}')
        if len(errors) > 30:
            print(f'  ... and {len(errors) - 30} more')

    # Loud-failure gate — every URL root failed to load. Mirrors the
    # gate added to scraper_blackbasin.py 2026-05-08 to prevent the
    # silent-stale class of bug, where a wholesale site migration or
    # CDN block left a scraper writing 0 rows behind exit code 0 for
    # nine days. Returning non-zero here makes the GitHub Actions
    # step go red and triggers the health-check email.
    if successful_roots == 0:
        print(
            "\nFATAL: every Bud's listing URL failed to load — site may "
            "have moved, slugs may have churned, or anti-bot wall is up. "
            "Manual recon needed."
        )
        return 1

    return 0 if not errors else 1


if __name__ == '__main__':
    sys.exit(main())
