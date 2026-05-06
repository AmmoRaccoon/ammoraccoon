"""scraper_brownells.py — Brownells.com ammunition scraper.

EpiServer Commerce storefront fronted by Cloudflare in CDN/cache mode (no
JS challenge). Plain `requests` returns the SSR'd category cards with all
the data we need on the card itself: data-pid, data-product-url,
data-price (single or range), data-round (per-round price, single or range).

Key design notes (full recon in scripts/brownells_unit_mapping.md, web repo):

  - Per-caliber URL slugs fall back to the parent ammunition page —
    /ammunition/handgun-ammo/9mm-luger-ammo/index.htm and
    /ammunition/handgun-ammunition/ return the same 32 mixed-caliber
    cards. So caliber assignment is title-driven via normalize_caliber(),
    NOT URL-driven. We crawl three parent indexes only.

  - Pagination is JS-driven (infinite scroll). ?page=N has no effect on
    plain HTTP. v1 takes the first SSR'd ~32 cards per parent index;
    after caliber filtering we expect ~30-60 listings per run. v2 paths
    (sitemap walk + PDP, or infinite-scroll endpoint) are documented in
    the recon doc; not implemented here.

  - Multi-pack product families render as range data on the card:
      data-price="$14.99 - $279.99"   data-round="($0.28/Round - $0.34/Round)"
    The high price pairs with the low PPR (the bulk pack). For the
    comparison engine, we store the bulk-tier row (best PPR the customer
    can get on this family).

  - OOS detection is the per-card "NOTIFY ME WHEN IT'S BACK IN STOCK"
    button text. The "back-in-stock" badge image is a "Recently Back In
    Stock" *marketing* tag — those products ARE in stock.

Robots.txt is permissive (Allow: /, no Crawl-delay). Defaulting to 10s
between requests to mirror the Powder Valley scraper's posture.
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
    CALIBERS, normalize_caliber, now_iso, with_stock_fields,
    parse_purchase_limit, parse_brand, sanity_check_ppr, clean_title,
    parse_bullet_type,
)

load_dotenv()

RETAILER_SLUG = "brownells"
SITE_BASE = "https://www.brownells.com"

# Three parent ammo indexes — see Trap #1 in the recon doc. Order is
# fixed (not alphabetical) so caliber-collision dedup via seen_pids
# behaves consistently across runs.
CATEGORY_URLS = {
    'handgun': '/ammunition/handgun-ammunition/',
    'rifle':   '/ammunition/rifle-ammunition/',
    'rimfire': '/ammunition/rimfire-ammunition/',
}

CRAWL_DELAY_SEC = 10
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

# Bulk-pack tier price-per-round and prices both come out of the card's
# data-* attributes. Both attributes carry single or range values.
_PRICE_RE = re.compile(r'\$\s*([\d,]+(?:\.\d{1,2})?)')
_PPR_RE = re.compile(r'\$?\s*(\d+(?:\.\d+)?)\s*/\s*Round', re.IGNORECASE)
_PID_RE = re.compile(r'data-pid="(\d+)"')
_URL_RE = re.compile(r'data-product-url="([^"]+)"')
_PRICE_ATTR_RE = re.compile(r'data-price="([^"]*)"')
_ROUND_ATTR_RE = re.compile(r'data-round="([^"]*)"')
_TITLE_RE = re.compile(
    r'class="category-slider__item__title[^"]*"[^>]*>\s*(.+?)\s*</a>',
    re.DOTALL | re.IGNORECASE,
)
# OOS marker — Brownells' Vue dialog renders this exact button text on
# cards with no in-stock variants. Spelling is the apostrophe + literal
# "S" in "IT'S" so use a relaxed regex that tolerates &#39; / smart quotes.
_OOS_RE = re.compile(
    r'NOTIFY\s*ME\s*WHEN\s*IT.{1,5}S?\s*BACK\s*IN\s*STOCK',
    re.IGNORECASE,
)


def parse_grain(text: str) -> Optional[int]:
    m = re.search(r'(\d+)\s*gr(?:ain)?\b', text, re.IGNORECASE)
    return int(m.group(1)) if m else None


def parse_rounds(text: str) -> Optional[int]:
    """Best-effort round count from a title.

    Brownells titles roll up product families and usually omit the count,
    so this is mostly a fallback for the rare card that does carry one.
    """
    for pat in (
        r'(\d[\d,]*)\s*[- ]?\s*rounds?\b',
        r'(\d[\d,]*)\s*[- ]?\s*rds?\b',
        r'(\d[\d,]*)\s*[- ]?\s*ct\b',
        r'(\d[\d,]*)\s*per\s*box',
    ):
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return int(m.group(1).replace(',', ''))
    return None


def parse_case_material(text: str) -> str:
    t = text.lower()
    steel_brands = ('wolf', 'tula', 'tulammo', 'brown bear', 'silver bear',
                    'golden bear', 'barnaul')
    if any(b in t for b in steel_brands) or 'steel case' in t or 'steel-case' in t \
            or ' steel ' in f' {t} ':
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


def _extract_amounts(price_attr: str, regex: re.Pattern) -> list[float]:
    out: list[float] = []
    for m in regex.finditer(price_attr or ''):
        try:
            out.append(float(m.group(1).replace(',', '')))
        except ValueError:
            continue
    return out


def parse_card(card_html: str) -> Optional[dict]:
    """Return a parsed listing dict for one Brownells card, or None.

    Returning None signals "skip this card" — caller increments its
    skipped counter. Use it for cards missing required fields, not for
    real upstream errors.
    """
    pid_m = _PID_RE.search(card_html)
    url_m = _URL_RE.search(card_html)
    price_attr_m = _PRICE_ATTR_RE.search(card_html)
    title_m = _TITLE_RE.search(card_html)
    if not (pid_m and url_m and price_attr_m and title_m):
        return None

    title_raw = re.sub(r'\s+', ' ', title_m.group(1)).strip()
    title = clean_title(htmllib.unescape(title_raw))
    if not title:
        return None

    in_stock = not bool(_OOS_RE.search(card_html))

    prices = _extract_amounts(price_attr_m.group(1), _PRICE_RE)
    if not prices:
        return None
    low_price = min(prices)
    high_price = max(prices)

    pprs: list[float] = []
    round_attr_m = _ROUND_ATTR_RE.search(card_html)
    if round_attr_m:
        pprs = _extract_amounts(round_attr_m.group(1), _PPR_RE)

    if pprs:
        # Best-tier (bulk-pack) row: high price + low PPR.
        base_price = high_price
        price_per_round = min(pprs)
        # Back-derive total_rounds. round() is well-defined since
        # Brownells doesn't sell partial rounds.
        if price_per_round <= 0:
            return None
        total_rounds = max(1, round(base_price / price_per_round))
    else:
        # OOS-card layout: data-round is sometimes empty. Fall back to
        # the box (low price) + title-parsed count. Many Brownells titles
        # omit the count, in which case we have to skip the card.
        base_price = low_price
        total_rounds = parse_rounds(title)
        if not total_rounds or total_rounds <= 0:
            return None
        price_per_round = round(base_price / total_rounds, 4)

    return {
        'pid': pid_m.group(1),
        'url': SITE_BASE + url_m.group(1),
        'title': title,
        'in_stock': in_stock,
        'base_price': base_price,
        'price_per_round': price_per_round,
        'total_rounds': total_rounds,
    }


# ---------- Page chunking ----------

def chunk_cards(html: str) -> list[str]:
    """Split a category page into per-card HTML chunks.

    Brownells wraps each card in <div class="category-slider__item ...">.
    Splitting on a lookahead at that wrapper open gives clean per-card
    fragments — each chunk starts with one wrapper and ends just before
    the next, so per-card OOS detection / data-* extraction can't bleed
    into a neighbor's HTML (which a fixed-window slice would do).
    """
    parts = re.split(
        r'(?=<div\s+class="[^"]*category-slider__item[^"]*")',
        html,
        flags=re.IGNORECASE,
    )
    return [p for p in parts[1:] if 'data-pid="' in p]


# ---------- Entrypoint ----------

def main() -> int:
    ap = argparse.ArgumentParser(description='Scrape Brownells ammunition listings.')
    ap.add_argument('--dry-run', action='store_true',
                    help='Parse and print only; no DB writes.')
    ap.add_argument('--category', choices=list(CATEGORY_URLS.keys()) + ['all'],
                    default='all',
                    help='Limit to one parent index (handgun/rifle/rimfire).')
    ap.add_argument('--limit-products', type=int, default=None,
                    help='Cap card processing per category (dev/test).')
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

    cats = list(CATEGORY_URLS.keys()) if args.category == 'all' else [args.category]

    seen_pids: set[str] = set()
    saved_total = 0
    skipped_total = 0
    errors: list[str] = []

    print(f'[{datetime.now().isoformat()}] Brownells scraper starting '
          f'(mode={"DRY RUN" if args.dry_run else "LIVE"}, '
          f'categories={",".join(cats)}, retailer_id={retailer_id})')

    for cat_name in cats:
        url = SITE_BASE + CATEGORY_URLS[cat_name]
        print(f'\n=== {cat_name.upper()} === {url}')
        try:
            html = fetch(url)
        except Exception as e:
            msg = f'[{cat_name}] fetch failed: {e}'
            print(f'  [FETCH-ERR] {msg}')
            errors.append(msg)
            time.sleep(CRAWL_DELAY_SEC)
            continue

        cards = chunk_cards(html)
        print(f'  {len(cards)} card(s) on page 1')
        if args.limit_products is not None:
            cards = cards[:args.limit_products]
            print(f'  capped to {len(cards)} for this run')

        for i, card_html in enumerate(cards, 1):
            try:
                row = parse_card(card_html)
            except Exception as e:
                msg = f'[{cat_name}] card {i}: parse failed: {e}'
                print(f'  [PARSE-ERR] {msg}')
                errors.append(msg)
                skipped_total += 1
                continue
            if not row:
                skipped_total += 1
                continue

            cal_disp, cal_norm = normalize_caliber(row['title'])
            if not cal_norm:
                skipped_total += 1
                # Keep this print quiet — most off-list calibers are expected
                # (Brownells parents are mixed-caliber on purpose).
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
                    f"  [DRY] pid={row['pid']:>9} cal={cal_norm:<8} "
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
                        f"  Saved pid={row['pid']:>9} cal={cal_norm:<8} "
                        f"${row['base_price']:>7.2f} ({row['price_per_round']}/rd) "
                        f"{'IN ' if row['in_stock'] else 'OUT'} {row['title'][:55]}"
                    )
                except Exception as e:
                    msg = f'[{cat_name}] {row["pid"]}: upsert failed: {e}'
                    print(f'  [DB-ERR] {msg}')
                    errors.append(msg)

        time.sleep(CRAWL_DELAY_SEC)

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
