"""scraper_kinetic_rebates.py — manufacturer rebate scraper for Federal + Remington.

Federal and Remington (both Kinetic Group brands) share an identical rebate-page
template: each active rebate is anchored at a `<div id="R104XX"/>` element, followed
by an <h3> title carrying the promotion #, and a sequence of <p> tags that hold:
  - eligible products and per-tier amounts (`<p class="fine-print">`)
  - minimum qty + max payout (`<p class="minimum-copy">`)
  - valid date window + submission deadline (`<p class="font-italic">`)

The scraper finds every R10XXX block on each brand's marketing page, parses it,
and upserts into manufacturer_rebates / manufacturer_rebate_eligible_products
(see migrations/007_rebates.sql). It does NOT touch the v1 `rebates` table.

Required env:
  SUPABASE_URL, SUPABASE_KEY

Usage:
  python scraper_kinetic_rebates.py                # scrape both, write to DB
  python scraper_kinetic_rebates.py --dry-run      # parse only, no writes
  python scraper_kinetic_rebates.py --source federal
"""

import argparse
import hashlib
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_KEY']

USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
)

SOURCES = {
    'federal': {
        'brand': 'Federal',
        'list_url': 'https://www.federalpremium.com/this-is-federal/rebates-and-promotions.html',
    },
    'remington': {
        'brand': 'Remington',
        'list_url': 'https://www.remington.com/remington-country/rebates-and-promotions.html',
    },
}

PROMOTION_ID_RE = re.compile(r'^R\d{5}$')


@dataclass
class EligibleProduct:
    product_line: str
    amount: float


@dataclass
class ParsedRebate:
    external_id: str
    title: str
    detail_url: str
    raw_terms: str
    eligible_products: list = field(default_factory=list)
    amount_min_per_unit: Optional[float] = None
    amount_max_per_unit: Optional[float] = None
    amount_max_total: Optional[float] = None
    min_qty_required: Optional[int] = None
    valid_from: Optional[str] = None        # ISO date 'YYYY-MM-DD'
    valid_through: Optional[str] = None
    submit_by: Optional[str] = None


def _strip(text: str) -> str:
    return re.sub(r'\s+', ' ', text or '').strip()


def _parse_us_date(s: str) -> Optional[str]:
    """Parse 'M/D/YYYY' or 'MM/DD/YYYY' into 'YYYY-MM-DD'."""
    m = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', s)
    if not m:
        return None
    mo, d, y = (int(x) for x in m.groups())
    try:
        return datetime(y, mo, d).strftime('%Y-%m-%d')
    except ValueError:
        return None


def _parse_eligible_products(fine_print_html: str) -> list:
    """Pull tiered (product, amount) pairs out of a fine-print paragraph.

    The structure is one tier per line, separated by <br>, with the shape:
      "<brand> <comma-separated product list> [—|-] $<amount> rebate per box"
    Federal uses em-dash, Remington uses ASCII dash with various spacing —
    accept both. Products carry trademark glyphs (®, ™) that we strip.
    """
    products = []

    # Split by <br>/<br/>/</br> — Kinetic pages use </br> (invalid but consistent).
    soup = BeautifulSoup(fine_print_html, 'html.parser')
    raw = soup.get_text('\n')
    lines = [_strip(line) for line in raw.split('\n') if _strip(line)]

    for line in lines:
        if 'rebate per box' not in line.lower():
            continue
        # Find amount: last $X.XX on the line.
        amounts = re.findall(r'\$\s*(\d+(?:\.\d{1,2})?)', line)
        if not amounts:
            continue
        amount = float(amounts[-1])

        # Strip trademark glyphs and normalize dashes.
        cleaned = re.sub(r'[®™©]', '', line)
        # Split off the amount portion.
        before_amount = re.split(r'[—–-]\s*\$', cleaned, maxsplit=1)[0]
        before_amount = _strip(before_amount.rstrip(',-—–'))

        # Drop a leading brand prefix so product_line is the line name
        # (e.g., "Strut-Shok") not "Federal Strut-Shok". Only strip the
        # bare brand — not brand+sub-line combos like "Remington Premier",
        # which would chew off the first product's name in a list like
        # "Remington Premier Magnum Turkey, Premier Magnum Turkey HV, ...".
        before_amount = re.sub(
            r'^(Federal Premium|Federal|Remington)\s+',
            '', before_amount, count=1, flags=re.IGNORECASE,
        )

        # Comma- and "or"-separated product list. Comma-split fires first,
        # so a stray "or " can survive on the next chunk — strip it after.
        chunks = re.split(r',\s*|\s+or\s+', before_amount)
        for chunk in chunks:
            name = re.sub(r'^or\s+', '', _strip(chunk), flags=re.IGNORECASE)
            name = _strip(name)
            if not name or name.lower() in {'and', 'or'}:
                continue
            products.append(EligibleProduct(product_line=name, amount=amount))

    return products


def _parse_min_max(min_copy_text: str) -> tuple:
    """Pull (min_qty, max_total) from a minimum-copy paragraph.

    e.g. 'Minimum purchase two (2) boxes required. Maximum rebate $100.00 per
    person or household.'
    """
    qty_match = re.search(r'\((\d+)\)\s*box', min_copy_text)
    min_qty = int(qty_match.group(1)) if qty_match else None

    max_match = re.search(r'Maximum rebate\s*\$\s*(\d+(?:\.\d{1,2})?)', min_copy_text, re.IGNORECASE)
    max_total = float(max_match.group(1)) if max_match else None

    return min_qty, max_total


def _parse_dates(italic_text: str) -> tuple:
    """Pull (valid_from, valid_through, submit_by) from a font-italic paragraph."""
    valid_from = valid_through = submit_by = None

    valid_match = re.search(
        r'(?:Valid for purchases made|valid)\s*'
        r'(\d{1,2}/\d{1,2}/\d{4})\s*through\s*(\d{1,2}/\d{1,2}/\d{4})',
        italic_text, re.IGNORECASE,
    )
    if valid_match:
        valid_from = _parse_us_date(valid_match.group(1))
        valid_through = _parse_us_date(valid_match.group(2))

    submit_match = re.search(
        r'(?:DEADLINE|deadline)[^0-9]*(\d{1,2}/\d{1,2}/\d{4})',
        italic_text,
    )
    if submit_match:
        submit_by = _parse_us_date(submit_match.group(1))

    return valid_from, valid_through, submit_by


def parse_rebate_page(html: str, list_url: str) -> list:
    """Find every <div id="R10XXX"/> on a Kinetic-template rebate page and parse it."""
    soup = BeautifulSoup(html, 'html.parser')
    parsed = []

    for anchor in soup.find_all('div', id=PROMOTION_ID_RE):
        external_id = anchor['id']
        # The rebate body is in a sibling/ancestor container with the H3 + paragraphs.
        # Most reliable: walk up to the nearest ancestor that contains both an <h3>
        # and our anchor.
        container = anchor
        for _ in range(6):
            container = container.parent
            if container is None:
                break
            if container.find('h3') and container.find('p', class_='fine-print'):
                break
        if container is None:
            continue

        h3 = container.find('h3')
        title = _strip(re.sub(
            r'Promotion\s*#\s*:?\s*R\d{5}\s*$', '',
            h3.get_text(' '), flags=re.IGNORECASE,
        )) if h3 else external_id

        fine_print = container.find('p', class_='fine-print')
        eligible_products = _parse_eligible_products(str(fine_print)) if fine_print else []

        min_copy = container.find('p', class_='minimum-copy')
        min_qty, max_total = _parse_min_max(min_copy.get_text(' ')) if min_copy else (None, None)

        italic = container.find('p', class_='font-italic')
        valid_from, valid_through, submit_by = (
            _parse_dates(italic.get_text(' ')) if italic else (None, None, None)
        )

        amounts = [p.amount for p in eligible_products]
        amount_min = min(amounts) if amounts else None
        amount_max = max(amounts) if amounts else None

        # The brand site doesn't have per-rebate pages for Federal/Remington —
        # the anchor is the closest thing to a detail URL.
        detail_url = f'{list_url}#{external_id}'

        # raw_terms: full text of the rebate container, for display / debugging.
        raw_terms = _strip(container.get_text(' '))

        parsed.append(ParsedRebate(
            external_id=external_id,
            title=title or external_id,
            detail_url=detail_url,
            raw_terms=raw_terms,
            eligible_products=eligible_products,
            amount_min_per_unit=amount_min,
            amount_max_per_unit=amount_max,
            amount_max_total=max_total,
            min_qty_required=min_qty,
            valid_from=valid_from,
            valid_through=valid_through,
            submit_by=submit_by,
        ))

    return parsed


def fetch(url: str) -> str:
    resp = requests.get(url, headers={'User-Agent': USER_AGENT}, timeout=30)
    resp.raise_for_status()
    return resp.text


def upsert_rebate(supabase, source: str, brand: str, source_url: str,
                  rebate: ParsedRebate, html_hash: str) -> int:
    """Upsert one rebate + replace its eligible products. Returns rebate id."""
    now = datetime.now(timezone.utc).isoformat()

    # Required-not-null check before sending — fail loud rather than write garbage.
    missing = [
        f for f, v in {
            'valid_from': rebate.valid_from,
            'valid_through': rebate.valid_through,
            'submit_by': rebate.submit_by,
        }.items() if v is None
    ]
    if missing:
        raise ValueError(
            f'rebate {rebate.external_id} missing required fields: {missing}. '
            f'Refusing to insert; check parser against page HTML.'
        )

    row = {
        'external_id': rebate.external_id,
        'source': source,
        'brand': brand,
        'title': rebate.title,
        'detail_url': rebate.detail_url,
        'source_url': source_url,
        'amount_min_per_unit': rebate.amount_min_per_unit,
        'amount_max_per_unit': rebate.amount_max_per_unit,
        'amount_unit': 'per_box',
        'amount_max_total': rebate.amount_max_total,
        'min_qty_required': rebate.min_qty_required,
        'valid_from': rebate.valid_from,
        'valid_through': rebate.valid_through,
        'submit_by': rebate.submit_by,
        'last_seen_active_at': now,
        'last_scraped_at': now,
        'raw_terms': rebate.raw_terms,
        'terms_html_hash': html_hash,
    }

    res = (
        supabase.table('manufacturer_rebates')
        .upsert(row, on_conflict='source,external_id')
        .execute()
    )
    rebate_id = res.data[0]['id']

    # Children: simplest correct strategy is delete-and-insert. Tier amounts
    # can change mid-cycle; preserving rows would risk stale per-product amounts.
    supabase.table('manufacturer_rebate_eligible_products').delete().eq(
        'rebate_id', rebate_id,
    ).execute()

    if rebate.eligible_products:
        children = [
            {
                'rebate_id': rebate_id,
                'product_line': p.product_line,
                'amount_override': p.amount,
            }
            for p in rebate.eligible_products
        ]
        supabase.table('manufacturer_rebate_eligible_products').insert(children).execute()

    return rebate_id


def scrape_source(source: str, dry_run: bool, supabase=None) -> int:
    cfg = SOURCES[source]
    print(f'\n=== {source} ({cfg["brand"]}) ===')
    print(f'  url: {cfg["list_url"]}')

    html = fetch(cfg['list_url'])
    html_hash = hashlib.sha256(html.encode('utf-8')).hexdigest()
    rebates = parse_rebate_page(html, cfg['list_url'])
    print(f'  found {len(rebates)} rebate block(s)')

    saved = 0
    for r in rebates:
        print(f'\n  [{r.external_id}] {r.title}')
        print(f'    valid: {r.valid_from} -> {r.valid_through}  |  submit by {r.submit_by}')
        print(f'    per-unit ${r.amount_min_per_unit}-${r.amount_max_per_unit}  '
              f'max total ${r.amount_max_total}  min qty {r.min_qty_required}')
        print(f'    eligible products ({len(r.eligible_products)}):')
        for p in r.eligible_products:
            print(f'      ${p.amount:.2f}  {p.product_line}')

        if dry_run:
            saved += 1
            continue
        try:
            upsert_rebate(supabase, source, cfg['brand'], cfg['list_url'], r, html_hash)
            saved += 1
        except Exception as e:
            print(f'    UPSERT FAILED: {e}')

    return saved


def main() -> int:
    parser = argparse.ArgumentParser(description='Scrape Federal + Remington rebate pages.')
    parser.add_argument('--dry-run', action='store_true',
                        help='Parse and print only; no DB writes.')
    parser.add_argument('--source', choices=list(SOURCES.keys()) + ['all'], default='all',
                        help='Which source to scrape. Default: all.')
    args = parser.parse_args()

    supabase = None if args.dry_run else create_client(SUPABASE_URL, SUPABASE_KEY)
    sources = list(SOURCES.keys()) if args.source == 'all' else [args.source]

    total = 0
    for s in sources:
        try:
            total += scrape_source(s, args.dry_run, supabase=supabase)
        except Exception as e:
            print(f'  source {s} FAILED: {e}')

    mode = 'DRY RUN' if args.dry_run else 'LIVE'
    print(f'\nDone ({mode}). {total} rebate(s) {"would be " if args.dry_run else ""}upserted.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
