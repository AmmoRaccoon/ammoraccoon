"""scraper_winchester_rebates.py — manufacturer rebate scraper for Winchester.

Two-step crawl: the index at https://winchester.com/Rebates lists active
rebates as <a href="/Rebates/<slug>"> cards; each slug expands to a detail
page whose body contains the rebate's title, headline amount, dates, min
qty / max payout, and eligible products list.

Detail-page conventions (verified against the Spring 2026 Turkey and 16GA
rebates):
  <h1>           rebate title (e.g., "Winchester Turkey Ammunition Rebate")
  <h2>           headline ("Earn $X per box back ...") — noisy, not parsed
  <p> after h2   canonical description: amount, max, dates, deadline
  <div> blocks   redundant fine-print copies of min/max/eligible/dates,
                 incl. the "Eligible on Winchester® <products>" line we
                 use as the authoritative source for eligible products.

Winchester rebates observed in the wild are single-tier — one rebate
amount applies to every eligible product — so eligible_products rows
all share the same amount_override.

Writes to:
  manufacturer_rebates                  (one row per active rebate)
  manufacturer_rebate_eligible_products (per-product rows, same amount)

Required env:
  SUPABASE_URL, SUPABASE_KEY

Usage:
  python scraper_winchester_rebates.py --dry-run
  python scraper_winchester_rebates.py
"""

import argparse
import hashlib
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from scraper_lib import parse_firearm_type
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_KEY']

USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
)

SOURCE = 'winchester'
BRAND = 'Winchester'
LIST_URL = 'https://winchester.com/Rebates'

NUMBER_WORDS = {
    'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
    'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10,
}


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
    amount_per_unit: Optional[float] = None
    amount_max_total: Optional[float] = None
    min_qty_required: Optional[int] = None
    valid_from: Optional[str] = None        # 'YYYY-MM-DD'
    valid_through: Optional[str] = None
    submit_by: Optional[str] = None


def _strip(text: str) -> str:
    return re.sub(r'\s+', ' ', text or '').strip()


def _parse_long_date(s: str) -> Optional[str]:
    """Parse 'Month D, YYYY' into ISO 'YYYY-MM-DD'."""
    m = re.search(r'([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})', s)
    if not m:
        return None
    try:
        return datetime.strptime(
            f'{m.group(1)} {m.group(2)} {m.group(3)}', '%B %d %Y',
        ).strftime('%Y-%m-%d')
    except ValueError:
        return None


def fetch(url: str) -> str:
    resp = requests.get(url, headers={'User-Agent': USER_AGENT}, timeout=30)
    resp.raise_for_status()
    return resp.text


def discover_rebate_slugs(html: str) -> list:
    """Pull unique '/Rebates/<slug>' hrefs out of the index page."""
    soup = BeautifulSoup(html, 'html.parser')
    seen = []
    for a in soup.find_all('a', href=re.compile(r'^/Rebates/[A-Za-z0-9-]+$')):
        href = a['href']
        if href not in seen:
            seen.append(href)
    return seen


def _find_eligible_products_text(soup) -> str:
    """Return the text of the 'Eligible on …' fine-print div, or ''.

    The detail-page template puts redundant fine-print blocks below the
    image. The one we want starts with 'Eligible on'.
    """
    for div in soup.find_all('div'):
        text = _strip(div.get_text(' '))
        if text.lower().startswith('eligible on '):
            return text
    return ''


def _parse_eligible_products(text: str, amount: Optional[float]) -> list:
    """Extract product names from a string like:
       'Eligible on Winchester® Super-X®, Double-X®, Long Beard® XR®,
        Long Beard® Tungsten and Long Beard® TSS turkey loads.'
       'Eligible on Winchester® WE16GT6A, X16H4A, ... AND XU168A'
    """
    if not text or amount is None:
        return []

    cleaned = re.sub(r'[®™©]', '', text)
    cleaned = re.sub(r'^Eligible on\s*', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'^Winchester\s+', '', cleaned, flags=re.IGNORECASE)

    # Drop a trailing product-category phrase ("turkey loads.", "loads.", etc.).
    cleaned = re.sub(
        r'\s*(?:turkey loads?|shotshells?|loads?|ammunition)\.?$',
        '', cleaned, flags=re.IGNORECASE,
    )
    cleaned = cleaned.rstrip('.').strip()

    # Comma- and (and|AND)-separated.
    chunks = re.split(r',\s*|\s+(?:and|AND)\s+', cleaned)
    products = []
    for chunk in chunks:
        name = _strip(chunk)
        if not name or name.lower() in {'and'}:
            continue
        products.append(EligibleProduct(product_line=name, amount=amount))
    return products


def _parse_amount_per_unit(main_text: str) -> Optional[float]:
    """e.g. 'earn $5 per box rebate' or '$2 back for each box'."""
    for pattern in (
        r'\$(\d+(?:\.\d{1,2})?)\s*(?:per box|back for each box)',
        r'earn\s*\$(\d+(?:\.\d{1,2})?)',
    ):
        m = re.search(pattern, main_text, re.IGNORECASE)
        if m:
            return float(m.group(1))
    return None


def _parse_max_total(main_text: str) -> Optional[float]:
    m = re.search(
        r'Maximum rebate(?:\s*amount)?\s*is\s*\$(\d+(?:\.\d{1,2})?)',
        main_text, re.IGNORECASE,
    )
    if m:
        return float(m.group(1))
    m = re.search(r'Earn\s*[Uu]p\s*[Tt]o\s*\$(\d+(?:\.\d{1,2})?)', main_text)
    if m:
        return float(m.group(1))
    return None


def _parse_min_qty(text: str) -> Optional[int]:
    m = re.search(
        r'Minimum\s+(\d+|' + '|'.join(NUMBER_WORDS) + r')\s+box',
        text, re.IGNORECASE,
    )
    if not m:
        return None
    token = m.group(1).lower()
    if token.isdigit():
        return int(token)
    return NUMBER_WORDS.get(token)


def _parse_valid_window(text: str) -> tuple:
    """e.g. 'between March 6, 2026 – May 31, 2026'."""
    m = re.search(
        r'between\s+([A-Za-z]+\s+\d{1,2},\s*\d{4})\s*[–\-]\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})',
        text,
    )
    if not m:
        return None, None
    return _parse_long_date(m.group(1)), _parse_long_date(m.group(2))


def _parse_submit_by(text: str) -> Optional[str]:
    m = re.search(
        r'postmarked\s+no\s+later\s+than\s+([A-Za-z]+\s+\d{1,2},\s*\d{4})',
        text, re.IGNORECASE,
    )
    return _parse_long_date(m.group(1)) if m else None


def parse_detail_page(html: str, detail_url: str, slug: str) -> ParsedRebate:
    soup = BeautifulSoup(html, 'html.parser')

    h1 = soup.find('h1')
    title = _strip(h1.get_text(' ')) if h1 else slug

    # The canonical description is the first <p> following the <h2> headline,
    # which sits inside the .p-content__main / .p-layout__main column. Falling
    # back to the longest <p> on the page covers structural drift.
    main_text = ''
    if h1:
        for sibling in h1.find_all_next(['h2', 'p']):
            if sibling.name == 'p':
                candidate = _strip(sibling.get_text(' '))
                if 'rebate' in candidate.lower() or '$' in candidate:
                    main_text = candidate
                    break
    if not main_text:
        # Fallback: longest <p> on the page.
        candidates = [_strip(p.get_text(' ')) for p in soup.find_all('p')]
        main_text = max(candidates, key=len) if candidates else ''

    eligible_text = _find_eligible_products_text(soup)
    fine_print_text = _strip(' '.join(
        _strip(div.get_text(' '))
        for div in soup.find_all('div')
        if 'rebate' in _strip(div.get_text(' ')).lower()
        or 'postmark' in _strip(div.get_text(' ')).lower()
        or 'between' in _strip(div.get_text(' ')).lower()
    ))

    combined = f'{main_text} {fine_print_text}'

    amount_per_unit = _parse_amount_per_unit(combined)
    amount_max_total = _parse_max_total(combined)
    min_qty = _parse_min_qty(combined)
    valid_from, valid_through = _parse_valid_window(combined)
    submit_by = _parse_submit_by(combined)

    eligible_products = _parse_eligible_products(eligible_text, amount_per_unit)
    if not eligible_products:
        # Try the main paragraph's product mention if the fine-print line is missing.
        # Pulls "Winchester® A, B and C turkey loads" from the main copy.
        m = re.search(
            r'Winchester[®\s]*([^.]+?(?:loads?|shotshells?|ammunition))',
            main_text,
        )
        if m:
            eligible_products = _parse_eligible_products(
                f'Eligible on Winchester {m.group(1)}', amount_per_unit,
            )

    raw_terms = _strip(combined)

    return ParsedRebate(
        external_id=slug,
        title=title,
        detail_url=detail_url,
        raw_terms=raw_terms,
        eligible_products=eligible_products,
        amount_per_unit=amount_per_unit,
        amount_max_total=amount_max_total,
        min_qty_required=min_qty,
        valid_from=valid_from,
        valid_through=valid_through,
        submit_by=submit_by,
    )


def upsert_rebate(supabase, rebate: ParsedRebate, html_hash: str) -> int:
    now = datetime.now(timezone.utc).isoformat()

    missing = [
        f for f, v in {
            'amount_per_unit': rebate.amount_per_unit,
            'valid_from': rebate.valid_from,
            'valid_through': rebate.valid_through,
            'submit_by': rebate.submit_by,
        }.items() if v is None
    ]
    if missing:
        raise ValueError(
            f'rebate {rebate.external_id} missing required fields: {missing}. '
            f'Refusing to insert; check parser against detail page HTML.'
        )

    row = {
        'external_id': rebate.external_id,
        'source': SOURCE,
        'brand': BRAND,
        'title': rebate.title,
        'detail_url': rebate.detail_url,
        'source_url': LIST_URL,
        'amount_min_per_unit': rebate.amount_per_unit,
        'amount_max_per_unit': rebate.amount_per_unit,
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

    # firearm_type: only set on inserts and on rows whose existing value
    # is NULL. Jon's manual classification, when present, is the source
    # of truth — never overwrite it with a heuristic call. Pre-fetch on
    # (source, external_id) is the upsert key, so the lookup is cheap
    # and unambiguous.
    existing = (
        supabase.table('manufacturer_rebates')
        .select('firearm_type')
        .eq('source', SOURCE)
        .eq('external_id', rebate.external_id)
        .limit(1)
        .execute()
        .data
    )
    if not existing or existing[0].get('firearm_type') is None:
        row['firearm_type'] = parse_firearm_type(rebate.title, rebate.raw_terms)

    res = (
        supabase.table('manufacturer_rebates')
        .upsert(row, on_conflict='source,external_id')
        .execute()
    )
    rebate_id = res.data[0]['id']

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


def main() -> int:
    parser = argparse.ArgumentParser(description='Scrape Winchester rebate pages.')
    parser.add_argument('--dry-run', action='store_true',
                        help='Parse and print only; no DB writes.')
    args = parser.parse_args()

    print(f'Index: {LIST_URL}')
    list_html = fetch(LIST_URL)
    slugs = discover_rebate_slugs(list_html)
    print(f'  found {len(slugs)} rebate slug(s):')
    for s in slugs:
        print(f'    {s}')

    supabase = None if args.dry_run else create_client(SUPABASE_URL, SUPABASE_KEY)
    saved = 0

    for href in slugs:
        detail_url = urljoin(LIST_URL, href)
        slug = href.rsplit('/', 1)[-1]
        try:
            html = fetch(detail_url)
        except Exception as e:
            print(f'\n  [{slug}] FETCH FAILED: {e}')
            continue
        html_hash = hashlib.sha256(html.encode('utf-8')).hexdigest()
        rebate = parse_detail_page(html, detail_url, slug)

        print(f'\n  [{rebate.external_id}] {rebate.title}')
        print(f'    valid: {rebate.valid_from} -> {rebate.valid_through}  |  submit by {rebate.submit_by}')
        print(f'    per-unit ${rebate.amount_per_unit}  '
              f'max total ${rebate.amount_max_total}  min qty {rebate.min_qty_required}')
        print(f'    eligible products ({len(rebate.eligible_products)}):')
        for p in rebate.eligible_products:
            print(f'      ${p.amount:.2f}  {p.product_line}')

        if args.dry_run:
            saved += 1
            continue
        try:
            upsert_rebate(supabase, rebate, html_hash)
            saved += 1
        except Exception as e:
            print(f'    UPSERT FAILED: {e}')

    mode = 'DRY RUN' if args.dry_run else 'LIVE'
    print(f'\nDone ({mode}). {saved} rebate(s) {"would be " if args.dry_run else ""}upserted.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
