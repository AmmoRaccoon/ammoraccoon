"""scraper_pmc_ballistics.py — manufacturer ballistics scraper for PMC.

PMC publishes ballistics on per-product pages at /product/<sku-slug>/ on
the bare-hostname `pmcammo.com` domain (NOT the descriptive-slug shape
the 2026-05-14 9mm coverage audit inferred — see web repo TASKS.md
"Audit URL pattern bug"). Pages are server-rendered static HTML with no
tables; ballistics live as inline label/value pairs in display order.

The 6 9mm seed URLs span two product lines: Bronze (training, 9A/9B/9G/9H)
and StarFire (defensive JHP, 9SFX/9SFX-147GR). Audit conflated both as
"Bronze" — see TASKS.md "Audit product-line confusion." Both lines
share the same on-page label sequence (ITEM NO / BULLET TYPE / WEIGHT /
VELOCITY / ENERGY) but differ in H1: Bronze pages H1 with the caliber
("9mm Luger"), StarFire pages H1 with the SKU ("9SFX"). The parser
falls back to SKU-prefix caliber derivation when the H1 doesn't yield
a known caliber.

JSON-LD on the page is SEO-only (WebPage, ImageObject, BreadcrumbList) —
no @type:Product blob. All extraction is from the visible HTML.

Writes to:
  manufacturer_ballistics  (one row per product page)

Required env:
  SUPABASE_URL, SUPABASE_KEY  (the service-role key)

Usage:
  python scripts/scraper_pmc_ballistics.py --dry-run
  python scripts/scraper_pmc_ballistics.py --source pmc_handgun_9mm
"""

import argparse
import hashlib
import os
import re
import sys
from dataclasses import dataclass
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

SOURCE = 'pmc'
BRAND = 'PMC'

# Caliber-name → caliber_normalized. Used when H1 carries the caliber
# (Bronze pages).
# Phase B step 4 (2026-06-12): re-exported from the shared union map in
# caliber_registry_gen (BALLISTICS_CALIBER_NORMALIZE, emitted from
# calibers.json). D2: the union is a deliberate SUPERSET of the old per-source
# maps; the fresh live replay showed PMC's crawl gains exactly its .40 S&W
# row ('40 smith & wesson', a verified spec its own map dropped) and
# changes/removes nothing (scripts/_replay_ballistics_maps.py). SKU_PREFIX_TO_
# CALIBER below is PMC's own SKU-digit scheme and stays hand-written. This
# module runs as `python scripts/scraper_pmc_ballistics.py`, so add the repo
# root (where caliber_registry_gen.py lives) to sys.path before importing it.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from caliber_registry_gen import BALLISTICS_CALIBER_NORMALIZE as CALIBER_NORMALIZE  # noqa: E402

# SKU numeric-prefix → caliber_normalized. PMC encodes the caliber as
# the leading digits of the SKU (9A, 9SFX → 9mm; 45A → 45acp; 223A →
# 223-556). Used as a fallback when the H1 doesn't directly carry the
# caliber (StarFire pages H1 with the SKU instead).
SKU_PREFIX_TO_CALIBER = {
    '9': '9mm',
    '45': '45acp',
    '40': '40sw',
    '38': '38spl',
    '357': '357mag',
    '380': '380acp',
    '22': '22lr',
    '223': '223-556',
    '308': '308win',
    '300': '300blk',
    '762': '762x39',
}

# Phrase-based bullet lookup, mirrors the other ballistics scrapers.
# "starfire" / "sfhp" map to JHP because that's what listings use for
# any hollow-point product regardless of manufacturer marketing name.
_BULLET_TYPE_LOOKUP = [
    ('jacketed hollow point', 'JHP'),
    ('total metal jacket', 'TMJ'),
    ('full metal jacket', 'FMJ'),
    ('jacketed soft point', 'JSP'),
    ('starfire', 'JHP'),
    ('hollow point', 'JHP'),
    ('soft point', 'SP'),
    ('round nose', 'LRN'),
    ('flat point', 'FP'),
]

# PMC spec list emits codes directly; pass-through for the canonical
# ones. SFHP (StarFire Hollow Point) collapses to JHP per the same
# convention as the phrase lookup above.
_PMC_BULLET_CODES = {
    'fmj': 'FMJ', 'jhp': 'JHP', 'jsp': 'JSP', 'tmj': 'TMJ',
    'sp':  'SP',  'hp':  'JHP', 'lrn': 'LRN', 'fp': 'FP',
    'sfhp': 'JHP', 'fmj-fp': 'FMJ',   # PMC .40 Bronze flat-point = FMJ (listings already tag it FMJ)
}

SOURCES = {
    'pmc_handgun_9mm': {
        'brand': BRAND,
        # Confirmed 2026-05-17: all 6 URLs return HTTP 200. The audit's
        # /product/9mm-luger-{grain}gr-{bullet}-bronze/ pattern was 0/6.
        'seed_urls': [
            'https://pmcammo.com/product/bronze-9a/',       # 115gr FMJ
            'https://pmcammo.com/product/bronze-9b/',       # 115gr JHP
            'https://pmcammo.com/product/bronze-9g/',       # 124gr FMJ
            'https://pmcammo.com/product/bronze-9h/',       # 147gr FMJ
            'https://pmcammo.com/product/9sfx/',            # 124gr StarFire JHP
            'https://pmcammo.com/product/9sfx-147gr/',      # 147gr StarFire JHP
        ],
    },
    'pmc_handgun_40sw': {
        'brand': BRAND,
        # PMC's full .40 S&W catalog — three Bronze loads, one per grain,
        # single load per (grain, bullet): zero within-grain velocity
        # collision, so all three ship Tier 1 (no holds). Catalog confirmed
        # complete at three SKUs 2026-05-28.
        'seed_urls': [
            'https://pmcammo.com/product/bronze-40d/',      # 165gr FMJ, 989 fps
            'https://pmcammo.com/product/bronze-40e/',      # 180gr FMJ, 985 fps
            'https://pmcammo.com/product/bronze-40b/',      # 165gr JHP, 1040 fps
        ],
    },
}


@dataclass
class ParsedBallistics:
    external_id: str
    source_url: str
    sku: Optional[str] = None
    product_line: Optional[str] = None
    caliber_normalized: Optional[str] = None
    grain: Optional[int] = None
    bullet_type: Optional[str] = None
    muzzle_velocity_fps: Optional[int] = None
    muzzle_energy_ftlb: Optional[int] = None
    velocity_50yd: Optional[int] = None
    velocity_100yd: Optional[int] = None
    raw_name: Optional[str] = None


_session = requests.Session()
_session.headers.update({
    'User-Agent': USER_AGENT,
    'Accept-Encoding': 'gzip',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
})


def fetch(url: str) -> str:
    resp = _session.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


def _normalize_caliber(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    key = re.sub(r'\s*\+\s*p\b', '', s.lower()).strip()
    key = re.sub(r'\s+', ' ', key)
    return CALIBER_NORMALIZE.get(key)


def _normalize_bullet_type(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    low = text.lower().strip()
    if low in _PMC_BULLET_CODES:
        return _PMC_BULLET_CODES[low]
    for needle, code in _BULLET_TYPE_LOOKUP:
        if needle in low:
            return code
    short = text.strip().upper()
    if 2 <= len(short) <= 4 and short.isalpha():
        return short
    return None


def _caliber_from_sku(sku: Optional[str]) -> Optional[str]:
    """Fallback caliber derivation from SKU's leading digits.
    Works because PMC encodes caliber as the SKU prefix: 9A → 9mm,
    45A → 45acp, etc.
    """
    if not sku:
        return None
    m = re.match(r'^(\d+)', sku)
    if not m:
        return None
    return SKU_PREFIX_TO_CALIBER.get(m.group(1))


def _product_line_from_url(url: str) -> Optional[str]:
    """Identify the PMC product line by tokens in the URL slug.

    bronze-9a, bronze-9b   → Bronze
    9sfx, 9sfx-147gr       → StarFire
    erange-9ema            → eRange
    x-tac, xtac-9, etc.    → X-TAC
    """
    slug = url.rstrip('/').rsplit('/', 1)[-1].lower()
    if 'bronze' in slug:
        return 'Bronze'
    if 'sfx' in slug:
        return 'StarFire'
    if 'erange' in slug:
        return 'eRange'
    if 'xtac' in slug or 'x-tac' in slug:
        return 'X-TAC'
    return None


def _stripped_text_lines(html: str) -> list:
    """Strip <script>/<style>, then collapse remaining tags to newlines.
    Returns the visible text in display order, one entry per non-empty
    chunk. Walker functions below scan this list."""
    body = re.sub(r'<(script|style)[^>]*>[\s\S]*?</\1>', '', html, flags=re.IGNORECASE)
    txt = re.sub(r'<[^>]+>', '\n', body)
    return [s.strip() for s in re.split(r'\n+', txt) if s.strip()]


def _value_after_label(lines: list, label_re) -> Optional[str]:
    """Return the first non-empty line immediately after the first line
    that matches label_re (full-match)."""
    for i, line in enumerate(lines):
        if label_re.fullmatch(line):
            if i + 1 < len(lines):
                return lines[i + 1]
            return None
    return None


_SECTION_HEADER_RE = re.compile(r'[A-Z][A-Z ]{2,30}')


def _integer_in_section(lines: list, section_header_re, distance_label: str) -> Optional[int]:
    """Inside the section starting at section_header_re, find the line
    matching distance_label, return its next-line integer. The section
    ends at the next all-caps header line or end of list.

    Used for VELOCITY (muzzle / 25 / 50 / 75 / 100 Yds) and ENERGY
    (muzzle only) sections."""
    start = None
    for i, l in enumerate(lines):
        if section_header_re.fullmatch(l):
            start = i
            break
    if start is None:
        return None
    end = len(lines)
    for j in range(start + 1, len(lines)):
        # Stop at the next ALL-CAPS section header that ISN'T immediately
        # after the start (the line right after a section header is often
        # an all-caps unit string like "(FEET PER SECOND)" — skip those).
        if _SECTION_HEADER_RE.fullmatch(lines[j]) and j != start + 1:
            end = j
            break
    for j in range(start + 1, end):
        if lines[j] == distance_label and j + 1 < end:
            m = re.search(r'\d+', lines[j + 1])
            if m:
                return int(m.group())
    return None


def parse_product_page(html: str, source_url: str) -> ParsedBallistics:
    lines = _stripped_text_lines(html)

    sku = _value_after_label(lines, re.compile(r'ITEM NO:?', re.IGNORECASE))
    bullet_raw = _value_after_label(lines, re.compile(r'BULLET TYPE:?', re.IGNORECASE))
    weight_raw = _value_after_label(lines, re.compile(r'WEIGHT:?', re.IGNORECASE))

    grain = None
    if weight_raw:
        m = re.search(r'(\d+)', weight_raw)
        if m:
            grain = int(m.group(1))

    # Caliber: try H1 first (works for Bronze pages where H1 is the
    # caliber). Fall back to SKU-prefix derivation (StarFire pages H1
    # with the SKU instead).
    soup = BeautifulSoup(html, 'html.parser')
    h1 = soup.find('h1')
    h1_text = h1.get_text(strip=True) if h1 else None
    caliber = _normalize_caliber(h1_text) or _caliber_from_sku(sku)

    velocity_section_re = re.compile(r'VELOCITY', re.IGNORECASE)
    energy_section_re = re.compile(r'ENERGY', re.IGNORECASE)

    muzzle_velocity = _integer_in_section(lines, velocity_section_re, 'Muzzle:')
    muzzle_energy = _integer_in_section(lines, energy_section_re, 'Muzzle:')
    velocity_50yd = _integer_in_section(lines, velocity_section_re, '50 Yds:')
    velocity_100yd = _integer_in_section(lines, velocity_section_re, '100 Yds:')

    product_line = _product_line_from_url(source_url)
    bullet_resolved = _normalize_bullet_type(bullet_raw)

    external_id = sku or source_url.rstrip('/').rsplit('/', 1)[-1]
    raw_name = ' '.join(filter(None, [h1_text, weight_raw, bullet_raw])) or None

    return ParsedBallistics(
        external_id=external_id,
        source_url=source_url,
        sku=sku,
        product_line=product_line,
        caliber_normalized=caliber,
        grain=grain,
        bullet_type=bullet_resolved,
        muzzle_velocity_fps=muzzle_velocity,
        muzzle_energy_ftlb=muzzle_energy,
        velocity_50yd=velocity_50yd,
        velocity_100yd=velocity_100yd,
        raw_name=raw_name,
    )


def upsert_ballistics(supabase, bal: ParsedBallistics, html_hash: str) -> int:
    if bal.muzzle_velocity_fps is None:
        raise ValueError(f'{bal.external_id}: muzzle_velocity_fps is null.')

    now = datetime.now(timezone.utc).isoformat()
    row = {
        'external_id': bal.external_id,
        'source': SOURCE,
        'brand': BRAND,
        'sku': bal.sku,
        'product_line': bal.product_line,
        'caliber_normalized': bal.caliber_normalized,
        'grain': bal.grain,
        'bullet_type': bal.bullet_type,
        'muzzle_velocity_fps': bal.muzzle_velocity_fps,
        'muzzle_energy_ftlb': bal.muzzle_energy_ftlb,
        'velocity_50yd': bal.velocity_50yd,
        'velocity_100yd': bal.velocity_100yd,
        'source_url': bal.source_url,
        'last_seen_at': now,
        'last_scraped_at': now,
        'raw_html_hash': html_hash,
    }
    res = (
        supabase.table('manufacturer_ballistics')
        .upsert(row, on_conflict='source,external_id')
        .execute()
    )
    return res.data[0]['id']


def scrape_source(source: str, dry_run: bool, supabase=None) -> int:
    cfg = SOURCES[source]
    print(f'\n=== {source} ({cfg["brand"]}) ===')
    saved = 0
    for url in cfg['seed_urls']:
        try:
            html = fetch(url)
        except Exception as e:
            print(f'\n  FETCH FAILED {url}: {e}')
            continue
        html_hash = hashlib.sha256(html.encode('utf-8')).hexdigest()
        bal = parse_product_page(html, url)

        print(f'\n  [{bal.external_id}] {bal.product_line or ""}')
        print(f'    url: {url}')
        print(f'    raw name: {bal.raw_name!r}')
        print(f'    caliber={bal.caliber_normalized!r}  grain={bal.grain}  '
              f'bullet={bal.bullet_type!r}  sku={bal.sku!r}')
        print(f'    muzzle_velocity={bal.muzzle_velocity_fps} fps  '
              f'muzzle_energy={bal.muzzle_energy_ftlb} ft-lb')
        print(f'    velocity_50yd={bal.velocity_50yd}  velocity_100yd={bal.velocity_100yd}')

        if dry_run:
            saved += 1
            continue
        try:
            upsert_ballistics(supabase, bal, html_hash)
            saved += 1
        except Exception as e:
            print(f'    UPSERT FAILED: {e}')
    return saved


def main() -> int:
    parser = argparse.ArgumentParser(description='Scrape PMC product-page ballistics.')
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
    print(f'\nDone ({mode}). {total} product(s) {"would be " if args.dry_run else ""}upserted.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
