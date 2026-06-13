"""scraper_sb_ballistics.py — manufacturer ballistics scraper for Sellier & Bellot.

S&B publishes US-spec ballistics on its US distributor site at
sellierbellot.us (NOT the Czech parent site sellier-bellot.cz that the
2026-05-14 9mm coverage audit inferred — see web repo TASKS.md "Audit
URL pattern bug"). Pages are server-rendered static HTML. Custom CMS;
no SFCC backend, no JSON-LD Product, no chart-data-velocity scripts.

The page H1 carries caliber, bullet type, grain, and SKU in one
structured string, e.g.:
  "9 mm LUGER / 9 mm PARA / 9 × 19 FMJ 115 GRS SB9A"
or for subsonic loads:
  "9 mm LUGER SUBSONIC / 9 × 19 FMJ 140 GRS SB9SUBA"

Ballistics live in a labeled <table class="large"> with two data rows
(Velocity in fps, Energy in ft·lb) and four distance columns (Muzzle /
25 / 50 / 100 yards). Imperial units native — no metric conversion
needed. Cell values use a comma thousands separator (e.g. "1,280")
which the parser strips before int().

URL convention is opaque numeric detail IDs:
  /products/pistol-and-revolver-ammunition/pistol-and-revolver-cartridges/detail/<N>/
Each SKU has a unique N; there is no SKU-to-N derivation rule. New
SKUs need manual ID lookup from the /list/ category page.

Writes to:
  manufacturer_ballistics  (one row per product page)

Required env:
  SUPABASE_URL, SUPABASE_KEY  (the service-role key)

Usage:
  python scripts/scraper_sb_ballistics.py --dry-run
  python scripts/scraper_sb_ballistics.py --source sb_handgun_9mm
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

SOURCE = 'sb'
BRAND = 'Sellier & Bellot'

# Caliber-name → caliber_normalized. S&B H1 uses lowercase-spaced format
# like "9 mm LUGER"; the lookup keys are lowercase, whitespace-normalized.
# Phase B step 4 (2026-06-12): re-exported from the shared union map in
# caliber_registry_gen (BALLISTICS_CALIBER_NORMALIZE, emitted from
# calibers.json — which includes S&B's '9 mm luger' spacing variant). D2: the
# union is a deliberate SUPERSET of the old per-source maps; the fresh live
# replay showed S&B's crawl gains no rows and changes/removes nothing
# (scripts/_replay_ballistics_maps.py). This module runs as
# `python scripts/scraper_sb_ballistics.py`, so add the repo root (where
# caliber_registry_gen.py lives) to sys.path before importing it.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from caliber_registry_gen import BALLISTICS_CALIBER_NORMALIZE as CALIBER_NORMALIZE  # noqa: E402

# Bullet-type lookup, mirrors the other ballistics scrapers.
_BULLET_TYPE_LOOKUP = [
    ('jacketed hollow point', 'JHP'),
    ('total metal jacket', 'TMJ'),
    ('full metal jacket', 'FMJ'),
    ('jacketed soft point', 'JSP'),
    ('hollow point', 'JHP'),
    ('soft point', 'SP'),
    ('round nose', 'LRN'),
    ('flat point', 'FP'),
]

# S&B H1 emits bullet-type codes directly; pass-through.
_SB_BULLET_CODES = {
    'fmj': 'FMJ', 'jhp': 'JHP', 'jsp': 'JSP', 'tmj': 'TMJ',
    'sp':  'SP',  'hp':  'JHP', 'lrn': 'LRN', 'fp': 'FP',
    'sjsp': 'JSP', 'sjfp': 'FP',  # S&B semi-jacketed variants
}

SOURCES = {
    'sb_handgun_9mm': {
        'brand': BRAND,
        # 5 9mm audit-row URLs, verified live 2026-05-17. Numeric IDs are
        # opaque — derived from the sellierbellot.us /list/ category page,
        # cross-referenced with retailer-known SKU names (SB9A/B/C/D/SUBA).
        'seed_urls': [
            'https://www.sellierbellot.us/products/pistol-and-revolver-ammunition/pistol-and-revolver-cartridges/detail/290/',   # SB9A 115gr FMJ (audit row #8)
            'https://www.sellierbellot.us/products/pistol-and-revolver-ammunition/pistol-and-revolver-cartridges/detail/288/',   # SB9C 115gr JHP (audit row #10)
            'https://www.sellierbellot.us/products/pistol-and-revolver-ammunition/pistol-and-revolver-cartridges/detail/289/',   # SB9B 124gr FMJ (audit row #13)
            'https://www.sellierbellot.us/products/pistol-and-revolver-ammunition/pistol-and-revolver-cartridges/detail/112/',   # SB9D 124gr JHP (audit row #70)
            'https://www.sellierbellot.us/products/pistol-and-revolver-ammunition/pistol-and-revolver-cartridges/detail/284/',   # SB9SUBA 140gr FMJ subsonic (audit row #59)
        ],
    },
    'sb_handgun_40sw': {
        'brand': BRAND,
        # .40 S&W (2026-05-28): full S&B US .40 handgun catalog, all Tier 1 (single
        # convergent line per grain+bullet). H1 is clean ("40 S&W FMJ 180 GRS SB40B"),
        # so caliber resolves 40sw + bullet FMJ/JHP. Opaque numeric detail IDs
        # (no SKU->ID rule); IDs verified live 2026-05-28. SB40G 165 = 0 current (future-proof).
        'seed_urls': [
            'https://www.sellierbellot.us/products/pistol-and-revolver-ammunition/pistol-and-revolver-cartridges/detail/270/',   # SB40B 180gr FMJ, 968 fps
            'https://www.sellierbellot.us/products/pistol-and-revolver-ammunition/pistol-and-revolver-cartridges/detail/304/',   # SB40C 180gr JHP, 974 fps
            'https://www.sellierbellot.us/products/pistol-and-revolver-ammunition/pistol-and-revolver-cartridges/detail/488/',   # SB40G 165gr FMJ, 1027 fps (0 current, future-proof)
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
    'Accept-Language': 'en-US,en;q=0.9',
})


def fetch(url: str) -> str:
    resp = _session.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


def _normalize_caliber(caliber_text: Optional[str]) -> Optional[str]:
    """Map S&B's "9 mm LUGER / 9 mm PARA / 9 × 19" or "9 mm LUGER
    SUBSONIC / 9 × 19" caliber string to our normalized code. Strips
    the "SUBSONIC" keyword and anything after the first "/" alias-chain
    separator before lookup."""
    if not caliber_text:
        return None
    # Take the primary name before any "/" alias or SUBSONIC keyword.
    primary = re.split(r'\s*/\s*|\s+SUBSONIC\b', caliber_text, maxsplit=1)[0]
    primary = re.sub(r'\s+', ' ', primary).strip().lower()
    return CALIBER_NORMALIZE.get(primary)


def _normalize_bullet_type(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    low = text.lower().strip()
    if low in _SB_BULLET_CODES:
        return _SB_BULLET_CODES[low]
    for needle, code in _BULLET_TYPE_LOOKUP:
        if needle in low:
            return code
    short = text.strip().upper()
    if 2 <= len(short) <= 5 and short.isalpha():
        return short
    return None


def _parse_h1(name: str) -> tuple:
    """Extract (caliber_text, grain, bullet_token, sku) from S&B's H1.

    Examples:
      "9 mm LUGER / 9 mm PARA / 9 × 19 FMJ 115 GRS SB9A"
        → ("9 mm LUGER / 9 mm PARA / 9 × 19", 115, "FMJ", "SB9A")
      "9 mm LUGER SUBSONIC / 9 × 19 FMJ 140 GRS SB9SUBA"
        → ("9 mm LUGER SUBSONIC / 9 × 19", 140, "FMJ", "SB9SUBA")
    """
    norm = re.sub(r'\s+', ' ', name).strip()
    # Tail: "<grain> GRS <sku>"
    m = re.search(r'^(?P<prefix>.+?)\s+(?P<grain>\d+)\s*GRS\s+(?P<sku>\S+)\s*$', norm)
    if not m:
        return None, None, None, None
    prefix = m.group('prefix').strip()
    grain = int(m.group('grain'))
    sku = m.group('sku').strip()
    # Bullet token at end of prefix — 2-5 uppercase letters.
    bm = re.search(r'\b([A-Z]{2,5})\s*$', prefix)
    if not bm:
        return prefix, grain, None, sku
    bullet = bm.group(1)
    caliber_text = prefix[:bm.start()].strip()
    return caliber_text, grain, bullet, sku


def _table_int(soup, row_label: str, col_idx: int) -> Optional[int]:
    """Pull an integer from S&B's <table class="large"> ballistics table.

    row_label : 'Velocity' or 'Energy' (matched against the row's first <th>).
    col_idx   : 0=Muzzle, 1=25y, 2=50y, 3=100y (zero-indexed into <td> cells
                after the two leading <th> cells).
    """
    table = soup.find('table', class_='large')
    if not table:
        return None
    for tr in table.find_all('tr'):
        ths = tr.find_all('th')
        if not ths:
            continue
        if ths[0].get_text(strip=True) == row_label:
            tds = tr.find_all('td')
            if col_idx >= len(tds):
                return None
            cell = tds[col_idx].get_text(strip=True).replace(',', '')
            m = re.search(r'\d+', cell)
            return int(m.group()) if m else None
    return None


def parse_product_page(html: str, source_url: str) -> ParsedBallistics:
    soup = BeautifulSoup(html, 'html.parser')

    h1 = soup.find('h1')
    raw_name = h1.get_text(' ', strip=True) if h1 else ''
    raw_name = re.sub(r'\s+', ' ', raw_name).strip()

    caliber_text, grain, bullet_token, sku = _parse_h1(raw_name)

    muzzle_velocity = _table_int(soup, 'Velocity', 0)
    muzzle_energy = _table_int(soup, 'Energy', 0)
    velocity_50yd = _table_int(soup, 'Velocity', 2)
    velocity_100yd = _table_int(soup, 'Velocity', 3)

    external_id = sku or source_url.rstrip('/').rsplit('/', 1)[-1]

    return ParsedBallistics(
        external_id=external_id,
        source_url=source_url,
        sku=sku,
        product_line=None,  # S&B doesn't name product lines on individual SKU pages.
        caliber_normalized=_normalize_caliber(caliber_text),
        grain=grain,
        bullet_type=_normalize_bullet_type(bullet_token),
        muzzle_velocity_fps=muzzle_velocity,
        muzzle_energy_ftlb=muzzle_energy,
        velocity_50yd=velocity_50yd,
        velocity_100yd=velocity_100yd,
        raw_name=raw_name or None,
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

        print(f'\n  [{bal.external_id}]')
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
    parser = argparse.ArgumentParser(description='Scrape Sellier & Bellot US product-page ballistics.')
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
