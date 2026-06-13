"""scraper_magtech_ballistics.py — manufacturer ballistics scraper for Magtech.

Magtech publishes ballistics on per-product pages at
/products/ammunition/<descriptive-slug>/ (NOT /<caliber>/<sku>/ as the
2026-05-14 9mm coverage audit inferred — see web repo TASKS.md "Audit URL
pattern bug" for the broader caveat). Pages are server-rendered static
HTML; no Playwright needed.

The Symbol (MP9A, MP9B, MP9C, etc.) and bullet metadata live in a
<ul class="list"> spec block. The muzzle / 50yd / 100yd ballistics live
in <table id="table-imperial" class="table-imperial active first">.
JSON-LD on the page is SEO-only (WebPage, BreadcrumbList, WebSite) — no
@type:Product blob — so all extraction is from the visible HTML.

Note on local TLS: Windows curl with Schannel rejects the Magtech cert
chain with SEC_E_WRONG_PRINCIPAL. Python `requests` uses OpenSSL and is
unaffected; this scraper runs cleanly on Backman's venv and on Linux CI.

Writes to:
  manufacturer_ballistics  (one row per product page)

Required env:
  SUPABASE_URL, SUPABASE_KEY  (the service-role key)

Usage:
  python scripts/scraper_magtech_ballistics.py --dry-run
  python scripts/scraper_magtech_ballistics.py --source magtech_handgun_9mm
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

SOURCE = 'magtech'
BRAND = 'Magtech'

# Caliber-name → caliber_normalized.
# Phase B step 4 (2026-06-12): re-exported from the shared union map in
# caliber_registry_gen (BALLISTICS_CALIBER_NORMALIZE, emitted from
# calibers.json — including the 44mag ballistics-only aliases this source
# carried). D2: the union is a deliberate SUPERSET of the old per-source maps;
# the fresh live replay showed Magtech's crawl gains no rows and
# changes/removes nothing (scripts/_replay_ballistics_maps.py). This module
# runs as `python scripts/scraper_magtech_ballistics.py`, so add the repo root
# (where caliber_registry_gen.py lives) to sys.path before importing it.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from caliber_registry_gen import BALLISTICS_CALIBER_NORMALIZE as CALIBER_NORMALIZE  # noqa: E402

# Phrase-based bullet lookup, mirrors scraper_hornady_ballistics.py — see
# the "hollow point" -> JHP rationale there. Sorted longest-needle-first
# so the most specific phrase wins.
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

# Magtech spec list emits bullet-type codes directly; pass-through for
# already-canonical ones. HP collapses to JHP to align with retailers'
# bullet_type convention on the listings table.
_MAGTECH_BULLET_CODES = {
    'fmj': 'FMJ', 'jhp': 'JHP', 'jsp': 'JSP', 'tmj': 'TMJ',
    'sp': 'SP',   'hp': 'JHP', 'lrn': 'LRN', 'fp': 'FP',
}

SOURCES = {
    'magtech_handgun_9mm': {
        'brand': BRAND,
        # 9A — confirmed during 2026-05-17 inspection probe.
        # 9B and 9C URLs are inferred from the related-products listing
        # on the 9A page; verified empirically in the dry-run pass.
        'seed_urls': [
            'https://magtechammunition.com/products/ammunition/9mm-luger-115gr-fmj/',
            'https://magtechammunition.com/products/ammunition/9mm-luger-115gr-jhp/',
            'https://magtechammunition.com/products/ammunition/9mm-luger-124gr-fmj/',
        ],
    },
    'magtech_handgun_40sw': {
        'brand': BRAND,
        # .40 S&W (2026-05-28): each (grain,bullet) is a single convergent-velocity line
        # (180 JHP standard + Guardian Gold both 990, zero spread) -> all three Tier 1.
        # NOTE: site spec "Bullet Type" reads "FMJ Flat" which the normalizer nulls; the
        # H1-title fallback rescues it to FMJ (must be proven at dry-run before ingest).
        'seed_urls': [
            'https://magtechammunition.com/products/ammunition/40-sw-180gr-fmj-flat/',  # 180gr FMJ, 990 fps (40B)
            'https://magtechammunition.com/products/ammunition/40-sw-165gr-fmj-flat/',  # 165gr FMJ, 1050 fps (40G)
            'https://magtechammunition.com/products/ammunition/40-sw-180gr-jhp/',       # 180gr JHP, 990 fps (40A)
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
    # Drop "+P" / " +P" load suffix; caliber_normalized doesn't distinguish +P.
    key = re.sub(r'\s*\+\s*p\b', '', s.lower()).strip()
    key = re.sub(r'\s+', ' ', key)
    return CALIBER_NORMALIZE.get(key)


def _normalize_bullet_type(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    low = text.lower().strip()
    if low in _MAGTECH_BULLET_CODES:
        return _MAGTECH_BULLET_CODES[low]
    for needle, code in _BULLET_TYPE_LOOKUP:
        if needle in low:
            return code
    short = text.strip().upper()
    if 2 <= len(short) <= 4 and short.isalpha():
        return short
    return None


def _spec_list_value(soup, label: str) -> Optional[str]:
    """Pull a value cell from <li class="item"><strong>LABEL: </strong>
    <span>VALUE</span></li> in the #specifications section. Case-insensitive
    label match; trailing colon and whitespace tolerated."""
    label_norm = label.lower().rstrip(':').strip()
    for li in soup.select('section#specifications li.item'):
        strong = li.find('strong')
        if not strong:
            continue
        got = strong.get_text(strip=True).lower().rstrip(':').strip()
        if got == label_norm:
            span = li.find('span')
            if span:
                return span.get_text(strip=True)
    return None


def _imperial_row_value(soup, row_label: str, col_index: int) -> Optional[int]:
    """Pull an integer from the imperial ballistics table.

    row_label : 'Muzzle' | '50 Yards' | '100 Yards'
    col_index : 0=Velocity (fps), 1=Energy (ft.lbs), 2=Trajectory (inch)
    """
    # The page has multiple <table id="table-imperial"> elements (the
    # ballistics table + a second one for test-barrel-length). The
    # ballistics one carries class "table-imperial … first". We find
    # the first imperial table that contains a row with the expected
    # 'fps' / 'ft.lbs' unit header to guard against drift.
    candidates = soup.find_all('table', id='table-imperial')
    for table in candidates:
        rows = table.find_all('tr')
        # Look for a row whose cells contain "fps" (the units row) — that
        # marks the imperial ballistics table specifically.
        has_units = any(
            tr.find_all('td')
            and tr.find_all('td')[0].get_text(strip=True).lower() == 'fps'
            for tr in rows
        )
        if not has_units:
            continue
        for tr in rows:
            cells = tr.find_all('td')
            if not cells:
                continue
            if cells[0].get_text(strip=True) == row_label:
                # Cell 0 is the row label; columns start at index 1.
                try:
                    raw = cells[1 + col_index].get_text(strip=True)
                except IndexError:
                    return None
                m = re.search(r'\d+', raw)
                return int(m.group()) if m else None
        return None  # right table but row not found
    return None


def parse_product_page(html: str, source_url: str) -> ParsedBallistics:
    soup = BeautifulSoup(html, 'html.parser')

    # Title from H1, trademark glyphs stripped (defensive; Magtech doesn't
    # currently use them but Hornady does).
    h1 = soup.find('h1')
    raw_name = h1.get_text(' ', strip=True) if h1 else ''
    name = re.sub(r'[®™©]', '', raw_name).strip()
    name = re.sub(r'\s+', ' ', name)

    # Title shape: "<caliber> <grain> GR <bullet> [<tail>]" e.g.
    # "9mm Luger 115GR FMJ" or "9mm Luger+P 124GR JHP Bonded".
    caliber_text = grain = bullet_text = product_line = None
    m = re.match(r'^(.+?)\s+(\d+)\s*GR\s+(\S+)(?:\s+(.+))?$', name, re.IGNORECASE)
    if m:
        caliber_text = m.group(1).strip()
        grain = int(m.group(2))
        bullet_text = m.group(3).strip()
        product_line = m.group(4).strip() if m.group(4) else None

    # Symbol: prefer the spec-list value (canonical form); fall back to
    # the inline <span class="symbol"> next to the H1 if the spec block
    # is absent or shaped differently.
    sku = _spec_list_value(soup, 'Symbol')
    if not sku:
        sym = soup.select_one('span.symbol')
        if sym:
            sku = sym.get_text(strip=True) or None

    # Spec list often has its own Bullet Type field — prefer it over the
    # title-derived bullet_text, since the title abbreviation can mismatch
    # (e.g., "JHP Bonded" tail vs spec "JHP").
    spec_bullet = _spec_list_value(soup, 'Bullet Type')
    bullet_resolved = _normalize_bullet_type(spec_bullet) or _normalize_bullet_type(bullet_text)

    muzzle_velocity = _imperial_row_value(soup, 'Muzzle', 0)
    muzzle_energy = _imperial_row_value(soup, 'Muzzle', 1)
    velocity_50yd = _imperial_row_value(soup, '50 Yards', 0)
    velocity_100yd = _imperial_row_value(soup, '100 Yards', 0)

    external_id = sku or source_url.rstrip('/').rsplit('/', 1)[-1]

    return ParsedBallistics(
        external_id=external_id,
        source_url=source_url,
        sku=sku,
        product_line=product_line,
        caliber_normalized=_normalize_caliber(caliber_text),
        grain=grain,
        bullet_type=bullet_resolved,
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
    parser = argparse.ArgumentParser(description='Scrape Magtech product-page ballistics.')
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
