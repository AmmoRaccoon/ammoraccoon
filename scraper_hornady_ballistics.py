"""scraper_hornady_ballistics.py — manufacturer ballistics scraper for Hornady.

Hornady publishes ballistics on per-product pages at /ammunition/<category>/<slug>.
Pages are static HTML (no Playwright needed) — the spec section is rendered
client-side via Angular and is not usable, but the ballistics block IS in the
initial HTML inside <div data-label="Muzzle Velocity">N</div> divs (alongside
50/100-yard velocity and energy). Caliber, grain, bullet type, and product
line are encoded in the H1 itemtitle (e.g. "9mm Luger 115 gr FTX(R) Critical
Defense(R)"). The SKU lives in <span class="stats">Item #90250 | 25/Box</span>.

Hornady has no JSON-LD product blob, so the H1 + stats span + data-label divs
are the only static, reliable signals.

Writes to:
  manufacturer_ballistics  (one row per product page)

Required env:
  SUPABASE_URL, SUPABASE_KEY

Usage:
  python scraper_hornady_ballistics.py --dry-run
  python scraper_hornady_ballistics.py --source hornady_handgun_9mm
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

SOURCE = 'hornady'
BRAND = 'Hornady'

# Hornady caliber names (as they appear in the H1) -> caliber_normalized.
CALIBER_NORMALIZE = {
    '9mm luger': '9mm', '9mm': '9mm',
    '380 auto': '380acp', '.380 auto': '380acp', '380 acp': '380acp',
    '38 special': '38spl', '.38 special': '38spl',
    '357 mag': '357mag', '.357 mag': '357mag', '357 magnum': '357mag',
    '40 s&w': '40sw', '.40 s&w': '40sw',
    '22 lr': '22lr', '.22 lr': '22lr', '22 long rifle': '22lr',
    '223 rem': '223-556', '.223 rem': '223-556', '223 remington': '223-556',
    '5.56 nato': '223-556', '5.56x45 nato': '223-556', '5.56x45mm nato': '223-556',
    '308 win': '308win', '.308 win': '308win', '308 winchester': '308win',
    '7.62x39': '762x39', '7.62x39mm': '762x39',
    '300 blk': '300blk', '.300 blk': '300blk', '300 blackout': '300blk',
}

# Phrase-based bullet lookup, mirrors scraper_kinetic_ballistics.py — see
# the "hollow point" -> JHP rationale there.
_BULLET_TYPE_LOOKUP = [
    ('jacketed hollow point', 'JHP'),
    ('total metal jacket', 'TMJ'),
    ('full metal jacket', 'FMJ'),
    ('open tip match', 'OTM'),
    ('hollow point', 'JHP'),
    ('soft point', 'SP'),
    ('round nose', 'LRN'),
    ('flat point', 'FP'),
]

# Hornady-proprietary bullet codes that appear as a single token in the
# product title. All of FTX/XTP/FlexLock/HAP/MonoFlex/TAP/DGH expand on
# impact and retailers tag them as JHP or HP when "Hollow Point" or the
# proprietary name maps that way in their parsers, so collapsing all of
# them to JHP keeps the matcher's 4-column equi-join productive.
_HORNADY_BULLET_CODES = {
    'ftx': 'JHP',       # Flex Tip eXpanding (Critical Defense)
    'xtp': 'JHP',       # eXtreme Terminal Performance (Custom, Subsonic, Black, ...)
    'flexlock': 'JHP',  # FlexLock (Critical Duty)
    'hap': 'JHP',       # Hornady Action Pistol
    'monoflex': 'JHP',  # MonoFlex (Handgun Hunter)
    'tap': 'JHP',       # TAP (FPD)
    'dgh': 'JHP',       # Dangerous Game Handgun (Backcountry Defense)
    'fmj': 'FMJ',
    'jhp': 'JHP',
    'hp': 'JHP',
    'sp': 'SP',
}

SOURCES = {
    'hornady_handgun_9mm': {
        'brand': BRAND,
        # Seed URLs for dev/dry-run. Discovery via the public sitemap
        # (/sitemap.xml exposes /ammunition/handgun/9mm-luger-* slugs)
        # is a separate follow-up.
        'seed_urls': [
            'https://www.hornady.com/ammunition/handgun/9mm-luger-115-gr-ftx-critical-defense',
            'https://www.hornady.com/ammunition/handgun/9mm-luger-115-gr-xtp-american-gunner',
            'https://www.hornady.com/ammunition/handgun/9mm-luger-124-gr-xtp',
            'https://www.hornady.com/ammunition/handgun/9mm-luger-147-gr-xtp-subsonic',
            'https://www.hornady.com/ammunition/handgun/9mm-135-gr-flexlock-critical-duty',
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
    if low in _HORNADY_BULLET_CODES:
        return _HORNADY_BULLET_CODES[low]
    for needle, code in _BULLET_TYPE_LOOKUP:
        if needle in low:
            return code
    short = text.strip().upper()
    if 2 <= len(short) <= 4 and short.isalpha():
        return short
    return None


def _label_int(soup, label: str) -> Optional[int]:
    """Integer text from <div data-label="LABEL">N</div>, or None."""
    div = soup.find('div', attrs={'data-label': label})
    if not div:
        return None
    m = re.search(r'\d+', div.get_text())
    return int(m.group()) if m else None


def parse_product_page(html: str, source_url: str) -> ParsedBallistics:
    soup = BeautifulSoup(html, 'html.parser')

    # H1 itemtitle: "9mm Luger 115 gr FTX(R) Critical Defense(R)"
    h1 = soup.find('h1', class_='itemtitle')
    raw_name = h1.get_text(' ', strip=True) if h1 else ''
    # Strip (R), TM, (C) trademark glyphs so the regex can match cleanly.
    name = re.sub(r'[®™©]', '', raw_name).strip()
    name = re.sub(r'\s+', ' ', name)

    # SKU from <span class="stats">Item #90250 | 25/Box ...</span>.
    sku = None
    stats = soup.find('span', class_='stats')
    if stats:
        m = re.search(r'item\s*#\s*(\S+)', stats.get_text(' ', strip=True), re.IGNORECASE)
        if m:
            sku = m.group(1)

    # Title shape: "<caliber> <grain> gr <bullet> [<product_line>]".
    caliber_text = grain = bullet_text = product_line = None
    m = re.match(r'^(.+?)\s+(\d+)\s+gr\s+(\S+)(?:\s+(.+))?$', name, re.IGNORECASE)
    if m:
        caliber_text = m.group(1).strip()
        grain = int(m.group(2))
        bullet_text = m.group(3).strip()
        product_line = m.group(4).strip() if m.group(4) else None

    muzzle_velocity = _label_int(soup, 'Muzzle Velocity')
    muzzle_energy = _label_int(soup, 'Muzzle Energy')
    velocity_50yd = _label_int(soup, '50 YD Velocity')
    velocity_100yd = _label_int(soup, '100 YD Velocity')

    external_id = sku or source_url.rstrip('/').rsplit('/', 1)[-1]

    return ParsedBallistics(
        external_id=external_id,
        source_url=source_url,
        sku=sku,
        product_line=product_line,
        caliber_normalized=_normalize_caliber(caliber_text),
        grain=grain,
        bullet_type=_normalize_bullet_type(bullet_text),
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

        print(f'\n  [{bal.external_id}] {bal.product_line or "?"}')
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
    parser = argparse.ArgumentParser(description='Scrape Hornady product-page ballistics.')
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
