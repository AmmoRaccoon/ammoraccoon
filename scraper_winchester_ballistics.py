"""scraper_winchester_ballistics.py — manufacturer ballistics scraper for Winchester.

Winchester's catalog uses a different shape from the Kinetic Group SCC sites:
each per-product page redirects to a category-listing page, but the listing
page itself is a goldmine — every product tile carries the full spec set
(caliber, grain, bullet type, muzzle velocity, muzzle energy, SKU, sub-brand)
inline. One fetch per category yields dozens of products, no per-product
detail crawl needed.

Tile shape (verified on /Products/Ammunition/Handgun):
  <div id="<SKU>" class="b-producttile ...">
    ...
    <div class="b-producttile__info">
      <div class="b-producttile__info-item cartridge">       9mm Luger
      <div class="b-producttile__info-item weight">          115 Grain
      <div class="b-producttile__info-item type">            Full Metal Jacket
      <div class="b-producttile__info-item muzzle-velocity"> 1190
      <div class="b-producttile__info-item muzzle-energy">   362
      <div class="b-producttile__info-item count">           50
      <div class="b-producttile__info-item symbol">          <a>Q4172</a>
      <div class="b-producttile__info-item brand">           <a>USA</a>

Writes to:
  manufacturer_ballistics  (one row per product tile)

Required env:
  SUPABASE_URL, SUPABASE_KEY

Usage:
  python scraper_winchester_ballistics.py --dry-run
  python scraper_winchester_ballistics.py --calibers 9mm
"""

import argparse
import hashlib
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin

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

SOURCE = 'winchester'
BRAND = 'Winchester'

# Cartridge-text → caliber_normalized. Winchester's tiles spell the cartridge
# name out long-form ("9mm Luger", "25 Automatic"), so this map is broader
# than Kinetic's because it covers every cartridge family on the catalog page,
# not just the ones we want — products whose cartridge isn't here drop out.
CALIBER_NORMALIZE = {
    '9mm luger': '9mm', '9mm luger +p': '9mm', '9mm nato': '9mm',
    '380 auto': '380acp', '.380 auto': '380acp', '380 acp': '380acp',
    '380 automatic': '380acp',
    '38 special': '38spl', '38 special +p': '38spl',
    '357 magnum': '357mag',
    '40 s&w': '40sw', '40 smith & wesson': '40sw',
    '22 long rifle': '22lr',
    '223 remington': '223-556', '5.56x45mm nato': '223-556', '5.56mm': '223-556',
    '308 winchester': '308win',
    '7.62x39mm': '762x39', '7.62 x 39mm': '762x39',
    '300 blackout': '300blk', '300 aac blackout': '300blk',
}

# Sorted by needle length descending so the most specific phrase wins.
# Mirrors scraper_kinetic_ballistics.py — see comment there for the
# "hollow point" -> JHP rationale.
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

SOURCES = {
    'winchester_handgun': {
        'brand': BRAND,
        'category_url': 'https://winchester.com/Products/Ammunition/Handgun',
    },
    'winchester_rifle': {
        'brand': BRAND,
        'category_url': 'https://winchester.com/Products/Ammunition/Rifle',
    },
    'winchester_rimfire': {
        'brand': BRAND,
        'category_url': 'https://winchester.com/Products/Ammunition/Rimfire',
    },
    # Shotshell uses the same template; not added because we don't track
    # shotshells in listings.
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
    raw_cartridge: Optional[str] = None


def _strip(text: str) -> str:
    return re.sub(r'\s+', ' ', text or '').strip()


def fetch(url: str) -> str:
    resp = requests.get(
        url,
        headers={'User-Agent': USER_AGENT, 'Accept-Encoding': 'gzip'},
        timeout=30,
        allow_redirects=True,
    )
    resp.raise_for_status()
    return resp.text


def _normalize_caliber(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    key = re.sub(r'\s+', ' ', s.lower().strip())
    return CALIBER_NORMALIZE.get(key)


def _normalize_bullet_type(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    low = text.lower()
    for needle, code in _BULLET_TYPE_LOOKUP:
        if needle in low:
            return code
    short = text.strip().upper()
    if 2 <= len(short) <= 4 and short.isalpha():
        return short
    return None


def _info_item_text(info_block, kind_class: str) -> Optional[str]:
    """Return the text content of a <div class="b-producttile__info-item KIND">,
    minus the inner <span class="sr-only"> accessibility label."""
    item = info_block.find(
        'div',
        class_=lambda c: c and 'b-producttile__info-item' in c and kind_class in c,
    )
    if not item:
        return None
    # Drop sr-only label so it doesn't leak into the value.
    for sr in item.find_all('span', class_='sr-only'):
        sr.decompose()
    return _strip(item.get_text(' '))


def parse_category_page(html: str, source_url: str, target_calibers=None) -> list:
    """Extract one ParsedBallistics per product tile.

    target_calibers: optional set of caliber_normalized values to keep
    (e.g. {'9mm'}). Tiles for other calibers are skipped.
    """
    soup = BeautifulSoup(html, 'html.parser')
    out = []

    for info_block in soup.find_all('div', class_='b-producttile__info'):
        cartridge = _info_item_text(info_block, 'cartridge')
        weight_text = _info_item_text(info_block, 'weight')
        bullet_text = _info_item_text(info_block, 'type')
        velocity_text = _info_item_text(info_block, 'muzzle-velocity')
        energy_text = _info_item_text(info_block, 'muzzle-energy')

        # Symbol cell holds an anchor whose text IS the SKU.
        symbol_item = info_block.find(
            'div',
            class_=lambda c: c and 'b-producttile__info-item' in c and 'symbol' in c,
        )
        sku = None
        if symbol_item:
            a = symbol_item.find('a')
            sku = _strip(a.get_text()) if a else None

        # Brand cell — Winchester sub-brand (USA, Super-X, ...). Use as product_line.
        brand_item = info_block.find(
            'div',
            class_=lambda c: c and 'b-producttile__info-item' in c and 'brand' in c,
        )
        sub_brand = None
        if brand_item:
            a = brand_item.find('a')
            sub_brand = _strip(a.get_text()) if a else None

        caliber_norm = _normalize_caliber(cartridge)
        if caliber_norm is None:
            # Cartridge isn't in CALIBER_NORMALIZE — a caliber we don't track
            # (e.g. .270 Win, .243 Win, .30-06 on the rifle page). Drop it
            # before downstream code sees a null join key.
            continue
        if target_calibers and caliber_norm not in target_calibers:
            continue

        grain = None
        if weight_text:
            m = re.search(r'(\d+)', weight_text)
            if m:
                grain = int(m.group(1))

        velocity = int(velocity_text) if velocity_text and velocity_text.isdigit() else None
        energy = int(energy_text) if energy_text and energy_text.isdigit() else None

        if sku is None:
            # Skip rows without a stable identifier — symbol cell is critical.
            continue
        if velocity is None:
            # Velocity is the whole point; skip tiles without it.
            continue

        out.append(ParsedBallistics(
            external_id=sku,
            source_url=source_url,
            sku=sku,
            product_line=sub_brand,
            caliber_normalized=caliber_norm,
            grain=grain,
            bullet_type=_normalize_bullet_type(bullet_text),
            muzzle_velocity_fps=velocity,
            muzzle_energy_ftlb=energy,
            raw_cartridge=cartridge,
        ))

    return out


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


def scrape_source(source: str, target_calibers, dry_run: bool, supabase=None) -> int:
    cfg = SOURCES[source]
    print(f'\n=== {source} ({cfg["brand"]}) ===')
    print(f'  url: {cfg["category_url"]}')

    html = fetch(cfg['category_url'])
    html_hash = hashlib.sha256(html.encode('utf-8')).hexdigest()
    items = parse_category_page(html, cfg['category_url'], target_calibers=target_calibers)
    print(f'  parsed {len(items)} product tile(s)'
          f'{f" (filtered to {sorted(target_calibers)})" if target_calibers else ""}')

    saved = 0
    for bal in items:
        print(f'\n  [{bal.sku}] {bal.product_line or "?"}  cartridge={bal.raw_cartridge!r}')
        print(f'    caliber={bal.caliber_normalized!r}  grain={bal.grain}  '
              f'bullet={bal.bullet_type!r}')
        print(f'    muzzle_velocity={bal.muzzle_velocity_fps} fps  '
              f'muzzle_energy={bal.muzzle_energy_ftlb} ft-lb')

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
    parser = argparse.ArgumentParser(description='Scrape Winchester category-page ballistics.')
    parser.add_argument('--dry-run', action='store_true',
                        help='Parse and print only; no DB writes.')
    parser.add_argument('--source', choices=list(SOURCES.keys()) + ['all'], default='all',
                        help='Which category to scrape. Default: all.')
    parser.add_argument('--calibers', nargs='*',
                        help='Only emit products in these caliber_normalized values '
                             '(e.g. 9mm 380acp). If omitted, all recognized calibers pass through.')
    args = parser.parse_args()

    supabase = None if args.dry_run else create_client(SUPABASE_URL, SUPABASE_KEY)
    sources = list(SOURCES.keys()) if args.source == 'all' else [args.source]
    target_calibers = set(args.calibers) if args.calibers else None

    total = 0
    for s in sources:
        try:
            total += scrape_source(s, target_calibers, args.dry_run, supabase=supabase)
        except Exception as e:
            print(f'  source {s} FAILED: {e}')

    mode = 'DRY RUN' if args.dry_run else 'LIVE'
    print(f'\nDone ({mode}). {total} product(s) {"would be " if args.dry_run else ""}upserted.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
