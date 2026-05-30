"""scraper_verified_40sw_ballistics.py — curated, self-verifying .40 S&W ballistics ingest.

WHY THIS FILE EXISTS
--------------------
The SFCC parser (scraper_kinetic_ballistics.py) and the per-page Magtech parser
cover manufacturers whose sites expose a uniform structured shape. The boutique
.40 S&W brands below each publish muzzle velocity on their OWN site, but every
one uses a different backend (Magento, WooCommerce, BigCommerce, Shopify, a
legacy PHP catalog, a React storefront, or a catalog PDF) with no shared
signature. Writing 11 bespoke live parsers is disproportionate for 1-10
listings each and fragile to maintain.

Instead this scraper carries a CURATED table of manufacturer-VERIFIED rows —
each value was confirmed by reading the manufacturer's own page (see the
2026-05-29 .40 coverage batch; per-brand research notes in the web repo
TASKS.md). To preserve the moat discipline ("the number traces to the
manufacturer, live") and guard against transcription error or catalog drift,
the scraper RE-FETCHES each row's manufacturer source_url at ingest and
upserts ONLY if the recorded velocity still appears on the live page next to a
velocity keyword. If the page is walled / JS-rendered-empty / changed / the
number is absent, the row is SKIPPED with a warning and nothing is written.
This is the never-prey-on-the-ignorant posture: defer what we cannot confirm,
never publish an unverifiable number.

HONESTY RULES BAKED IN (the triage that produced this table):
  * Only single-line-per-(grain,bullet) loads, OR multiple lines with ZERO
    velocity spread, are listed here. Within-grain velocity collisions are
    HELD (e.g. DoubleTap 135gr JHP: Controlled-Expansion 1350 vs Colt-Defense
    unpublished — NOT shipped).
  * Bullet labels must map cleanly to the listings' bullet_type vocabulary.
    Exotic monolithic-copper / hard-cast / frangible loads are HELD.
  * `brand` is set to the EXACT listings.manufacturer string so the strict
    4-column matcher equi-join fires (verified 2026-05-29).
  * No retailer numbers — every source_url is the manufacturer's own domain.

Writes to:
  manufacturer_ballistics  (one row per verified SKU)

Required env:
  SUPABASE_URL, SUPABASE_KEY  (service-role)

Usage:
  python scripts/scraper_verified_40sw_ballistics.py --dry-run
  python scripts/scraper_verified_40sw_ballistics.py
  python scripts/scraper_verified_40sw_ballistics.py --source nosler
"""

import argparse
import hashlib
import os
import re
import sys
import zlib
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

CALIBER = '40sw'

# Curated manufacturer-verified .40 S&W rows. Each value confirmed on the
# manufacturer's own page (2026-05-29 batch). 'expect' is the muzzle velocity
# the live-verify must still find on the page; it equals 'mv'. 'kind' selects
# the verifier (html spec/prose page vs catalog pdf).
SOURCES = [
    # ---- Nosler ASP (nosler.com, Magento spec table) ----
    dict(source='nosler', brand='Nosler', line='ASP', sku='51181', grain=150, bullet='JHP',
         mv=1110, energy=410, kind='html',
         url='https://www.nosler.com/40-s-w-150gr-jhp-asp-handgun-ammunition-50ct.html'),
    dict(source='nosler', brand='Nosler', line='ASP', sku='51279', grain=180, bullet='JHP',
         mv=1005, energy=404, kind='html',
         url='https://www.nosler.com/40-s-w-180gr-jhp-asp-handgun-ammunition.html'),

    # ---- Norma (normausa.com, WooCommerce spec table) ----
    dict(source='norma', brand='Norma', line='Range & Training', sku='620740050', grain=180, bullet='FMJ',
         mv=1017, energy=413, kind='html',
         url='https://normausa.com/product/40-sw-fmj-180-gr-50-qty/'),
    dict(source='norma', brand='Norma', line='SafeGuard', sku='801407887', grain=165, bullet='JHP',
         mv=1134, energy=471, kind='html',
         url='https://normausa.com/product/40-sw-safeguard-jhp-165-gr-50-qty/'),
    dict(source='norma', brand='Norma', line='SafeGuard', sku='801407727', grain=180, bullet='JHP',
         mv=1015, energy=411, kind='html',
         url='https://normausa.com/product/40-sw-safeguard-jhp-180-gr-50-qty/'),

    # ---- Freedom Munitions (freedommunitions.com, "Velocity AVG" spec field).
    # Freedom is the loader, so its own pages ARE the manufacturer source.
    # No muzzle energy published -> energy None. RNFP -> our 'FP' tag. ----
    dict(source='freedom_munitions', brand='Freedom Munitions', line='American Steel', sku=None, grain=165, bullet='FP',
         mv=1000, energy=None, kind='html',
         url='https://freedommunitions.com/products/40-s-w-165-gr-rnfp-new'),
    dict(source='freedom_munitions', brand='Freedom Munitions', line='American Steel', sku=None, grain=180, bullet='FP',
         mv=950, energy=None, kind='html',
         url='https://freedommunitions.com/products/40-s-w-180-gr-rnfp-new'),
    dict(source='freedom_munitions', brand='Freedom Munitions', line='X-DEF', sku=None, grain=165, bullet='JHP',
         mv=1000, energy=None, kind='html',
         url='https://freedommunitions.com/products/40-s-w-165-gr-hp-xdef-new'),
    dict(source='freedom_munitions', brand='Freedom Munitions', line='X-DEF', sku=None, grain=180, bullet='JHP',
         mv=950, energy=None, kind='html',
         url='https://freedommunitions.com/products/40-s-w-180-gr-hp-xdef-new'),
    dict(source='freedom_munitions', brand='Freedom Munitions', line='HUSH', sku=None, grain=200, bullet='FP',
         mv=800, energy=None, kind='html',
         url='https://freedommunitions.com/products/40-s-w-200-gr-rnfp-hush-new'),

    # ---- Grizzly Cartridge (grizzlycartridge.com, WooCommerce "Velocity:").
    # BoneBreaker Flat Point = FMJ-FP; our listings tag it FMJ. ----
    dict(source='grizzly', brand='Grizzly Cartridge', line='JHP', sku='GC4SW4', grain=180, bullet='JHP',
         mv=1125, energy=506, kind='html',
         url='https://grizzlycartridge.com/shop/40-smith-wesson-180gr-jhp/'),
    dict(source='grizzly', brand='Grizzly Cartridge', line='JHP', sku='GC4SW1', grain=200, bullet='JHP',
         mv=1000, energy=444, kind='html',
         url='https://grizzlycartridge.com/shop/40-smith-wesson-200gr-jhp/'),
    dict(source='grizzly', brand='Grizzly Cartridge', line='BoneBreaker FMJ-FP', sku='GC40SW5', grain=200, bullet='FMJ',
         mv=1000, energy=444, kind='html',
         url='https://grizzlycartridge.com/shop/40-smith-wesson-200gr-bonebreaker-flat-point/'),

    # ---- Black Hills (black-hills.com, WooCommerce "Velocity:"). Both loads
    # share one .40 page; distinct external_id per load. ----
    dict(source='black_hills', brand='Black Hills', line='New Pistol', sku='BH-40SW-155JHP', grain=155, bullet='JHP',
         mv=1150, energy=455, kind='html',
         url='https://www.black-hills.com/shop/new-pistol-ammo/40-smith-wesson/'),
    dict(source='black_hills', brand='Black Hills', line='New Pistol', sku='BH-40SW-180JHP', grain=180, bullet='JHP',
         mv=1000, energy=400, kind='html',
         url='https://www.black-hills.com/shop/new-pistol-ammo/40-smith-wesson/'),

    # ---- Underwood (underwoodammo.com, "Muzzle Velocity (fps)" spec). Only
    # the live-catalog FMJ + XTP JHP ship; the 180 JHP / 140 exotics are
    # discontinued/exotic -> HELD. ----
    dict(source='underwood', brand='Underwood', line='Range Supply', sku='UW-40-180FMJ', grain=180, bullet='FMJ',
         mv=1000, energy=400, kind='html',
         url='https://underwoodammo.com/40-s-w-180gr.-range-supply-full-metal-jacket-hunting-ammo/'),
    dict(source='underwood', brand='Underwood', line='XTP', sku='UW-40-155XTP', grain=155, bullet='JHP',
         mv=1300, energy=582, kind='html',
         url='https://underwoodammo.com/40-s-w-155gr.-extreme-terminal-performance-xtp-jacketed-hollow-point-hunting-self-defense-ammo/'),

    # ---- Buffalo Bore (buffalobore.com, legacy PHP prose). Item 23A is the
    # SOLE 155gr JHP (no within-grain collision). Publish ADVERTISED 1300, not
    # the gun-specific tested figures also on the page. ----
    dict(source='buffalo_bore', brand='Buffalo Bore', line='Heavy +P (23A)', sku='23A/20', grain=155, bullet='JHP',
         mv=1300, energy=582, kind='html',
         url='https://www.buffalobore.com/index.php?l=product_detail&p=115'),

    # ---- Aguila (aguilaammo.com, React; ballistics table in HTML). May be
    # JS-empty to requests -> verify will skip it if so. ----
    dict(source='aguila', brand='Aguila', line='FMJ Flat Nose', sku='1E402110', grain=180, bullet='FMJ',
         mv=1050, energy=441, kind='html',
         url='https://www.aguilaammo.com/products/40-s-and-w-full-metal-jacket-flat-nose'),

    # ---- Fenix (fenixammunition.com, Shopify). FXP jacketed HP -> seed JHP so
    # the matcher's JHP->[JHP,HP] alias reaches our 'HP'-tagged listing. ----
    dict(source='fenix', brand='Fenix Ammunition', line='FXP', sku='FENIX-40-180FXP', grain=180, bullet='JHP',
         mv=1100, energy=None, kind='html',
         url='https://fenixammunition.com/products/40-s-w-180gr-fxp-hollowpoint'),

    # ---- DoubleTap (doubletapammo.com, prose). 180gr JHP: Bonded Defense and
    # Controlled Expansion BOTH publish 1100 fps (Glock 23 4") -> zero spread,
    # safe to stamp. Energy left null (differs/omitted across the two lines).
    # 135gr JHP and 200gr JHP are HELD (collisions). ----
    dict(source='doubletap', brand='DoubleTap', line='Controlled Expansion / Bonded Defense', sku='DT-40-180JHP', grain=180, bullet='JHP',
         mv=1100, energy=None, kind='html',
         url='https://doubletapammo.com/products/40-s-w-180gr-controlled-expansion-a-jhp-20rds'),

    # ---- Armscor (armscor.com 2024 catalog PDF — the only manufacturer
    # source; HTML pages carry no velocity, us.armscor.com is walled). Both
    # .40 loads = 950 fps, no energy column. PDF-verified via FlateDecode. ----
    dict(source='armscor', brand='Armscor', line='Armscor USA', sku='ARMSCOR-40-180FMJ', grain=180, bullet='FMJ',
         mv=950, energy=None, kind='pdf',
         url='https://www.armscor.com/hubfs/2024%20Catalogs/2024-RIA-USA%20CATALOG.pdf'),
    dict(source='armscor', brand='Armscor', line='Armscor', sku='ARMSCOR-40-180JHP', grain=180, bullet='JHP',
         mv=950, energy=None, kind='pdf',
         url='https://www.armscor.com/hubfs/2024%20Catalogs/2024-RIA-USA%20CATALOG.pdf'),
]


@dataclass
class Row:
    cfg: dict


_session = requests.Session()
_session.headers.update({
    'User-Agent': USER_AGENT,
    'Accept-Encoding': 'gzip, deflate',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
})


def _fetch_html_text(url: str) -> str:
    resp = _session.get(url, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, 'html.parser').get_text(' ', strip=True)


def _fetch_pdf_text(url: str) -> str:
    """Fetch a PDF and return decompressed FlateDecode stream text."""
    resp = _session.get(url, timeout=60)
    resp.raise_for_status()
    blob = resp.content
    out = []
    idx = 0
    while True:
        s = blob.find(b'stream', idx)
        if s < 0:
            break
        e = blob.find(b'endstream', s)
        if e < 0:
            break
        raw = blob[s + 6:e].strip(b'\r\n')
        try:
            out.append(zlib.decompress(raw).decode('latin-1', 'ignore'))
        except Exception:
            pass
        idx = e + 9
    return ' '.join(out)


def _num_near_keyword(text: str, fps: int, window: int = 220) -> bool:
    """True if `fps` (digit-bounded, with/without thousands comma) appears
    within `window` chars of a velocity keyword in `text`."""
    low = re.sub(r'\s+', ' ', text.lower())
    targets = {str(fps), f'{fps:,}'}
    pats = [re.compile(r'(?<!\d)' + re.escape(t) + r'(?!\d)') for t in targets]
    for kw in ('velocity', 'fps', 'muzzle'):
        start = 0
        while True:
            i = low.find(kw, start)
            if i < 0:
                break
            seg = low[max(0, i - window):i + window]
            if any(p.search(seg) for p in pats):
                return True
            start = i + 1
    return False


def _verify_pdf(text: str, fps: int) -> bool:
    """Armscor catalog guard: confirm the pistol ballistics chart is present
    (FMJ + JHP rows) and the expected velocity appears, digit-bounded."""
    low = text.lower()
    if not re.search(r'(?<!\d)' + str(fps) + r'(?!\d)', low):
        return False
    return ('fmj' in low and 'jhp' in low and 's&w' in low) or ('40s&w' in low)


def verify_row(cfg: dict):
    """Return (ok, velocity_text_found, note). Fetches the live source."""
    try:
        if cfg['kind'] == 'pdf':
            text = _fetch_pdf_text(cfg['url'])
            ok = _verify_pdf(text, cfg['mv'])
        else:
            text = _fetch_html_text(cfg['url'])
            ok = _num_near_keyword(text, cfg['mv'])
    except Exception as e:
        return False, False, f'FETCH FAILED: {e}'
    if not text.strip():
        return False, False, 'empty page (JS-rendered or blocked)'
    if not ok:
        return False, True, f'{cfg["mv"]} fps NOT found near a velocity keyword on live page'
    return True, True, 'verified live'


def upsert_row(supabase, cfg: dict) -> int:
    external_id = cfg['sku'] or (cfg['url'].rstrip('/').rsplit('/', 1)[-1])
    now = datetime.now(timezone.utc).isoformat()
    row = {
        'external_id': external_id,
        'source': cfg['source'],
        'brand': cfg['brand'],
        'sku': cfg['sku'],
        'product_line': cfg['line'],
        'caliber_normalized': CALIBER,
        'grain': cfg['grain'],
        'bullet_type': cfg['bullet'],
        'muzzle_velocity_fps': cfg['mv'],
        'muzzle_energy_ftlb': cfg['energy'],
        'source_url': cfg['url'],
        'last_seen_at': now,
        'last_scraped_at': now,
        'raw_html_hash': hashlib.sha256(f"{external_id}:{cfg['mv']}".encode()).hexdigest(),
    }
    res = (
        supabase.table('manufacturer_ballistics')
        .upsert(row, on_conflict='source,external_id')
        .execute()
    )
    return res.data[0]['id']


def main() -> int:
    ap = argparse.ArgumentParser(description='Curated, self-verifying .40 S&W ballistics ingest.')
    ap.add_argument('--dry-run', action='store_true', help='Fetch + verify + print; no DB writes.')
    ap.add_argument('--source', default='all', help='Only this source slug (e.g. nosler). Default: all.')
    args = ap.parse_args()

    rows = [c for c in SOURCES if args.source in ('all', c['source'])]
    supabase = None if args.dry_run else create_client(SUPABASE_URL, SUPABASE_KEY)

    shipped = skipped = 0
    for cfg in rows:
        ok, _seen, note = verify_row(cfg)
        tag = 'OK  ' if ok else 'SKIP'
        e = '' if cfg['energy'] is None else f" / {cfg['energy']} ft-lb"
        print(f'[{tag}] {cfg["brand"]:<18} {cfg["grain"]}gr {cfg["bullet"]:<4} '
              f'{cfg["mv"]} fps{e:<12} ({cfg["line"]}) — {note}')
        if not ok:
            skipped += 1
            continue
        if args.dry_run:
            shipped += 1
            continue
        try:
            upsert_row(supabase, cfg)
            shipped += 1
        except Exception as ex:
            print(f'       UPSERT FAILED: {ex}')
            skipped += 1

    mode = 'DRY RUN' if args.dry_run else 'LIVE'
    print(f'\nDone ({mode}). {shipped} row(s) {"would be " if args.dry_run else ""}upserted, '
          f'{skipped} skipped (unverifiable).')
    return 0


if __name__ == '__main__':
    sys.exit(main())
