"""scraper_verified_9mm_uw_fm_ballistics.py - curated, self-verifying 9mm Luger
ballistics ingest for Underwood + Freedom Munitions.

WHY THIS FILE EXISTS
--------------------
Underwood and Freedom Munitions both publish muzzle velocity on their OWN sites
(Underwood = BigCommerce "Muzzle Velocity (fps):" spec; Freedom = "Velocity AVG
#### fps" spec). Neither had a single 9mm row in manufacturer_ballistics, so
100% of their in-stock 9mm pool (Underwood 73 / Freedom 61) was unmatched - the
two single largest never-seeded 9mm brands. This curated, live-verifying ingest
mirrors scraper_verified_40sw_ballistics.py: every row was confirmed by reading
the manufacturer's own page (2026-05-30 batch), and the scraper RE-FETCHES each
source_url at ingest and upserts ONLY if the recorded velocity still appears on
the live page next to a velocity keyword. Unverifiable -> SKIPPED, nothing written.

HONESTY RULES BAKED IN (per Jon's 2026-05-30 rulings + the .40 discipline):
  * RULING #1 (Underwood solids): the Xtreme Defender / Xtreme Penetrator loads
    are solid-copper MONOLITHIC projectiles, NOT JHP/FMJ. We do NOT force them
    into a convenient tag. Our listings mis-tag them HP/FMJ/JHP/blank, so they
    are HELD and logged as LISTING-SIDE data fixes - never seeded under a wrong
    label. Honest label over a convenient match.
  * RULING #2 (Freedom RN/FP): RN and FP are seeded as their OWN bullet_type
    rows. NO round-nose->FMJ matcher alias (RN and FMJ are physically different).
    The matcher join is verified to accept arbitrary bullet_type strings end-to-
    end (free-text column, no CHECK; equi-join falls back to the literal token;
    frontend renders velocity regardless of bullet_type) - confirmed 2026-05-30.
  * WITHIN-GRAIN VELOCITY COLLISIONS ARE HELD. Both brands sell standard / +P /
    +P+ / X-DEF / ProMatch / XTP variants at one grain that all collapse to a
    single (grain, bullet_type) tag in our listings. Stamping one velocity would
    mislabel the others, so every colliding (grain, bullet) is HELD, not seeded.
  * brand = the EXACT listings.manufacturer string ('Underwood' / 'Freedom
    Munitions') so the strict 4-column equi-join fires.
  * Freedom publishes no muzzle energy -> energy None (frontend derives + labels
    it "calculated from velocity").

Writes to: manufacturer_ballistics (one row per verified SKU)
Required env (LIVE only): SUPABASE_URL, SUPABASE_KEY  (read lazily; --dry-run
needs no DB and writes nothing).

Usage:
  py scripts/scraper_verified_9mm_uw_fm_ballistics.py --dry-run
  py scripts/scraper_verified_9mm_uw_fm_ballistics.py
"""

import argparse
import hashlib
import os
import re
import sys
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

CALIBER = '9mm'

USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
)

# ---------------------------------------------------------------------------
# SEED - verified single-velocity-per-(grain,bullet) loads. Each `mv` must
# still appear next to a velocity keyword on the live `url`. `n_listings` is
# the in-stock 9mm listing count carrying this exact (brand,grain,bullet) tag
# (from the live coverage probe, 2026-05-30) - what this row matches once the
# matcher runs. `tag_reaches` documents that our listings carry this exact tag.
# ---------------------------------------------------------------------------
SEED = [
    # ---- Freedom Munitions (freedommunitions.com, "Velocity AVG"). RN/FP/FMJ
    # are single-velocity per (grain,bullet); seeded as their own honest tags. ----
    dict(source='freedom_munitions', brand='Freedom Munitions', line='Range (new+reman, all RN)',
         sku=None, grain=115, bullet='RN', mv=1120, energy=None, n_listings=10,
         url='https://www.freedommunitions.com/products/9mm-luger-115-gr-rn-new-500-ct'),
    dict(source='freedom_munitions', brand='Freedom Munitions', line='Range (new+reman, all RN)',
         sku=None, grain=124, bullet='RN', mv=1065, energy=None, n_listings=8,
         url='https://www.freedommunitions.com/products/9mm-luger-124-gr-rn-new-250-ct'),
    dict(source='freedom_munitions', brand='Freedom Munitions', line='Range RNFP',
         sku=None, grain=135, bullet='FP', mv=1000, energy=None, n_listings=5,
         url='https://www.freedommunitions.com/products/9mm-luger-135-gr-rnfp-new'),
    dict(source='freedom_munitions', brand='Freedom Munitions', line='HUSH subsonic',
         sku=None, grain=165, bullet='RN', mv=800, energy=None, n_listings=1,
         url='https://www.freedommunitions.com/products/9mm-luger-165-gr-rn-hush-new'),
    dict(source='freedom_munitions', brand='Freedom Munitions', line='Range FMJ',
         sku=None, grain=115, bullet='FMJ', mv=1180, energy=None, n_listings=1,
         url='https://www.freedommunitions.com/products/9mm-luger-115-gr-fmj-new'),
    dict(source='freedom_munitions', brand='Freedom Munitions', line='NATO FMJ',
         sku=None, grain=124, bullet='FMJ', mv=1175, energy=None, n_listings=1,
         url='https://www.freedommunitions.com/products/9mm-nato-124-gr-fmj-new'),
]

# ---------------------------------------------------------------------------
# HELD - documented for transparency; NOT seeded. `vels` = the distinct
# manufacturer velocities at this (grain,bullet) that force the hold.
# ---------------------------------------------------------------------------
HELD = [
    # ---- Underwood (underwoodammo.com) - 73 listings, 0 seedable ----
    dict(brand='Underwood', grain=90,  bullet='HP/FMJ/JHP/blank', n=21, vels='1400/1475/1550',
         reason='SOLID-COPPER Xtreme Defender (monolithic), mis-tagged + 3-way +P velocity collision -> RULING#1 HOLD + listing-fix'),
    dict(brand='Underwood', grain=68,  bullet='blank/HP/FMJ/SP', n=11, vels='1700/1800',
         reason='SOLID-COPPER Xtreme Defender (monolithic), mis-tagged + +P collision -> RULING#1 HOLD + listing-fix'),
    dict(brand='Underwood', grain=124, bullet='JHP', n=13, vels='1150/1225/1300',
         reason='3 manufacturer JHP loads (XTP / +P / +P XTP) collapse to one 124gr JHP tag -> collision HOLD'),
    dict(brand='Underwood', grain=115, bullet='JHP', n=9, vels='1200/1300/1400',
         reason='3 manufacturer JHP loads (Sporting / +P / +P Sporting) collapse to one 115gr JHP tag -> collision HOLD'),
    dict(brand='Underwood', grain=147, bullet='JHP', n=9, vels='1050/1125/1175',
         reason='3 manufacturer JHP loads (std / +P / +P) collapse to one 147gr JHP tag -> collision HOLD'),
    dict(brand='Underwood', grain=147, bullet='FN', n=3, vels='1100',
         reason='Single hard-cast Flat-Nose load (clean 1100) BUT exotic hard-cast -> lean-HOLD per .40 discipline; JUDGMENT for Jon'),
    dict(brand='Underwood', grain=115, bullet='FMJ', n=2, vels='1150/1300',
         reason='Our "115gr FMJ" listings are Xtreme Penetrator SOLIDS mis-tagged -> RULING#1 HOLD + listing-fix'),
    dict(brand='Underwood', grain=147, bullet='FMJ', n=2, vels='1000/1175',
         reason='Range-Supply FMJ 1000 vs +P FMJ-FN 1175 collapse to one 147gr FMJ tag -> collision HOLD'),
    dict(brand='Underwood', grain=115, bullet='blank', n=1, vels='1150/1250',
         reason='Xtreme Penetrator solid, blank bullet tag -> RULING#1 HOLD + listing-fix'),
    dict(brand='Underwood', grain=None, bullet='null/JHP', n=2, vels='-',
         reason='null grain and/or null bullet - never satisfies the equi-join -> listing-fix'),
    # ---- Freedom Munitions - 35 of 61 held (all JHP grains + 147 RN) ----
    dict(brand='Freedom Munitions', grain=115, bullet='JHP', n=8, vels='1095/1150/1175',
         reason='X-DEF / plain JHP / XTP collapse to one 115gr JHP tag -> collision HOLD'),
    dict(brand='Freedom Munitions', grain=124, bullet='JHP', n=7, vels='1060/1125',
         reason='reman HP 1060 vs X-DEF 1125 collapse to one 124gr JHP tag -> collision HOLD'),
    dict(brand='Freedom Munitions', grain=147, bullet='JHP', n=5, vels='890/925/950',
         reason='ProMatch / plain / X-DEF collapse to one 147gr JHP tag -> collision HOLD'),
    dict(brand='Freedom Munitions', grain=135, bullet='JHP', n=5, vels='990/1000',
         reason='ProMatch 990 vs plain 1000 collapse to one 135gr JHP tag -> collision HOLD'),
    dict(brand='Freedom Munitions', grain=147, bullet='RN', n=10, vels='925/870',
         reason='regular RN 925 vs HUSH subsonic RN 870 collapse to one 147gr RN tag -> collision HOLD'),
]

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


def _num_near_keyword(text: str, fps: int, window: int = 220) -> bool:
    """True if `fps` (digit-bounded, with/without thousands comma) appears
    within `window` chars of a velocity keyword."""
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


def verify_row(cfg: dict):
    """Return (ok, note). Fetches the live manufacturer source."""
    try:
        text = _fetch_html_text(cfg['url'])
    except Exception as e:
        return False, f'FETCH FAILED: {e}'
    if not text.strip():
        return False, 'empty page (JS-rendered or blocked)'
    if not _num_near_keyword(text, cfg['mv']):
        return False, f'{cfg["mv"]} fps NOT found near a velocity keyword on live page'
    return True, 'verified live'


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
    ap = argparse.ArgumentParser(description='Curated, self-verifying 9mm Underwood+Freedom ballistics ingest.')
    ap.add_argument('--dry-run', action='store_true', help='Fetch + verify + print; no DB writes.')
    args = ap.parse_args()

    supabase = None
    if not args.dry_run:
        from dotenv import load_dotenv
        from supabase import create_client
        load_dotenv()
        supabase = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])

    print('=' * 78)
    print('SEED - verify each velocity LIVE on the manufacturer page')
    print('=' * 78)
    shipped = skipped = matched_listings = 0
    by_brand_seed = {}
    for cfg in SEED:
        ok, note = verify_row(cfg)
        tag = 'OK  ' if ok else 'SKIP'
        reaches = f"Y (matches {cfg['n_listings']} listing(s) tagged {cfg['grain']}gr {cfg['bullet']})"
        print(f'[{tag}] {cfg["brand"]:<17} {cfg["grain"]}gr {cfg["bullet"]:<4} '
              f'{cfg["mv"]} fps  ({cfg["line"]})')
        print(f'       src: {cfg["url"]}')
        print(f'       emits bullet_type={cfg["bullet"]!r} -> reaches our tags: {reaches}')
        print(f'       -> {note}')
        if not ok:
            skipped += 1
            continue
        by_brand_seed[cfg['brand']] = by_brand_seed.get(cfg['brand'], 0) + 1
        matched_listings += cfg['n_listings']
        if args.dry_run:
            shipped += 1
        else:
            try:
                upsert_row(supabase, cfg)
                shipped += 1
            except Exception as ex:
                print(f'       UPSERT FAILED: {ex}')
                skipped += 1

    print('\n' + '=' * 78)
    print('HELD - NOT seeded (documented). Underwood solids = listing-side fixes.')
    print('=' * 78)
    held_by_brand = {}
    for h in HELD:
        held_by_brand[h['brand']] = held_by_brand.get(h['brand'], 0) + h['n']
        g = 'null' if h['grain'] is None else f"{h['grain']}gr"
        print(f"  HOLD  {h['brand']:<17} {g:<6} {h['bullet']:<16} x{h['n']:<3} "
              f"vels=[{h['vels']}]")
        print(f"        {h['reason']}")

    print('\n' + '=' * 78)
    print('SUMMARY - projected matched-after-seeding (equi-join on live listing tags)')
    print('=' * 78)
    for brand in ('Underwood', 'Freedom Munitions'):
        s = by_brand_seed.get(brand, 0)
        held = held_by_brand.get(brand, 0)
        seeded_listings = sum(c['n_listings'] for c in SEED if c['brand'] == brand)
        print(f'  {brand:<17} seeded rows={s:<2} -> matches {seeded_listings:<3} listing(s);  HELD {held} listing(s)')
    print(f'\n  TOTAL seed rows {"would be " if args.dry_run else ""}written: {shipped}  (skipped {skipped})')
    print(f'  TOTAL listings newly matched (projected): {matched_listings} of 134 in-stock 9mm '
          f'({100*matched_listings/134:.1f}%)')
    print(f'  9mm coverage 1274/2302 (55.3%) -> {1274+matched_listings}/2302 '
          f'({100*(1274+matched_listings)/2302:.1f}%)')
    mode = 'DRY RUN - nothing written' if args.dry_run else 'LIVE'
    print(f'\nDone ({mode}).')
    return 0


if __name__ == '__main__':
    sys.exit(main())
