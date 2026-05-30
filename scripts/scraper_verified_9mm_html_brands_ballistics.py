"""scraper_verified_9mm_html_brands_ballistics.py - curated, self-verifying 9mm
ballistics ingest for the own-site-HTML PUBLISHES brands (2026-05-30 verification
pass). Mirrors scraper_verified_40sw_ballistics.py and the Freedom 9mm ingest.

Each SEED row was confirmed on the manufacturer's OWN page; the scraper RE-FETCHES
each source_url at ingest and writes ONLY if the recorded velocity still appears
live next to a velocity keyword. Unverifiable -> SKIPPED, nothing written.

HONESTY RULES BAKED IN (per Jon's 2026-05-30 batch directive):
  * SELF-VERIFY every row at write time (below).
  * WITHIN-GRAIN COLLISION = HELD. Brands selling std/+P/+P+/XTP/defense at one
    grain+bullet with different velocities are NOT seeded (see HELD).
  * BULLET-LABEL TRAP = HELD. Solid-copper monolithic / exotic loads mislabeled
    in our listings (Underwood XD/XP, Fort Scott TUI, Gorilla Silverback, Norma
    MHP, Grizzly Bonebreaker, DoubleTap lead-free) are held as listing-side fixes.
  * NO new matcher aliases. Bullet types seeded as their own honest tokens; the
    only alias in play is the pre-existing JHP->[JHP,HP] (so a JHP row reaches an
    HP-tagged listing of the SAME single load - used for Atomic + Fenix FXP).
  * brand = EXACT listings.manufacturer string so the equi-join fires.
  * Liberty HELD pending review (disputed marketing-velocity claims).

Writes to: manufacturer_ballistics. Required env (LIVE only): SUPABASE_URL,
SUPABASE_KEY (read lazily; --dry-run needs no DB and writes nothing).

Usage:
  py scripts/scraper_verified_9mm_html_brands_ballistics.py --dry-run
  py scripts/scraper_verified_9mm_html_brands_ballistics.py
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
USER_AGENT = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
              '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36')

# ---------------------------------------------------------------------------
# SEED - verified single-velocity-per-(grain,bullet), non-collision, non-exotic,
# tag-reaching loads. `mv` must still appear next to a velocity keyword on `url`.
# `n` = in-stock 9mm listings carrying the reachable tag (live probe 2026-05-30).
# ---------------------------------------------------------------------------
SEED = [
    # Nosler ASP (Magento) - 124 JHP HELD (collision w/ +P Bonded Defense)
    dict(source='nosler', brand='Nosler', line='ASP', grain=115, bullet='JHP', mv=1170, n=11,
         url='https://www.nosler.com/9mm-luger-115gr-jhp-asp-handgun-ammunition.html'),
    dict(source='nosler', brand='Nosler', line='ASP', grain=147, bullet='JHP', mv=950, n=10,
         url='https://www.nosler.com/9mm-luger-147gr-jhp-asp-handgun-ammunition-20ct.html'),
    # Wolf Military Classic steel (plain HTML .aspx table)
    dict(source='wolf', brand='Wolf', line='Military Classic (steel)', grain=115, bullet='FMJ', mv=1150, n=19,
         url='https://wolfammo.com/steel-casing.aspx'),
    # Norma Range & Training (Next.js/Sanity - publishes m/s AND f/s; verify the f/s)
    dict(source='norma', brand='Norma', line='Range & Training', grain=115, bullet='FMJ', mv=1214, n=3,
         url='https://www.norma-ammunition.com/products/shooting/norma-range-and-training/norma-9-mm-luger-fmj-range-training-115-gr---620240050'),
    dict(source='norma', brand='Norma', line='Range & Training', grain=124, bullet='FMJ', mv=1181, n=1,
         url='https://www.norma-ammunition.com/products/shooting/norma-range-and-training/norma-9-mm-luger-fmj-range-training-124-gr---620340050'),
    # Black Hills (WordPress) - one 9mm page lists all loads. 124 JHP HELD (+P/std collision).
    dict(source='black_hills', brand='Black Hills', line='115gr FMJ', grain=115, bullet='FMJ', mv=1150, n=2,
         url='https://www.black-hills.com/shop/new-pistol-ammo/9mm-luger/'),
    dict(source='black_hills', brand='Black Hills', line='115gr EXP HP', grain=115, bullet='HP', mv=1200, n=2,
         url='https://www.black-hills.com/shop/new-pistol-ammo/9mm-luger/'),
    # Atomic (WordPress) - 2 loads total. JHP row reaches HP+JHP listings (one load each).
    dict(source='atomic', brand='Atomic', line='Subsonic 147 JHP', grain=147, bullet='JHP', mv=980, n=10,
         url='https://www.atomicammunition.com/products/handgun/9mm-147gr-jhp-subsonic-defense-ammo/'),
    dict(source='atomic', brand='Atomic', line='+P 124 JHP Defense', grain=124, bullet='JHP', mv=1200, n=2,
         url='https://www.atomicammunition.com/products/handgun/9mm-plus-p-124gr-jhp-defense-ammo/'),
    # Scorpion (WooCommerce) - all single
    dict(source='scorpion', brand='Scorpion', line='Self Defense JHP', grain=115, bullet='JHP', mv=1200, n=2,
         url='https://scorpionammo.com/product/scorpion-9mm-115gr-jhp-20-box/'),
    dict(source='scorpion', brand='Scorpion', line='Training FMJ', grain=115, bullet='FMJ', mv=1148, n=2,
         url='https://scorpionammo.com/product/9mm-training-115gr-fmj-box-50/'),
    dict(source='scorpion', brand='Scorpion', line='Self Defense JHP', grain=124, bullet='JHP', mv=1150, n=1,
         url='https://scorpionammo.com/product/scorpion-9mm-124gr-jhp-20-box/'),
    # Browning (ASP.NET) - 1190 on the handgun listing page (product page is empty)
    dict(source='browning', brand='Browning', line='FMJ Bulk', grain=115, bullet='FMJ', mv=1190, n=6,
         url='https://browningammo.com/Products/Ammunition/Handgun'),
    # Aguila (headless) - 124 single (115 FMJ HELD: std 1150 / +P 1250 collision)
    dict(source='aguila', brand='Aguila', line='FMJ', grain=124, bullet='FMJ', mv=1115, n=1,
         url='https://www.aguilaammo.com/ammunition/1e092110/'),
    # Fenix (Shopify) - 147 JHP row = FXP jacketed HP, reaches our 147 HP listing via alias
    dict(source='fenix', brand='Fenix Ammunition', line='FMJ', grain=124, bullet='FMJ', mv=1050, n=1,
         url='https://fenixammo.com/products/9mm-124gr-fmj'),
    dict(source='fenix', brand='Fenix Ammunition', line='FMJ', grain=115, bullet='FMJ', mv=1100, n=1,
         url='https://fenixammo.com/products/9mm-115gr-fmj'),
    dict(source='fenix', brand='Fenix Ammunition', line='FMJ', grain=147, bullet='FMJ', mv=890, n=1,
         url='https://fenixammo.com/products/9mm-147gr-fmj'),
    dict(source='fenix', brand='Fenix Ammunition', line='FXP Hollowpoint', grain=147, bullet='JHP', mv=950, n=1,
         url='https://fenixammo.com/products/9mm-147gr-fxp-hollowpoint-50-ct'),
    # New Republic (WordPress)
    dict(source='new_republic', brand='New Republic', line='FMJ', grain=115, bullet='FMJ', mv=1145, n=3,
         url='https://www.newrepublicammunition.com/9mm-luger-115-grain-full-metal-jacket/'),
    dict(source='new_republic', brand='New Republic', line='FMJ', grain=124, bullet='FMJ', mv=1090, n=1,
         url='https://www.newrepublicammunition.com/9mm-luger-124-grain-full-metal-jacket/'),
    # Precision One (WooCommerce) - 124 FMJ HELD (New/Reman/Comp collision)
    dict(source='precision_one', brand='Precision One', line='Competition FMJ', grain=135, bullet='FMJ', mv=950, n=1,
         url='https://precisiononeammunition.com/product/9mm-135gr-fmj-new-competition/'),
    dict(source='precision_one', brand='Precision One', line='FMJ', grain=115, bullet='FMJ', mv=1182, n=1,
         url='https://precisiononeammunition.com/product/9mm-115gr-fmj-new/'),
    # Georgia Arms (BigCommerce) - 147 JHP HELD (no velocity published)
    dict(source='georgia_arms', brand='Georgia Arms', line='FMJ NATO', grain=124, bullet='FMJ', mv=1150, n=1,
         url='https://www.georgia-arms.com/9mm-luger-124gr-full-metal-jacket-nato/'),
    dict(source='georgia_arms', brand='Georgia Arms', line='FMJ', grain=115, bullet='FMJ', mv=1150, n=1,
         url='https://www.georgia-arms.com/9mm-luger-115gr-full-metal-jacket/'),
    # Staccato (BigCommerce) - 136gr OTM match
    dict(source='staccato', brand='Staccato', line='Match OTM', grain=136, bullet='OTM', mv=1000, n=2,
         url='https://staccato2011.com/products/staccato-9mm-match-ammo'),
    # Wilson Combat (Magento) - Bill Wilson Signature 135gr HBFN
    dict(source='wilson_combat', brand='Wilson Combat', line='Bill Wilson Signature HBFN', grain=135, bullet='FN', mv=985, n=1,
         url='https://wilsoncombat.com/9mm-bill-wilson-signature-jacketed-match-training-load-135gr-berry-hbfn-100-box.html'),
]

# ---------------------------------------------------------------------------
# HELD - documented for the report; not seeded. (count, reason)
# ---------------------------------------------------------------------------
HELD = [
    ('Underwood', 73, 'collisions (std/+P/+P+ at every JHP grain) + solid-copper XD/XP mis-tagged; listing-fix'),
    ('Fort Scott Munitions', 32, 'ALL listings null bullet_type + TUI solid-copper - unmatchable; listing-fix'),
    ('DoubleTap', 29, 'every combo: collision (gun-tested +P) / no velocity published / exotic (77gr lead-free, 165 Equalizer dual-projectile) / mis-tag (50,145)'),
    ('Grizzly Cartridge', 20, 'all +P/+P+ collisions + Bonebreaker hardcast exotic; URL slugs mislabel JHP/FMJ'),
    ('Sterling', 15, 'velocity is a SPAN "1115-1180 fps", not a point - cannot store an honest single MV'),
    ('Ammo Inc', 10, 'own site TLS-walled (anti-bot) - cannot self-verify any velocity live'),
    ('Liberty', 9, 'HELD pending Jon review - disputed marketing-velocity claims (2040 fps)'),
    ('Buffalo Bore', 5, '147 & 115 JHP both std/+P/+P+ collisions'),
    ('Patriot Sports', 9, '115 V0 only on a PDF flyer in m/s (PDF follow-up); 124 is V4 (downrange, not muzzle)'),
    ('Excalibur', 9, 'no maker-published MUZZLE velocity (STV lists only V5 proof-barrel + V0 energy)'),
    ('HSM', 4, '180 FN mis-tag (no such load) + 115 JHP collision (plain/XTP/plated-RN)'),
    ('STV', 4, 'only V5 proof-barrel (355 m/s), not muzzle; + Luger/NATO collision'),
    ('Badlands', 2, '147: on-page 980 vs 950 conflict + copper-plated RN (not true FMJ)'),
    ('Colt', 2, '147 Colt National Match FMJ real, but DoubleTap publishes no velocity for it'),
    ('Super Vel', 2, '115 FMJ velocity not verified on own site (+ 1 null-bullet listing)'),
    ('Supernova', 2, '119gr FMJ mis-tag: tracer, no grain published, boilerplate 1070 shared across calibers'),
    ('Geco', 1, '50gr FMJ mis-tag: Geco 9mm is 124gr only'),
    ('Gorilla', 1, '90gr null-bullet Silverback solid-copper - unmatchable; listing-fix'),
    ('Global Ordnance', 1, '55gr FMJ mis-tag: that is Sterling 5.56 M193 (wrong caliber)'),
    # partial holds on brands that also shipped:
    ('Nosler (124 JHP)', 8, 'collision: ASP 1150 vs +P Bonded Defense'),
    ('Black Hills (124 JHP/FMJ, 115 JHP)', 7, '124 JHP +P/std collision; 124 FMJ no such load; 115 JHP+P would alias-bleed onto HP listings'),
    ('Norma (115/124 JHP, 158 TMJ)', 11, 'Safeguard JHP = MHP solid-copper/discontinued; 158 TMJ mis-tag (.357/.38)'),
    ('Aguila (115 FMJ)', 4, 'collision: std 1150 vs +P 1250'),
    ('Precision One (124 FMJ)', 1, 'collision: New 1100 / Reman 1089 / Comp 1044-1083'),
    ('Georgia Arms (147 JHP)', 1, 'Canned Heat 147 JHP exists but no velocity published'),
]

_session = requests.Session()
_session.headers.update({'User-Agent': USER_AGENT, 'Accept-Encoding': 'gzip, deflate',
                         'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                         'Accept-Language': 'en-US,en;q=0.9'})


def _fetch_html_text(url):
    resp = _session.get(url, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, 'html.parser').get_text(' ', strip=True)


def _num_near_keyword(text, fps, window=220):
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


def verify_row(cfg):
    try:
        text = _fetch_html_text(cfg['url'])
    except Exception as e:
        return False, f'FETCH FAILED: {e}'
    if not text.strip():
        return False, 'empty page (JS-rendered or blocked)'
    if not _num_near_keyword(text, cfg['mv']):
        return False, f'{cfg["mv"]} fps NOT found near a velocity keyword on live page'
    return True, 'verified live'


def upsert_row(supabase, cfg):
    external_id = cfg.get('sku') or (cfg['url'].rstrip('/').rsplit('/', 1)[-1] + f"-{cfg['grain']}{cfg['bullet']}")
    now = datetime.now(timezone.utc).isoformat()
    row = {
        'external_id': external_id, 'source': cfg['source'], 'brand': cfg['brand'],
        'sku': cfg.get('sku'), 'product_line': cfg['line'], 'caliber_normalized': CALIBER,
        'grain': cfg['grain'], 'bullet_type': cfg['bullet'], 'muzzle_velocity_fps': cfg['mv'],
        'muzzle_energy_ftlb': None, 'source_url': cfg['url'],
        'last_seen_at': now, 'last_scraped_at': now,
        'raw_html_hash': hashlib.sha256(f"{external_id}:{cfg['mv']}".encode()).hexdigest(),
    }
    res = supabase.table('manufacturer_ballistics').upsert(row, on_conflict='source,external_id').execute()
    return res.data[0]['id']


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true', help='Fetch + verify + print; no DB writes.')
    args = ap.parse_args()

    supabase = None
    if not args.dry_run:
        from dotenv import load_dotenv
        from supabase import create_client
        load_dotenv()
        supabase = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])

    print('=' * 84)
    print('SEED - re-verify each velocity LIVE on the manufacturer page')
    print('=' * 84)
    shipped = skipped = matched = 0
    by_brand = {}
    skips = []
    for cfg in SEED:
        ok, note = verify_row(cfg)
        tag = 'OK  ' if ok else 'SKIP'
        print(f'[{tag}] {cfg["brand"]:<17} {cfg["grain"]}gr {cfg["bullet"]:<4} {cfg["mv"]} fps  '
              f'-> reaches {cfg["n"]} listing(s)  ({cfg["line"]})')
        print(f'        {cfg["url"]}')
        print(f'        {note}')
        if not ok:
            skipped += 1
            skips.append(cfg)
            continue
        by_brand[cfg['brand']] = by_brand.get(cfg['brand'], 0) + 1
        matched += cfg['n']
        if args.dry_run:
            shipped += 1
        else:
            try:
                upsert_row(supabase, cfg)
                shipped += 1
            except Exception as ex:
                print(f'        UPSERT FAILED: {ex}')
                skipped += 1

    print('\n' + '=' * 84)
    print('HELD - not seeded (collision / exotic / mis-tag / range / walled / no-velocity)')
    print('=' * 84)
    held_total = 0
    for brand, n, reason in HELD:
        held_total += n
        print(f'  HOLD  {brand:<34} x{n:<3} {reason}')

    print('\n' + '=' * 84)
    print('SUMMARY')
    print('=' * 84)
    for b in sorted(by_brand):
        bl = sum(c['n'] for c in SEED if c['brand'] == b and verify_cached(c, skips))
        print(f'  {b:<18} {by_brand[b]} row(s)')
    print(f'\n  SEED rows {"would be " if args.dry_run else ""}written: {shipped}   SKIPPED: {skipped}')
    print(f'  Projected in-stock 9mm listings matched (verified rows only): {matched}')
    print(f'  9mm coverage 1301/2302 (56.5%) -> {1301 + matched}/2302 ({100 * (1301 + matched) / 2302:.1f}%)')
    if skips:
        print(f'  SKIPPED rows (self-verify failed - held): ' +
              ', '.join(f'{c["brand"]} {c["grain"]}{c["bullet"]}' for c in skips))
    print(f'  HELD listings documented: {held_total}')
    print(f'\nDone ({"DRY RUN - nothing written" if args.dry_run else "LIVE"}).')
    return 0


def verify_cached(cfg, skips):
    return cfg not in skips


if __name__ == '__main__':
    sys.exit(main())
