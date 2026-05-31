"""scraper_verified_fedremspeer_multiline_ballistics.py - curated, self-verifying
ingest for Federal/Remington/Speer multi-line 9mm + .40 loads that the live
line-aware matcher can now split by product line. Same pattern as the prior
verified batches: re-fetch each line's manufacturer page at ingest and write
ONLY if the velocity still appears live next to a velocity keyword; else SKIP.

Per Jon's rulings (2026-05-30):
  - Federal 9mm 124 HST: seed BOTH std 1150 (P9HST1S) and +P 1200 (P9HST3S);
    HST listings without a SKU/+P marker stay HELD (honest blank).
  - Federal LE-portal velocities (HST .40 165 = 1130, Hi-Shok 9mm 115 = 1180)
    are seeded - Federal's own parent (Vista Outdoor), self-verifiable.
  - Remington Ultimate Defense .40 180: not seeded as its own row; the .40 180
    JHP combo is single-velocity 1015 so its listings match the 1015 row anyway.
  - Speer Gold Dot G2 left on SKU-only (no 3-char keyword loosening).

--dry-run re-verifies each velocity live AND projects resolved-vs-held listings
per combo using the EXACT live matcher Pass-1/Pass-2 logic. No DB writes in dry-run.

Usage:
  py scripts/scraper_verified_fedremspeer_multiline_ballistics.py --dry-run
  py scripts/scraper_verified_fedremspeer_multiline_ballistics.py
"""
import argparse, hashlib, os, re, sys
from collections import defaultdict
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup

USER_AGENT = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
             '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36')

# matcher-identical resolution helpers
def BT(s): return None if s is None else str(s).strip().upper()
def norm(s): return re.sub(r'[^a-z0-9]', '', (s or '').lower())
def toks(s): return [t for t in re.split(r'[^a-z0-9]+', (s or '').lower()) if t]
STOP = {'usa','series','handgun','ammo','ammunition','the','grain','gr','rounds','round','box','bx','rd','rds','new','reman','luger','auto','pistol','rifle','centerfire','rimfire','fps','free','shipping','per','case','value','pack','bulk','best','count','ct','p','pp','personal','defense','training'}
BULLET_TYPE_ALIASES = {'JHP': ['JHP', 'HP']}

# [source, brand, cal, grain, bullet, product_line, sku, mv, url]
SEED = [
    # Federal 9mm 124 JHP (collision 1120/1150/1200; HST std + +P)
    ['federal','Federal','9mm',124,'JHP','Personal Defense HST','P9HST1S',1150,'https://www.federalpremium.com/handgun/personal-defense-hst/11-P9HST1S.html'],
    ['federal','Federal','9mm',124,'JHP','Personal Defense HST','P9HST3S',1200,'https://www.federalpremium.com/handgun/personal-defense-hst/11-P9HST3S.html'],
    ['federal','Federal','9mm',124,'JHP','Personal Defense Hydra-Shok','P9HS1',1120,'https://www.federalpremium.com/handgun/personal-defense-hydra-shok/11-P9HS1.html'],
    ['federal','Federal','9mm',124,'JHP','Personal Defense Punch','PD9P1',1150,'https://www.federalpremium.com/handgun/personal-defense-punch/11-PD9P1.html'],
    # Federal 9mm 115 JHP (single velocity 1180 - Hi-Shok; LE portal source)
    ['federal','Federal','9mm',115,'JHP','Classic Hi-Shok','9BP',1180,'https://le.vistaoutdoor.com/ammunition/federal/handgun/details.aspx?id=525'],
    # Federal .40 165 JHP (collision 980/1050/1130)
    ['federal','Federal','40sw',165,'JHP','Personal Defense HST','P40HST3',1130,'https://le.vistaoutdoor.com/ammunition/federal/handgun/details.aspx?id=568'],
    ['federal','Federal','40sw',165,'JHP','Personal Defense Punch','PD40P1',1130,'https://www.federalpremium.com/handgun/personal-defense-punch/11-PD40P1.html'],
    ['federal','Federal','40sw',165,'JHP','Personal Defense Hydra-Shok','P40HS3',980,'https://www.federalpremium.com/handgun/personal-defense-hydra-shok/11-P40HS3.html'],
    ['federal','Federal','40sw',165,'JHP','Personal Defense Hydra-Shok Deep','P40HSD1',1050,'https://www.federalpremium.com/handgun/premium-personal-defense/personal-defense-hydra-shok-deep/11-P40HSD1.html'],
    # Federal .40 175 JHP (single - Syntech Defense segmented HP)
    ['federal','Federal','40sw',175,'JHP','Syntech Defense','S40SJT1',1000,'https://www.federalpremium.com/handgun/syntech/syntech-defense/11-S40SJT1.html'],
    # Remington 9mm 124 JHP (collision 1125/1180 - Golden Saber std/+P/bonded)
    ['remington','Remington','9mm',124,'JHP','Golden Saber Defense','27601',1125,'https://www.remington.com/handgun/golden-saber-defense/29-27601.html'],
    ['remington','Remington','9mm',124,'JHP','Golden Saber Defense','27603',1180,'https://www.remington.com/handgun/golden-saber-defense/29-27603.html'],
    ['remington','Remington','9mm',124,'JHP','Golden Saber Bonded','29341',1125,'https://www.remington.com/handgun/golden-saber-bonded/29-29341.html'],
    # Remington .40 180 JHP (single velocity 1015 - UMC/GS/HTP converge; one row)
    ['remington','Remington','40sw',180,'JHP','UMC Handgun','23687',1015,'https://www.remington.com/handgun/umc-handgun/29-23687.html'],
    # Speer .40 180 JHP (collision 950/1015/1025)
    ['speer','Speer','40sw',180,'JHP','Gold Dot Personal Protection','23962GD',1025,'https://www.speer.com/ammunition/gold-dot/gold-dot-handgun-personal-protection/19-23962GD.html'],
    ['speer','Speer','40sw',180,'JHP','Gold Dot G2','23999',1015,'https://www.speer.com/ammunition/gold-dot/gold-dot-g2/19-23999.html'],
    ['speer','Speer','40sw',180,'JHP','Gold Dot Short Barrel','23974GD',950,'https://www.speer.com/ammunition/gold-dot/gold-dot-short-barrel-personal-protection/19-23974GD.html'],
    # Speer .40 165 TMJ (collision 1050/1150 - Lawman vs Clean-Fire)
    ['speer','Speer','40sw',165,'TMJ','Lawman Handgun Training','53955',1150,'https://www.speer.com/ammunition/lawman/lawman-handgun-training/19-53955.html'],
    ['speer','Speer','40sw',165,'TMJ','Clean-Fire','53954',1050,'https://www.speer.com/ammunition/lawman/clean-fire/19-53954.html'],
]

_session = requests.Session()
_session.headers.update({'User-Agent': USER_AGENT, 'Accept-Encoding': 'gzip, deflate',
                         'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', 'Accept-Language': 'en-US,en;q=0.9'})

def _num_near_keyword(text, fps, window=220):
    low = re.sub(r'\s+', ' ', text.lower())
    targets = {str(fps), f'{fps:,}'}
    pats = [re.compile(r'(?<!\d)' + re.escape(t) + r'(?!\d)') for t in targets]
    for kw in ('velocity', 'fps', 'muzzle'):
        start = 0
        while True:
            i = low.find(kw, start)
            if i < 0: break
            seg = low[max(0, i - window):i + window]
            if any(p.search(seg) for p in pats): return True
            start = i + 1
    return False

def verify_row(cfg):
    try:
        r = _session.get(cfg[8], timeout=30); r.raise_for_status()
        text = BeautifulSoup(r.text, 'html.parser').get_text(' ', strip=True)
    except Exception as e:
        return False, f'FETCH FAILED: {e}'
    if not text.strip(): return False, 'empty page'
    if not _num_near_keyword(text, cfg[7]): return False, f'{cfg[7]} not near a velocity keyword'
    return True, 'verified live'

def sb_fetch(sb, table, sel, flt=''):
    out, start = [], 0
    while True:
        q = sb.table(table).select(sel)
        d = q.range(start, start + 999).execute().data
        if not d: break
        out.extend(d)
        if len(d) < 1000: break
        start += 1000
    return out

def project(verified):
    """Run the exact live-matcher resolution against current listings for the verified rows."""
    from dotenv import load_dotenv
    from supabase import create_client
    load_dotenv()
    sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])
    listings = sb_fetch(sb, 'listings', 'id,manufacturer,caliber_normalized,grain,bullet_type,product_url,retailer_product_id,is_component,in_stock')
    listings = [l for l in listings if l['in_stock'] and not l['is_component'] and (l['manufacturer'] or '').strip() in ('Federal','Remington','Speer') and l['caliber_normalized'] in ('9mm','40sw')]
    matchRows = sb_fetch(sb, 'manufacturer_ballistics_listing_matches', 'listing_id')
    matched = {m['listing_id'] for m in matchRows}
    combos = defaultdict(list)
    for c in verified: combos[(c[1], c[2], c[3], c[4])].append(c)
    def cands(brand, cal, grain, bullet):
        blts = BULLET_TYPE_ALIASES.get(bullet, [bullet])
        return [l for l in listings if (l['manufacturer'] or '').strip() == brand and l['caliber_normalized'] == cal and l['grain'] == grain and BT(l['bullet_type']) in blts and l['id'] not in matched]
    print('\n' + '=' * 90)
    print('PROJECTION (live line-aware matcher logic) - resolved vs held per combo, unmatched listings')
    print('=' * 90)
    tot_res = tot_held = 0
    for key, rows in combos.items():
        brand, cal, grain, bullet = key
        cs = cands(brand, cal, grain, bullet)
        vels = sorted({r[7] for r in rows})
        reach = f"bullet '{bullet}' reaches listings tagged {BULLET_TYPE_ALIASES.get(bullet, [bullet])}"
        if len(vels) == 1:
            print(f"\n  {brand} {cal} {grain} {bullet}: SINGLE velocity {vels[0]} -> Pass 1: ALL {len(cs)} match.  ({reach})")
            tot_res += len(cs); continue
        tokvel, is_sku, tok_row = defaultdict(set), {}, {}
        for r in rows:
            ks = []
            ns = norm(r[6])
            if ns and len(ns) >= 4: ks.append((ns, True))
            for t in toks(r[5]):
                if t not in STOP and len(t) >= 3: ks.append((t, False))
            for kw, sk in ks:
                tokvel[kw].add(r[7]); is_sku[kw] = is_sku.get(kw, False) or sk; tok_row.setdefault(kw, r)
        distinct = {kw: next(iter(v)) for kw, v in tokvel.items() if len(v) == 1}
        sku_keys = sorted([k for k in distinct if is_sku[k]], key=len, reverse=True)
        kw_keys = sorted([k for k in distinct if not is_sku[k]], key=len, reverse=True)
        by_vel = defaultdict(int); held = 0
        for l in cs:
            nb = norm(l['product_url']) + norm(l['retailer_product_id'])
            acc = []
            for kw in sku_keys:
                if any(kw in a for a in acc): continue
                if (any(c.isalpha() for c in kw) or len(kw) >= 5) and kw in nb: acc.append(kw)
            vs = {distinct[k] for k in acc}
            if len(vs) == 1: by_vel[distinct[acc[0]]] += 1
            elif len(vs) >= 2: held += 1
            else:
                kacc = []
                for kw in kw_keys:
                    if any(kw in a for a in kacc): continue
                    if kw in nb: kacc.append(kw)
                kvs = {distinct[k] for k in kacc}
                if len(kvs) == 1: by_vel[distinct[kacc[0]]] += 1
                else: held += 1
        res = sum(by_vel.values())
        dropped = ','.join(k for k, v in tokvel.items() if len(v) >= 2) or 'none'
        print(f"\n  {brand} {cal} {grain} {bullet}: COLLISION vels={vels}  ({reach})")
        print(f"     distinctive sku=[{','.join(sku_keys)}] kw=[{','.join(kw_keys)}]  dropped-shared=[{dropped}]")
        print(f"     candidates={len(cs)}  RESOLVED={res} {dict(by_vel)}  HELD={held}")
        tot_res += res; tot_held += held
    print(f"\n  === PROJECTED total: RESOLVED={tot_res}  HELD-after-seed={tot_held} ===")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()
    print('=' * 90); print('SEED - re-verify each line velocity LIVE on the manufacturer page'); print('=' * 90)
    verified = []
    for cfg in SEED:
        ok, note = verify_row(cfg)
        tag = 'OK  ' if ok else 'SKIP'
        print(f"[{tag}] {cfg[1]:<10} {cfg[2]:<5} {cfg[3]}gr {cfg[4]:<4} {cfg[7]} fps  emits bullet_type={cfg[4]!r}  line='{cfg[5]}' sku={cfg[6]}")
        print(f"       {cfg[8]}")
        print(f"       -> {note}")
        if ok: verified.append(cfg)
    print(f"\nVerified {len(verified)}/{len(SEED)} rows.")
    if not args.dry_run:
        from dotenv import load_dotenv
        from supabase import create_client
        load_dotenv()
        sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])
        now = datetime.now(timezone.utc).isoformat()
        for cfg in verified:
            ext = cfg[6] or norm(cfg[8])
            row = {'external_id': ext, 'source': cfg[0], 'brand': cfg[1], 'sku': cfg[6], 'product_line': cfg[5],
                   'caliber_normalized': cfg[2], 'grain': cfg[3], 'bullet_type': cfg[4], 'muzzle_velocity_fps': cfg[7],
                   'muzzle_energy_ftlb': None, 'source_url': cfg[8], 'last_seen_at': now, 'last_scraped_at': now,
                   'raw_html_hash': hashlib.sha256(f'{ext}:{cfg[7]}'.encode()).hexdigest()}
            sb.table('manufacturer_ballistics').upsert(row, on_conflict='source,external_id').execute()
        print(f'LIVE: upserted {len(verified)} rows.')
    project(verified)
    print(f"\nDone ({'DRY RUN - nothing written' if args.dry_run else 'LIVE'}).")

if __name__ == '__main__':
    sys.exit(main())
