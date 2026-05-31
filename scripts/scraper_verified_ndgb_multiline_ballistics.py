"""scraper_verified_ndgb_multiline_ballistics.py - curated, self-verifying ingest
for Nosler / DoubleTap / Grizzly / Buffalo Bore multi-line 9mm + .40 collisions the
line-aware matcher can now split. Same pattern as the prior verified batches.

Per Jon's rulings (2026-05-30):
  - DoubleTap gun-tested velocities seeded as-is (their only published figures).
  - DoubleTap .40 135 JHP is NOT seeded: it has TWO lines (Controlled Expansion
    1350 published, Colt Defense EXISTS but velocity UNPUBLISHED). With one
    published velocity the matcher would Pass-1-stamp the Colt listings, so per
    ruling #2 the whole combo is HELD (honest blank over a guess).
  - Exotics HELD as listing-side fixes: Grizzly BoneBreaker (hardcast, mislabeled
    FMJ), DoubleTap lead-free SC-HP / Equalizer / SnakeShot / hardcast, Buffalo
    Bore Barnes lead-free / hardcast, Nosler Match Grade (discontinued).

--dry-run re-verifies each velocity live AND projects resolved-vs-held per combo
via the EXACT live matcher logic, plus confirms DoubleTap .40 135 stays held.

Usage:
  py scripts/scraper_verified_ndgb_multiline_ballistics.py --dry-run
  py scripts/scraper_verified_ndgb_multiline_ballistics.py
"""
import argparse, hashlib, os, re, sys
from collections import defaultdict
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup

UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36')

def BT(s): return None if s is None else str(s).strip().upper()
def norm(s): return re.sub(r'[^a-z0-9]', '', (s or '').lower())
def toks(s): return [t for t in re.split(r'[^a-z0-9]+', (s or '').lower()) if t]
STOP = {'usa','series','handgun','ammo','ammunition','the','grain','gr','rounds','round','box','bx','rd','rds','new','reman','luger','auto','pistol','rifle','centerfire','rimfire','fps','free','shipping','per','case','value','pack','bulk','best','count','ct','p','pp','personal','defense','training','item'}
BULLET_TYPE_ALIASES = {'JHP': ['JHP', 'HP']}

# [source, brand, cal, grain, bullet, product_line, sku, mv, url]
SEED = [
    # Nosler 9mm 124 JHP (collision 1150/1200; Match Grade discontinued -> held)
    ['nosler','Nosler','9mm',124,'JHP','ASP Assured Stopping Power','51286',1150,'https://www.nosler.com/9mm-luger-124gr-jhp-asp-handgun-ammunition-20ct.html'],
    ['nosler','Nosler','9mm',124,'JHP','Bonded Performance Defense','38432',1200,'https://www.nosler.com/9mm-luger-p-124gr-jhp-bonded-performance-defense-ammunition.html'],
    # DoubleTap 9mm 124 JHP (collision 1200/1300/1310; gun-tested)
    ['doubletap','DoubleTap','9mm',124,'JHP','Bonded Defense','9mm124bd',1300,'https://doubletapammo.com/products/9mm-p-124gr-bonded-defensea-jhp-20rds'],
    ['doubletap','DoubleTap','9mm',124,'JHP','Controlled Expansion','9mm124hp20',1310,'https://doubletapammo.com/products/9mm-p-124gr-controlled-expansion-a-jhp-20rds'],
    ['doubletap','DoubleTap','9mm',124,'JHP','Colt Defense','9m124ct',1200,'https://doubletapammo.com/products/9mm-124gr-colt-defense-ammunition-a-jhp-20rds'],
    # DoubleTap 9mm 147 JHP (single - Bonded Defense)
    ['doubletap','DoubleTap','9mm',147,'JHP','Bonded Defense','9mm147bd',1135,'https://doubletapammo.com/products/9mm-p-147gr-bonded-defensea-jhp-20rds'],
    # DoubleTap .40 200 JHP (single - Controlled Exp)
    ['doubletap','DoubleTap','40sw',200,'JHP','Controlled Expansion','40200ce',1050,'https://doubletapammo.com/products/40-s-w-200gr-controlled-expansion-a-jhp-20rds'],
    # Grizzly 9mm 115 JHP (collision std 1250 / +P 1350 - SKU markers)
    ['grizzly','Grizzly Cartridge','9mm',115,'JHP','9mm 115gr JHP','GC9MM1',1250,'https://grizzlycartridge.com/shop/9mm-115gr-jhp/'],
    ['grizzly','Grizzly Cartridge','9mm',115,'JHP','9mm +P 115gr JHP','GC9PCM9',1350,'https://grizzlycartridge.com/shop/9mm-p-115gr-jhp/'],
    # Grizzly 9mm 124 JHP (collision std 1200 / +P 1275)
    ['grizzly','Grizzly Cartridge','9mm',124,'JHP','9mm 124gr JHP','GC9MM2',1200,'https://grizzlycartridge.com/shop/9mm-124gr-jhp/'],
    ['grizzly','Grizzly Cartridge','9mm',124,'JHP','9mm +P 124gr JHP','GC9PCM10',1275,'https://grizzlycartridge.com/shop/9mm-p-124gr-jhp/'],
    # Buffalo Bore 9mm 147 JHP (collision 24C +P+ 1175 / 24I std 1000)
    ['buffalo_bore','Buffalo Bore','9mm',147,'JHP','24C','24C',1175,'https://www.buffalobore.com/index.php?l=product_detail&p=120'],
    ['buffalo_bore','Buffalo Bore','9mm',147,'JHP','24I','24I',1000,'https://www.buffalobore.com/index.php?l=product_detail&p=341'],
    # Buffalo Bore 9mm 115 JHP (collision 24A +P+ 1400 / 24D +P 1300)
    ['buffalo_bore','Buffalo Bore','9mm',115,'JHP','24A','24A',1400,'https://www.buffalobore.com/index.php?l=product_detail&p=118'],
    ['buffalo_bore','Buffalo Bore','9mm',115,'JHP','24D','24D',1300,'https://www.buffalobore.com/index.php?l=product_detail&p=121'],
]

_session = requests.Session()
_session.headers.update({'User-Agent': UA, 'Accept-Encoding': 'gzip, deflate',
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

def sb_fetch(sb, table, sel):
    out, start = [], 0
    while True:
        d = sb.table(table).select(sel).range(start, start + 999).execute().data
        if not d: break
        out.extend(d)
        if len(d) < 1000: break
        start += 1000
    return out

def project(verified):
    from dotenv import load_dotenv
    from supabase import create_client
    load_dotenv()
    sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])
    listings = sb_fetch(sb, 'listings', 'id,manufacturer,caliber_normalized,grain,bullet_type,product_url,retailer_product_id,is_component,in_stock')
    listings = [l for l in listings if l['in_stock'] and not l['is_component'] and (l['manufacturer'] or '').strip() in ('Nosler','DoubleTap','Grizzly Cartridge','Buffalo Bore') and l['caliber_normalized'] in ('9mm','40sw')]
    matched = {m['listing_id'] for m in sb_fetch(sb, 'manufacturer_ballistics_listing_matches', 'listing_id')}
    combos = defaultdict(list)
    for c in verified: combos[(c[1], c[2], c[3], c[4])].append(c)
    def cands(brand, cal, grain, bullet):
        blts = BULLET_TYPE_ALIASES.get(bullet, [bullet])
        return [l for l in listings if (l['manufacturer'] or '').strip() == brand and l['caliber_normalized'] == cal and l['grain'] == grain and BT(l['bullet_type']) in blts and l['id'] not in matched]
    print('\n' + '=' * 92)
    print('PROJECTION (live line-aware matcher logic) - resolved vs held per combo, unmatched listings')
    print('=' * 92)
    tot_res = tot_held = 0
    for key, rows in combos.items():
        brand, cal, grain, bullet = key
        cs = cands(brand, cal, grain, bullet)
        vels = sorted({r[7] for r in rows})
        reach = f"bullet '{bullet}' reaches {BULLET_TYPE_ALIASES.get(bullet, [bullet])}"
        if len(vels) == 1:
            print(f"\n  {brand} {cal} {grain} {bullet}: SINGLE {vels[0]} -> Pass1 ALL {len(cs)} match.  ({reach})")
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
        print(f"     sku=[{','.join(sku_keys)}] kw=[{','.join(kw_keys)}] dropped-shared=[{dropped}]")
        print(f"     candidates={len(cs)}  RESOLVED={res} {dict(by_vel)}  HELD={held}")
        tot_res += res; tot_held += held
    # explicit confirmation: DoubleTap .40 135 JHP must remain HELD (not seeded)
    dt135 = cands('DoubleTap', '40sw', 135, 'JHP')
    print(f"\n  [ruling #2 check] DoubleTap .40 135 JHP NOT seeded -> all {len(dt135)} listings remain HELD (Colt Defense velocity unpublished).")
    print(f"\n  === PROJECTED: RESOLVED={tot_res}  HELD-after-seed(seeded combos)={tot_held}  + .40 135 held={len(dt135)} ===")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()
    print('=' * 92); print('SEED - re-verify each line velocity LIVE on the manufacturer page'); print('=' * 92)
    verified = []
    for cfg in SEED:
        ok, note = verify_row(cfg)
        tag = 'OK  ' if ok else 'SKIP'
        print(f"[{tag}] {cfg[1]:<17} {cfg[2]:<5} {cfg[3]}gr {cfg[4]:<4} {cfg[7]} fps  emits={cfg[4]!r}  line='{cfg[5]}' sku={cfg[6]}")
        print(f"       {cfg[8]}  -> {note}")
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
