"""match_ballistics_to_listings.py - populate manufacturer_ballistics_listing_matches
(LINE-AWARE, two-pass).

For every row in manufacturer_ballistics, finds the listings it should match.
The join key is still manufacturer + caliber_normalized + grain + bullet_type
(with BRAND_ALIASES CCI<->Blazer and BULLET_TYPE_ALIASES JHP<->HP), BUT when 2+
ballistics rows share that key at DIFFERENT velocities (a "collision combo" - e.g.
Federal HST vs Hydra-Shok vs Punch, or Winchester USA vs WinClean vs Silvertip),
a line-blind join would stamp one velocity across differing loads. So:

  PASS 1 (non-collision keys): unchanged strict equi-join - every candidate
    listing matches the single row. Byte-identical to the historical matcher.

  PASS 2 (collision keys): per-listing line resolution. Read the listing's
    product line out of product_url + retailer_product_id and assign it to the
    one row whose line it actually is:
      (a) SKU-in-slug, longest-first (sku >= 4 chars, alpha-preferred so a bare
          5-digit numeric sku needs len>=5 to count).
      (b) else DISTINCTIVE line-keyword, longest-first (so 'hydra-shok-deep'
          wins over 'hydra-shok', 'short-barrel'/'g2' over 'gold-dot').
      (c) else HOLD - write NOTHING for that listing.
    Tokens shared across DIFFERING-velocity lines (e.g. 'usa') are dropped. If
    the surviving signals resolve to 2+ different velocities -> HOLD. There is
    NO default-line fallback and the velocity is NEVER read out of the slug:
    when the line cannot be confidently identified the listing simply gets no
    match row (honest blank), exactly like an unseeded brand. This is the
    never-prey-on-the-ignorant trade: an honest gap over a stamped guess.

Stale-match handling is unchanged: every run deletes each ballistics row's
existing matches and re-inserts the current set, so a listing that no longer
resolves to a row drops out cleanly.

Required env: SUPABASE_URL, SUPABASE_KEY
Usage:
  python scripts/match_ballistics_to_listings.py --dry-run
  python scripts/match_ballistics_to_listings.py
"""

import argparse
import os
import re
import sys
from collections import defaultdict

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_KEY']
PAGE = 1000

BULLET_TYPE_ALIASES = {'JHP': ['JHP', 'HP']}
BRAND_ALIASES = {'CCI': ['CCI', 'Blazer']}
# Generic tokens that never identify a product line (umbrella brand line names,
# cartridge words, pack/qty words). Dropped from line-keyword resolution.
STOP = {'usa', 'series', 'handgun', 'ammo', 'ammunition', 'the', 'grain', 'gr',
        'rounds', 'round', 'box', 'bx', 'rd', 'rds', 'new', 'reman', 'luger', 'auto',
        'pistol', 'rifle', 'centerfire', 'rimfire', 'fps', 'free', 'shipping', 'per',
        'case', 'value', 'pack', 'bulk', 'best', 'count', 'ct', 'p', 'pp'}


def BT(s):
    return None if s is None else str(s).strip().upper()


def norm(s):
    return re.sub(r'[^a-z0-9]', '', (s or '').lower())


def toks(s):
    return [t for t in re.split(r'[^a-z0-9]+', (s or '').lower()) if t]


def fetch(sb, table, sel):
    out, start = [], 0
    while True:
        d = sb.table(table).select(sel).range(start, start + PAGE - 1).execute().data
        if not d:
            break
        out.extend(d)
        if len(d) < PAGE:
            break
        start += PAGE
    return out


def write_matches(sb, ballistics_id, matches):
    """Delete existing matches for this ballistics row, then insert the new set."""
    sb.table('manufacturer_ballistics_listing_matches').delete().eq('ballistics_id', ballistics_id).execute()
    if not matches:
        return 0
    rows = [{'ballistics_id': ballistics_id, 'listing_id': lid, 'match_reason': reason} for lid, reason in matches]
    inserted = 0
    CHUNK = 500
    for i in range(0, len(rows), CHUNK):
        sb.table('manufacturer_ballistics_listing_matches').insert(rows[i:i + CHUNK]).execute()
        inserted += min(CHUNK, len(rows) - i)
    return inserted


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true', help='Compute match counts; no DB writes.')
    args = ap.parse_args()

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    ball = fetch(sb, 'manufacturer_ballistics',
                 'id,external_id,source,brand,sku,product_line,caliber_normalized,grain,bullet_type,muzzle_velocity_fps')
    listings = fetch(sb, 'listings', 'id,manufacturer,caliber_normalized,grain,bullet_type,product_url,retailer_product_id')
    print(f'Ballistics rows: {len(ball)}   Listings: {len(listings)}')
    if not ball:
        print('Nothing to match.')
        return 0

    # index listings for fast candidate lookup
    lidx = defaultdict(list)
    for l in listings:
        lidx[((l['manufacturer'] or '').strip(), l['caliber_normalized'], l['grain'], BT(l['bullet_type']))].append(l)

    def candidates(brand, cal, grain, bullet):
        out, seen = [], set()
        for mfr in BRAND_ALIASES.get(brand, [brand]):
            for blt in BULLET_TYPE_ALIASES.get(bullet, [bullet]):
                for l in lidx.get((mfr, cal, grain, blt), []):
                    if l['id'] not in seen:
                        seen.add(l['id'])
                        out.append(l)
        return out

    def has_keys(r):
        return r['brand'] and r['caliber_normalized'] and r['grain'] is not None and r['bullet_type']

    # group + collision detection
    grp = defaultdict(list)
    for r in ball:
        if has_keys(r):
            grp[(r['brand'], r['caliber_normalized'], r['grain'], BT(r['bullet_type']))].append(r)
    collision_keys = {k for k, rs in grp.items() if len({r['muzzle_velocity_fps'] for r in rs}) >= 2}

    # ---- PASS 2 resolution: per-listing -> chosen ballistics row id (or held) ----
    resolved = {}          # listing_id -> (ballistics_id, reason)
    held_noline = held_ambig = 0
    for key in collision_keys:
        rows = grp[key]
        tokvel, is_sku, tok_row = defaultdict(set), {}, {}
        for r in rows:
            ks = []
            ns = norm(r['sku'])
            if ns and len(ns) >= 4:
                ks.append((ns, True))
            for t in toks(r['product_line']):
                if t not in STOP and len(t) >= 3:
                    ks.append((t, False))
            for k, sk in ks:
                tokvel[k].add(r['muzzle_velocity_fps'])
                is_sku[k] = is_sku.get(k, False) or sk
                tok_row.setdefault(k, r)
        distinct = {k: next(iter(v)) for k, v in tokvel.items() if len(v) == 1}
        sku_keys = sorted([k for k in distinct if is_sku[k]], key=len, reverse=True)
        kw_keys = sorted([k for k in distinct if not is_sku[k]], key=len, reverse=True)

        for l in candidates(*key):
            nblob = norm(l['product_url']) + norm(l['retailer_product_id'])
            # (a) SKU pass
            acc = []
            for k in sku_keys:
                if any(k in a for a in acc):
                    continue
                if (any(c.isalpha() for c in k) or len(k) >= 5) and k in nblob:
                    acc.append(k)
            vels = {distinct[k] for k in acc}
            if len(vels) == 1:
                resolved[l['id']] = (tok_row[acc[0]]['id'], f"line-resolved sku={tok_row[acc[0]]['sku']}")
            elif len(vels) >= 2:
                held_ambig += 1
            else:
                # (b) keyword pass
                kacc = []
                for k in kw_keys:
                    if any(k in a for a in kacc):
                        continue
                    if k in nblob:
                        kacc.append(k)
                kvels = {distinct[k] for k in kacc}
                if len(kvels) == 1:
                    resolved[l['id']] = (tok_row[kacc[0]]['id'], f"line-resolved kw={kacc[0]}")
                elif len(kvels) >= 2:
                    held_ambig += 1
                else:
                    held_noline += 1

    # ---- per-row match sets + write ----
    grand_total = 0
    skipped_keys = 0
    collision_pairs = 0
    for r in ball:
        missing = [k for k in ('brand', 'caliber_normalized', 'grain', 'bullet_type') if r.get(k) is None]
        if missing:
            skipped_keys += 1
            print(f'\n[id={r["id"]}] {r["source"]:<10} sku={r["sku"]!r:<12} SKIPPED - missing join key(s): {missing}')
            continue
        key = (r['brand'], r['caliber_normalized'], r['grain'], BT(r['bullet_type']))
        cands = candidates(*key)
        if key in collision_keys:
            matches = [(l['id'], resolved[l['id']][1]) for l in cands
                       if l['id'] in resolved and resolved[l['id']][0] == r['id']]
            collision_pairs += len(matches)
            tag = f'COLLISION line-aware'
        else:
            reason = f"brand={r['brand']} cal={r['caliber_normalized']} gr={r['grain']} bullet={r['bullet_type']}"
            matches = [(l['id'], reason) for l in cands]
            tag = 'pass-1'
        print(f'\n[id={r["id"]}] {r["source"]:<10} sku={r["sku"]!r:<12} cal={r["caliber_normalized"]!r:<6} '
              f'gr={r["grain"]:>3} bullet={r["bullet_type"]!r} vel={r["muzzle_velocity_fps"]} [{tag}]  matched={len(matches)}')
        if not args.dry_run:
            written = write_matches(sb, r['id'], matches)
            grand_total += written
        else:
            grand_total += len(matches)

    mode = 'DRY RUN' if args.dry_run else 'LIVE'
    print(f'\nDone ({mode}). {grand_total} listing match(es) {"would be " if args.dry_run else ""}written. '
          f'{skipped_keys} ballistics row(s) skipped for missing join keys.')
    print(f'  collision combos: {len(collision_keys)}  | collision listings line-resolved (pairs): {collision_pairs}  '
          f'| held: no-line={held_noline} ambiguous={held_ambig}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
