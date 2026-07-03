"""Thin price_history older than 30 days to the rows the /history chart uses.

VERBATIM FLOOR-KEEP (replaces the 2026-05 mean-rollup, which averaged a
day's prices and OR'd in_stock — a synthetic price the customer never saw
could feed the chart's floor; see DECISIONS.md 2026-07-02).

Retention policy:
  - Last 30 days:   never touched (five consumers read 30d windows).
  - TIER 1 (>30d):  per (listing_id, UTC day) KEEP, VERBATIM, the row(s)
                    the /history chart would draw:
                      * the cheapest row with in_stock=true and
                        price_per_round > 0 (tie -> earliest recorded_at,
                        then lowest id), AND
                      * if different, the cheapest such row at/above the
                        caliber's chart floor (PER_CALIBER_FLOOR twin) —
                        guards the rare junk-low day where the global min
                        is below the sanity floor the chart applies.
                    If the day has NO chart-eligible row, keep the day's
                    LAST row verbatim as an out-of-stock presence marker.
                    DELETE everything else. Nothing is inserted, averaged,
                    or synthesized; kept rows keep price, timestamp, id.
                    Idempotent: re-running keeps the same rows, deletes 0.
  - TIER 2 (>90d):  delete listing-days with NO in-stock observation at
                    all (the presence markers). Never deletes a row with
                    in_stock=true — asserted, not assumed.

Chart floors are parsed from migrations/gen/caliber_floors.values.sql
(GENERATED from calibers.json — the registry-derived twin of
ammoraccoon-web/lib/priceBounds.js PER_CALIBER_FLOOR). No hand-written
floor literals here, per the registry doctrine. Listings whose caliber is
absent from that table get floor 0.0 (they can never be charted — the
/history caliber filter only offers registry calibers — so the global-min
keep alone is exact for them).

SAFETY RAILS (every run):
  1. DRY-RUN BY DEFAULT. --execute is required to delete, and --execute
     additionally requires --ids-file pointing at the delete-set this
     script wrote in a prior dry-run AND that the external parity gate
     (ammoraccoon-web scripts/probe-condense-floorkeep-parity.mjs) has
     PASSED on that exact file. Execute deletes exactly those ids —
     nothing is re-derived between gate and delete.
  2. Volume sanity band: abort if the delete-set exceeds an absolute cap
     or a fraction of examined rows (June-prune-guard pattern).
  3. In-memory invariants after planning (see _check_invariants).
  4. Execute mode spot-verifies random processed listing-days afterward.
  5. Orphan rows (listing_id null or no longer in listings) are never
     deleted (conservative; matches the June 2026 one-off prune).

Discord: posts a summary to DISCORD_WEBHOOK_URL if set (silent skip if not).

Required env: SUPABASE_URL, SUPABASE_KEY.
"""

import argparse
import gzip
import json
import os
import random
import re
import sys
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

PAGE = 1000
DELETE_BATCH = 500
RETENTION_DAYS = 30
TIER2_DAYS = 90

# Volume band: measured tier-1 redundancy is ~10% of >30d rows (~45K today);
# steady-state weekly tier-2 marker expiry is ~60K. Anything past these means
# a logic bug, not a big week.
MAX_DELETE_ABS = 150_000
MAX_DELETE_FRAC = 0.35

FLOORS_SQL = Path(__file__).resolve().parent.parent / 'migrations' / 'gen' / 'caliber_floors.values.sql'


def load_chart_floors():
    """Parse the registry-generated caliber_floors VALUES block."""
    text = FLOORS_SQL.read_text(encoding='utf-8')
    floors = {m.group(1): float(m.group(2))
              for m in re.finditer(r"\('([^']+)',\s*([0-9.]+)", text)}
    if not floors:
        raise RuntimeError(f"no floors parsed from {FLOORS_SQL} — registry artifact moved?")
    return floors


def ts_of(row):
    return datetime.fromisoformat(row['recorded_at'].replace('Z', '+00:00'))


def ppr_of(row):
    try:
        v = float(row['price_per_round'])
    except (TypeError, ValueError):
        return None
    return v if v == v and v not in (float('inf'), float('-inf')) else None


def eligible(row):
    """Mirror of the chart's row gate: in_stock===true AND finite ppr>0
    (HistoryClient.js buildPointsFromRows; the SQL fetch already filters
    in_stock=true, buildPoints re-checks — we mirror the union)."""
    p = ppr_of(row)
    return row.get('in_stock') is True and p is not None and p > 0


def fetch_listing_calibers():
    out = {}
    last = 0
    while True:
        batch = (supabase.table('listings')
                 .select('id,caliber_normalized')
                 .gt('id', last).order('id').limit(PAGE)
                 .execute().data)
        if not batch:
            break
        for l in batch:
            out[l['id']] = l.get('caliber_normalized')
        last = batch[-1]['id']
        if len(batch) < PAGE:
            break
    return out


def fetch_stale_rows(cutoff_iso):
    """Keyset-paginate ALL price_history rows older than the cutoff
    (including is_condensed ones — the keep-rule is idempotent over them)."""
    rows = []
    last = 0
    while True:
        batch = (supabase.table('price_history')
                 .select('id,listing_id,price,price_per_round,in_stock,recorded_at,is_condensed')
                 .lt('recorded_at', cutoff_iso)
                 .gt('id', last).order('id').limit(PAGE)
                 .execute().data)
        if not batch:
            break
        rows.extend(batch)
        last = batch[-1]['id']
        if len(batch) < PAGE:
            break
        if len(rows) % 50_000 < PAGE:
            print(f"  fetched {len(rows)} stale rows...")
    return rows


def plan(rows, calibers, floors, tier2_day):
    """Pure planning pass — decides keeps/deletes, touches nothing."""
    groups = defaultdict(list)
    orphan_rows = 0
    for r in rows:
        lid = r['listing_id']
        if lid is None or lid not in calibers:
            orphan_rows += 1
            continue
        groups[(lid, r['recorded_at'][:10])].append(r)

    tier1_ids, tier2_ids = [], []
    kept = {}            # (lid, day) -> list of kept rows (for invariants)
    stats = defaultdict(int)

    for (lid, day), batch in groups.items():
        elig = [r for r in batch if eligible(r)]
        if elig:
            key_min = lambda r: (ppr_of(r), ts_of(r), r['id'])
            best = min(elig, key=key_min)
            keeps = {best['id']: best}
            floor = floors.get(calibers.get(lid) or '', 0.0)
            above = [r for r in elig if ppr_of(r) >= floor]
            if above:
                best_above = min(above, key=key_min)
                keeps[best_above['id']] = best_above
            if len(keeps) > 1:
                stats['dual_keep_days'] += 1
            tier1_ids.extend(r['id'] for r in batch if r['id'] not in keeps)
            kept[(lid, day)] = list(keeps.values())
        else:
            has_instock = any(r.get('in_stock') is True for r in batch)
            if day < tier2_day and not has_instock:
                tier2_ids.extend(r['id'] for r in batch)
                kept[(lid, day)] = []
                stats['tier2_days'] += 1
            else:
                marker = max(batch, key=lambda r: (ts_of(r), r['id']))
                tier1_ids.extend(r['id'] for r in batch if r['id'] != marker['id'])
                kept[(lid, day)] = [marker]
                stats['marker_days'] += 1

    stats['groups'] = len(groups)
    stats['orphan_rows'] = orphan_rows
    return groups, kept, tier1_ids, tier2_ids, stats


def _check_invariants(groups, kept, tier1_ids, tier2_ids, examined, orphan_rows):
    """Abort-worthy self-checks on the plan. Raises AssertionError."""
    t1, t2 = set(tier1_ids), set(tier2_ids)
    assert not (t1 & t2), "tier1/tier2 overlap"
    by_id = {r['id']: r for batch in groups.values() for r in batch}
    # never delete an in-stock row in tier 2
    assert all(by_id[i].get('in_stock') is not True for i in t2), \
        "tier2 would delete an in_stock=true row"
    kept_total = 0
    for gkey, batch in groups.items():
        keeps = kept[gkey]
        kept_total += len(keeps)
        deleted_here = sum(1 for r in batch if r['id'] in t1 or r['id'] in t2)
        assert deleted_here + len(keeps) == len(batch), f"row accounting broke for {gkey}"
        elig = [r for r in batch if eligible(r)]
        if elig:
            assert 1 <= len(keeps) <= 2, f"eligible group kept {len(keeps)} rows: {gkey}"
            want = min(ppr_of(r) for r in elig)
            got = min(ppr_of(r) for r in keeps)
            assert got == want, f"kept min {got} != group min {want} for {gkey}"
        else:
            assert len(keeps) <= 1, f"marker group kept {len(keeps)} rows: {gkey}"
    assert len(t1) + len(t2) + kept_total + orphan_rows == examined, \
        "examined != deletes + keeps + orphans"


def discord(msg):
    url = os.environ.get('DISCORD_WEBHOOK_URL')
    if not url:
        return
    try:
        req = urllib.request.Request(
            url, data=json.dumps({'content': msg}).encode(),
            headers={'Content-Type': 'application/json'})
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print(f"  [warn] discord post failed: {e}")


def table_size_note():
    try:
        resp = supabase.table('price_history').select('id', count='exact').limit(1).execute()
        return f"{resp.count} rows"
    except Exception:
        return "row count unavailable"


def do_execute(ids_file, tier1_planned, tier2_planned, groups, kept):
    """Delete EXACTLY the gated ids, then spot-verify processed groups."""
    with gzip.open(ids_file, 'rt', encoding='utf-8') as f:
        payload = json.load(f)
    file_t1, file_t2 = payload['tier1'], payload['tier2']
    if sorted(file_t1) != sorted(tier1_planned) or sorted(file_t2) != sorted(tier2_planned):
        print("ABORT: the ids-file does not match this run's freshly derived plan.")
        print("       (DB changed since the gated dry-run — re-run dry-run + parity gate.)")
        return 1
    all_ids = file_t1 + file_t2
    deleted = 0
    for i in range(0, len(all_ids), DELETE_BATCH):
        chunk = all_ids[i:i + DELETE_BATCH]
        supabase.table('price_history').delete().in_('id', chunk).execute()
        deleted += len(chunk)
        if (i // DELETE_BATCH) % 40 == 0:
            print(f"  deleted {deleted}/{len(all_ids)}")
    print(f"Deleted {deleted} rows.")

    # spot-verify: surviving rows of sampled groups == predicted keeps
    sample = random.sample(list(kept.keys()), min(200, len(kept)))
    bad = 0
    for (lid, day) in sample:
        live = (supabase.table('price_history')
                .select('id')
                .eq('listing_id', lid)
                .gte('recorded_at', f'{day}T00:00:00+00:00')
                .lt('recorded_at', f'{day}T23:59:59.999999+00:00')
                .execute().data)
        want = sorted(r['id'] for r in kept[(lid, day)])
        got = sorted(r['id'] for r in live)
        if want != got:
            bad += 1
            print(f"  [verify-fail] listing={lid} day={day} want={want} got={got}")
    print(f"Spot-verify: {len(sample) - bad}/{len(sample)} sampled listing-days match prediction.")
    return 0 if bad == 0 else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--execute', action='store_true',
                    help='Actually delete. Requires --ids-file from a parity-gated dry-run.')
    ap.add_argument('--ids-file', help='Delete-set gz written by the dry-run and passed through the parity gate.')
    ap.add_argument('--out-dir', default='condense_out', help='Where dry-run artifacts are written.')
    args = ap.parse_args()

    if args.execute and not args.ids_file:
        print("ABORT: --execute requires --ids-file (the parity-gated delete-set).")
        return 1

    now = datetime.now(timezone.utc)
    cutoff_day = (now - timedelta(days=RETENTION_DAYS)).strftime('%Y-%m-%d')
    tier2_day = (now - timedelta(days=TIER2_DAYS)).strftime('%Y-%m-%d')
    cutoff_iso = f'{cutoff_day}T00:00:00+00:00'   # whole UTC days only
    print(f"Floor-keep condense — processing complete UTC days < {cutoff_day} "
          f"(tier 2 marker expiry: days < {tier2_day})")
    print(f"Mode: {'EXECUTE' if args.execute else 'DRY RUN (nothing will be deleted)'}")

    floors = load_chart_floors()
    print(f"Chart floors loaded from registry artifact: {len(floors)} calibers")

    calibers = fetch_listing_calibers()
    print(f"Listings mapped: {len(calibers)}")

    rows = fetch_stale_rows(cutoff_iso)
    examined = len(rows)
    print(f"Stale rows examined: {examined}")
    if not rows:
        print("Nothing to do.")
        return 0

    groups, kept, tier1_ids, tier2_ids, stats = plan(rows, calibers, floors, tier2_day)
    deletes = len(tier1_ids) + len(tier2_ids)
    kept_rows = examined - deletes - stats['orphan_rows']

    print(f"\nPlan:")
    print(f"  (listing, day) groups:        {stats['groups']}")
    print(f"  orphan rows (never deleted):  {stats['orphan_rows']}")
    print(f"  TIER 1 deletes (>30d thin):   {len(tier1_ids)}")
    print(f"  TIER 2 deletes (>90d marker): {len(tier2_ids)}  (marker days remaining: {stats['marker_days']})")
    print(f"  dual-keep days (junk-low):    {stats['dual_keep_days']}")
    print(f"  rows kept:                    {kept_rows}")

    # ---- sanity band ----
    frac = deletes / examined if examined else 0.0
    if deletes > MAX_DELETE_ABS or frac > MAX_DELETE_FRAC:
        msg = (f"ABORT — delete volume outside sanity band: {deletes} rows "
               f"({frac:.1%} of examined; caps {MAX_DELETE_ABS} / {MAX_DELETE_FRAC:.0%})")
        print(msg)
        discord(f"price_history condense ABORTED (sanity band): {msg}")
        return 2
    print(f"  sanity band: OK ({deletes} deletes = {frac:.1%} of examined)")

    # ---- invariants ----
    _check_invariants(groups, kept, tier1_ids, tier2_ids, examined, stats['orphan_rows'])
    print("  invariants: OK (accounting, min-preserved, no in-stock row in tier 2)")

    if args.execute:
        rc = do_execute(args.ids_file, tier1_ids, tier2_ids, groups, kept)
        verdict = 'OK' if rc == 0 else 'VERIFY-FAIL'
        discord(f"price_history condense EXECUTED: examined {examined}, kept {kept_rows}, "
                f"deleted {deletes} (t1 {len(tier1_ids)} / t2 {len(tier2_ids)}). "
                f"Spot-verify {verdict}. Table now: {table_size_note()}.")
        return rc

    # ---- dry-run artifacts for the parity gate ----
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    ids_path = out / 'condense_delete_ids.json.gz'
    with gzip.open(ids_path, 'wt', encoding='utf-8') as f:
        json.dump({'tier1': tier1_ids, 'tier2': tier2_ids}, f)
    summary = {
        'generated_at': now.isoformat(),
        'cutoff_day': cutoff_day, 'tier2_day': tier2_day,
        'examined': examined, 'groups': stats['groups'],
        'orphan_rows': stats['orphan_rows'],
        'tier1_deletes': len(tier1_ids), 'tier2_deletes': len(tier2_ids),
        'kept_rows': kept_rows, 'dual_keep_days': stats['dual_keep_days'],
        'marker_days': stats['marker_days'], 'tier2_days': stats['tier2_days'],
    }
    (out / 'condense_dryrun_summary.json').write_text(json.dumps(summary, indent=2))
    print(f"\nDRY RUN complete — nothing deleted.")
    print(f"  delete-set: {ids_path}")
    print(f"  summary:    {out / 'condense_dryrun_summary.json'}")
    print(f"Next: run the parity gate on the delete-set, then (on approval)")
    print(f"  python scripts/condense_history.py --execute --ids-file {ids_path}")
    discord(f"price_history condense DRY RUN: examined {examined}, would keep {kept_rows}, "
            f"would delete {deletes} (t1 {len(tier1_ids)} / t2 {len(tier2_ids)}). Awaiting parity gate.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
