"""Backfill manufacturer on listings rows mis-tagged by the pre-fix
parse_brand longest-match bug.

Background: prior to the 2026-05-07 fix, parse_brand resolved by
longest-alias-substring-wins. Caliber names that embed a brand token
(".223 Remington", ".308 Winchester", ".45 Colt", ".357 SIG Sauer")
would out-length the actual manufacturer prefix (e.g. "hornady" 7
chars, "remington" 9 chars), so titles like "Hornady Frontier .223
Remington 55gr FMJ" were saved as Remington. The fixed parse_brand
strips caliber-with-brand patterns before the alias scan; this
backfill re-runs it on every existing listing's product_url and
updates the rows whose manufacturer changes as a result.

Update policy:
  - Only updates manufacturer when the new value is non-None AND
    differs from the current value.
  - Skips rows where the new value is None (don't overwrite a real
    brand with NULL just because the URL slug omits brand info).
  - Does NOT touch any other column.

Default mode is dry-run. Reports per-retailer counts plus a sample of
the rows that would change. Pass --apply to write.

Usage:
  python scripts/backfill_brand_fix.py             # dry-run
  python scripts/backfill_brand_fix.py --apply     # live
  python scripts/backfill_brand_fix.py --sample 30 # show 30 sample diffs
"""
import argparse
import os
import sys
from collections import Counter, defaultdict

from dotenv import load_dotenv
from supabase import create_client

# Run from repo root so `scraper_lib` is importable.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scraper_lib import parse_brand  # noqa: E402

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])


def fetch_all():
    """Page through listings; default cap is 1000 per request."""
    rows, off = [], 0
    while True:
        r = (sb.table('listings')
             .select('id,retailer_id,manufacturer,product_url')
             .range(off, off + 999)
             .execute())
        if not r.data:
            break
        rows.extend(r.data)
        if len(r.data) < 1000:
            break
        off += 1000
    return rows


def collect_changes(rows):
    """Return [(id, retailer_id, current, new, url)] for rows whose
    re-parsed brand differs from the stored value."""
    changes = []
    for r in rows:
        url = r.get('product_url') or ''
        cur = r.get('manufacturer')
        new = parse_brand(url)
        # Skip when re-parse can't find a brand — preserve existing value
        # so we don't blank out manually-curated or scraper-set brands
        # for URLs that happen to omit the brand keyword.
        if new is None:
            continue
        if new == cur:
            continue
        changes.append((r['id'], r['retailer_id'], cur, new, url))
    return changes


def report(changes, sample_n):
    if not changes:
        print('No rows would change.')
        return

    print(f'\n=== {len(changes)} row(s) would have manufacturer updated ===\n')

    # Per-retailer breakdown
    by_retailer = defaultdict(Counter)
    for _, rid, cur, new, _ in changes:
        by_retailer[rid][(cur, new)] += 1

    # Look up retailer slugs for a friendlier print
    retailers = {r['id']: r['slug']
                 for r in sb.table('retailers').select('id,slug').execute().data}

    print('--- per-retailer ---')
    print(f'  {"id":>3}  {"slug":<22}  {"rows":>5}  top transitions')
    print(f'  {"-"*3}  {"-"*22}  {"-"*5}  {"-"*40}')
    for rid in sorted(by_retailer.keys(),
                      key=lambda k: -sum(by_retailer[k].values())):
        slug = retailers.get(rid, '?')
        ts = by_retailer[rid]
        total = sum(ts.values())
        top = ', '.join(f'{(c or "NULL")}->{n}={v}' for (c, n), v in ts.most_common(3))
        print(f'  {rid:>3}  {slug:<22}  {total:>5}  {top}')

    # Aggregate transition counts
    print()
    print('--- transition totals ---')
    trans = Counter((c, n) for _, _, c, n, _ in changes)
    print(f'  {"current":<22}  {"new":<22}  {"count":>5}')
    print(f'  {"-"*22}  {"-"*22}  {"-"*5}')
    for (cur, new), n in trans.most_common(30):
        print(f'  {(cur or "NULL"):<22}  {new:<22}  {n:>5}')
    if len(trans) > 30:
        print(f'  ... and {len(trans) - 30} more transition(s)')

    # Sample
    print()
    print(f'--- {min(sample_n, len(changes))} sample row(s) ---')
    # Take a varied sample: at most 2 rows per (cur, new) pair so we see
    # breadth of transitions instead of N rows of the same transition.
    seen = Counter()
    sample = []
    for cid, rid, cur, new, url in changes:
        key = (cur, new)
        if seen[key] >= 2:
            continue
        seen[key] += 1
        sample.append((cid, rid, cur, new, url))
        if len(sample) >= sample_n:
            break
    for cid, rid, cur, new, url in sample:
        slug = retailers.get(rid, '?')[:18]
        url_short = url[-90:] if len(url) > 90 else url
        print(f'  id={cid:>6} [{slug:<18}] {(cur or "NULL"):<18} -> {new:<18}  ...{url_short}')


def apply_updates(changes):
    """Per-row UPDATE. ~500 rows max — completes in seconds without
    needing the in_('id', chunk) batching trick."""
    written = 0
    for cid, _, _, new, _ in changes:
        sb.table('listings').update({'manufacturer': new}).eq('id', cid).execute()
        written += 1
        if written % 100 == 0:
            print(f'  ...{written} rows written')
    print(f'\nDone. {written} rows updated.')


_BUG_TRIGGER_BRANDS = frozenset({'Remington', 'Winchester', 'Colt', 'Sig Sauer'})


def filter_bug_fix_only(changes):
    """Restrict changes to the caliber-collision bug pattern + clean Unknown fills.

    Includes:
      (a) old in {Remington, Winchester, Colt, Sig Sauer} AND new is different
          — this is the actual bug signature (a caliber name's brand token
          out-lengthed the real manufacturer prefix at the original parse).
      (b) old is NULL/empty/'Unknown' AND new is a real brand — these are
          gap-fills where the original scraper couldn't resolve a brand
          and the alias table has since gained the right entry.

    Excludes by design:
      - Blazer ↔ CCI churn (pre-existing alias ambiguity, separate decision).
      - "Sellier and Bellot" → "Sellier & Bellot" normalization (those rows
        come from scrapers that don't call parse_brand; fix upstream instead).
      - Any other "real brand → real brand" transitions outside the trigger
        set (e.g. Hornady → Underwood — usually correct, but not the bug).
    """
    out = []
    for cid, rid, cur, new, url in changes:
        if cur in _BUG_TRIGGER_BRANDS and new != cur:
            out.append((cid, rid, cur, new, url))
        elif (cur is None or cur == '' or cur == 'Unknown') and new and new != 'Unknown':
            out.append((cid, rid, cur, new, url))
    return out


def main():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--apply', action='store_true',
                   help='Actually write updates (default is dry-run)')
    p.add_argument('--sample', type=int, default=20,
                   help='Number of sample diffs to print (default 20)')
    p.add_argument('--bug-fix-only', action='store_true',
                   help='Restrict to the caliber-collision bug pattern + '
                        'NULL/Unknown -> real brand gap-fills. Excludes '
                        'Blazer/CCI churn and Sellier-and/& normalization.')
    args = p.parse_args()

    print('Fetching all listings...')
    rows = fetch_all()
    print(f'  {len(rows)} rows')

    print('Re-parsing brand on each product_url...')
    changes = collect_changes(rows)
    print(f'  {len(changes)} total candidate change(s)')

    if args.bug_fix_only:
        before = len(changes)
        changes = filter_bug_fix_only(changes)
        print(f'  --bug-fix-only: {len(changes)} after filter '
              f'(excluded {before - len(changes)})')

    report(changes, args.sample)

    if args.apply:
        print('\n' + '=' * 78)
        print('APPLY — writing updates...')
        print('=' * 78)
        apply_updates(changes)
    else:
        print('\n' + '=' * 78)
        print('[DRY-RUN] No changes written. Re-run with --apply to commit.')
        print('=' * 78)


if __name__ == '__main__':
    main()
