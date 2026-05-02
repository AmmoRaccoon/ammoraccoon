"""Backfill bullet_type on rows where it's NULL but the new parser can derive one.

Default mode is dry-run — counts the rows that would change, breaks the
result down by retailer and target bullet_type, and prints a sample of
30 rows. Pass `--apply` to actually issue the updates.

The listings table has no `name` / `title` column, so we feed the new
`scraper_lib.parse_bullet_type` the `product_url` (which is normalized
to space-separated tokens by the parser anyway). This is exactly what
the slug-fallback inside scrapers does at scrape time.

Skips Gorilla (retailer_id=29) by request — Gorilla's source data
doesn't expose bullet type at all, so any value we'd derive from a slug
that just says "9mm-147gr" would be a guess.

Usage:
    python scripts/backfill_bullet_type.py             # dry-run (default)
    python scripts/backfill_bullet_type.py --apply     # write changes
"""
import argparse
import os
import sys
from collections import defaultdict, Counter
from dotenv import load_dotenv
from supabase import create_client

# Run from repo root so `scraper_lib` is importable.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scraper_lib import parse_bullet_type, BULLET_TYPES  # noqa: E402

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

SKIP_RETAILER_IDS = {29}  # Gorilla — source data has no bullet-type info.


def fetch_all(table, select, **filters):
    """Page through Supabase results since the default cap is 1000."""
    rows = []
    offset = 0
    while True:
        q = sb.table(table).select(select)
        for k, v in filters.items():
            if v is None:
                q = q.is_(k, 'null')
            else:
                q = q.eq(k, v)
        r = q.range(offset, offset + 999).execute()
        if not r.data:
            break
        rows.extend(r.data)
        if len(r.data) < 1000:
            break
        offset += 1000
    return rows


def collect_candidates():
    """Return list of (id, retailer_id, product_url, derived_bullet_type)
    for every NULL row where the new parser produces a non-None value."""
    print("Fetching NULL bullet_type listings...")
    null_rows = fetch_all('listings', 'id,retailer_id,product_url', bullet_type=None)
    print(f"  {len(null_rows)} listings with NULL bullet_type")

    candidates = []
    skipped_no_url = 0
    skipped_excluded_retailer = 0
    skipped_no_match = 0
    for r in null_rows:
        if r['retailer_id'] in SKIP_RETAILER_IDS:
            skipped_excluded_retailer += 1
            continue
        url = r.get('product_url')
        if not url:
            skipped_no_url += 1
            continue
        bt = parse_bullet_type(url)
        if bt is None:
            skipped_no_match += 1
            continue
        if bt not in BULLET_TYPES:
            # Defensive — the parser should never emit anything outside
            # BULLET_TYPES, but DB constraint mismatch would be a worse
            # silent failure than skipping here.
            skipped_no_match += 1
            continue
        candidates.append((r['id'], r['retailer_id'], url, bt))

    print(f"  {len(candidates)} would update")
    print(f"  {skipped_no_match} no parser match (real ambiguity in slug)")
    print(f"  {skipped_no_url} no product_url")
    print(f"  {skipped_excluded_retailer} excluded retailer (Gorilla)")
    return candidates


def report(candidates):
    """Print the dry-run report: per-retailer breakdown + bullet-type
    distribution + 30-row sample with full URLs."""
    if not candidates:
        print("\nNothing to update.")
        return

    # Pull retailer name lookup (small table, no pagination concern).
    retailers = {r['id']: r['slug']
                 for r in sb.table('retailers').select('id,slug').execute().data}

    by_retailer = defaultdict(Counter)
    by_type = Counter()
    for _, rid, _, bt in candidates:
        by_retailer[rid][bt] += 1
        by_type[bt] += 1

    print("\n" + "=" * 78)
    print(f"DRY-RUN — {len(candidates)} rows would be updated")
    print("=" * 78)

    print("\nBy target bullet_type (across all retailers):")
    for bt, n in by_type.most_common():
        print(f"  {bt:<12}  {n:>5}")

    print("\nBy retailer:")
    print(f"  {'id':>3}  {'slug':<22}  {'rows':>5}  type-breakdown")
    print(f"  {'-'*3}  {'-'*22}  {'-'*5}  {'-'*40}")
    for rid in sorted(by_retailer.keys(),
                      key=lambda k: -sum(by_retailer[k].values())):
        slug = retailers.get(rid, '?')
        types = by_retailer[rid]
        total = sum(types.values())
        breakdown = ', '.join(f'{bt}={n}' for bt, n in types.most_common())
        print(f"  {rid:>3}  {slug:<22}  {total:>5}  {breakdown}")

    print("\n30-row sample (across retailers, alternating types):")
    # Take a few rows from each (retailer, bullet_type) combination so
    # the sample shows breadth instead of 30 of the same pattern.
    seen_combo = set()
    sample = []
    for cid, rid, url, bt in candidates:
        key = (rid, bt)
        if key in seen_combo and len(sample) < 30:
            continue
        seen_combo.add(key)
        sample.append((cid, rid, url, bt))
        if len(sample) >= 30:
            break
    for cid, rid, url, bt in sample:
        slug = retailers.get(rid, '?')[:18]
        print(f"  id={cid:>5} [{slug:<18}] -> {bt:<10}  {url}")


def apply_updates(candidates):
    """Issue the updates in batches grouped by target bullet_type so we
    use one SQL statement per bullet_type instead of one per row."""
    if not candidates:
        print("\nNothing to apply.")
        return

    # Group ids by target value.
    groups = defaultdict(list)
    for cid, _, _, bt in candidates:
        groups[bt].append(cid)

    print(f"\nAPPLY — issuing {len(groups)} grouped updates "
          f"covering {len(candidates)} rows...")
    total_written = 0
    # PostgREST limits the URL length, so chunk each group's ID list
    # into batches of 200 to stay well under any URL ceiling.
    BATCH = 200
    for bt, ids in groups.items():
        for i in range(0, len(ids), BATCH):
            chunk = ids[i:i + BATCH]
            r = sb.table('listings').update({'bullet_type': bt}) \
                .in_('id', chunk).execute()
            n = len(r.data) if r.data else 0
            total_written += n
            print(f"  {bt:<10}  ids[{i}:{i+len(chunk)}]  -> {n} rows updated")

    print(f"\nDone. {total_written} rows written.")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--apply', action='store_true',
                        help='Actually write updates (default is dry-run)')
    args = parser.parse_args()

    candidates = collect_candidates()
    report(candidates)
    if args.apply:
        apply_updates(candidates)
    else:
        print("\n[DRY-RUN] No changes written. Re-run with --apply to commit.")


if __name__ == '__main__':
    main()
