"""Condense price_history older than 30 days to one row per listing per day.

Retention policy:
  - Last 30 days:  keep every 2-hour scrape row at full resolution
  - Older rows:    roll up to a single row per (listing_id, UTC day)
                   whose price_per_round and price are daily means

Runs weekly (Sunday midnight via .github/workflows/condense.yml).

Flow:
  1. Pull every price_history row with recorded_at < cutoff_30d AND
     is_condensed = FALSE.
  2. Group by listing_id + UTC-day-of(recorded_at).
  3. For each group, compute mean(price_per_round), mean(price), and
     OR-together in_stock.
  4. Insert one condensed row per group with is_condensed = TRUE,
     recorded_at pinned to midnight UTC of that day.
  5. Delete every source row in the group.
  6. Log totals.

Passing --dry-run prints what would happen without writing anything.

Idempotent: because the condensed row has is_condensed=TRUE, a
second pass sees zero raw rows to roll up and no-ops.

Required env:
  SUPABASE_URL, SUPABASE_KEY
"""

import argparse
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from statistics import mean

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

PAGE = 1000
RETENTION_DAYS = 30


def _fetch_stale_rows_with_flag(cutoff_iso):
    rows, start = [], 0
    while True:
        batch = (
            supabase.table('price_history')
            .select('id,listing_id,price,price_per_round,in_stock,recorded_at,is_condensed')
            .lt('recorded_at', cutoff_iso)
            .eq('is_condensed', False)
            .range(start, start + PAGE - 1)
            .execute()
            .data
        )
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < PAGE:
            break
        start += PAGE
    return rows


def _fetch_stale_rows_no_flag(cutoff_iso):
    """Fallback used when is_condensed column doesn't exist yet
    (first run before migration 004 is applied). Every row older than
    the cutoff is considered raw since no condense pass has run."""
    rows, start = [], 0
    while True:
        batch = (
            supabase.table('price_history')
            .select('id,listing_id,price,price_per_round,in_stock,recorded_at')
            .lt('recorded_at', cutoff_iso)
            .range(start, start + PAGE - 1)
            .execute()
            .data
        )
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < PAGE:
            break
        start += PAGE
    return rows


def fetch_stale_rows(cutoff_iso):
    try:
        return _fetch_stale_rows_with_flag(cutoff_iso), True
    except Exception as e:
        msg = str(e).lower()
        if 'is_condensed' in msg and 'does not exist' in msg:
            print("  [warn] is_condensed column missing — run migrations/004.")
            print("         falling back to flag-less query (safe on first run).")
            return _fetch_stale_rows_no_flag(cutoff_iso), False
        raise


def day_key(iso_ts):
    """Return the UTC date string (YYYY-MM-DD) for an ISO timestamp."""
    dt = datetime.fromisoformat(iso_ts.replace('Z', '+00:00')).astimezone(timezone.utc)
    return dt.strftime('%Y-%m-%d')


def day_midnight_iso(day_str):
    """Midnight UTC for a YYYY-MM-DD string, as ISO."""
    return datetime.strptime(day_str, '%Y-%m-%d').replace(tzinfo=timezone.utc).isoformat()


def safe_mean(values):
    cleaned = [float(v) for v in values if v is not None]
    return mean(cleaned) if cleaned else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true',
                        help="Print what would be condensed; don't write or delete anything.")
    args = parser.parse_args()

    cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).isoformat()
    print(f"Condensing price_history rows older than {cutoff} (>{RETENTION_DAYS}d)")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")

    rows, has_flag = fetch_stale_rows(cutoff)
    print(f"Stale uncondensed rows found: {len(rows)}")
    if not rows:
        print("Nothing to do.")
        return 0

    # (listing_id, day) -> list of source rows
    groups = defaultdict(list)
    for r in rows:
        groups[(r['listing_id'], day_key(r['recorded_at']))].append(r)
    print(f"Distinct (listing_id, day) groups: {len(groups)}")

    inserts_attempted = 0
    deletes_attempted = 0
    inserts_ok = 0
    deletes_ok = 0

    for (listing_id, day), batch in groups.items():
        condensed = {
            'listing_id': listing_id,
            'price': round(safe_mean([r.get('price') for r in batch]) or 0, 4),
            'price_per_round': round(safe_mean([r.get('price_per_round') for r in batch]) or 0, 6),
            'in_stock': any(bool(r.get('in_stock')) for r in batch),
            'recorded_at': day_midnight_iso(day),
        }
        if has_flag:
            condensed['is_condensed'] = True
        inserts_attempted += 1
        deletes_attempted += len(batch)

        if args.dry_run:
            if inserts_attempted <= 5:
                print(f"  [dry] listing={listing_id} day={day} "
                      f"rows_in={len(batch)} avg_ppr=${condensed['price_per_round']}")
            continue

        try:
            supabase.table('price_history').insert(condensed).execute()
            inserts_ok += 1
        except Exception as e:
            print(f"  insert failed for listing={listing_id} day={day}: {e}")
            continue

        ids_to_delete = [r['id'] for r in batch]
        try:
            supabase.table('price_history').delete().in_('id', ids_to_delete).execute()
            deletes_ok += len(ids_to_delete)
        except Exception as e:
            print(f"  delete failed for listing={listing_id} day={day}: {e}")

        if inserts_ok % 100 == 0:
            print(f"  progress: {inserts_ok} groups condensed, {deletes_ok} rows deleted")

    if args.dry_run:
        print(f"\nDRY RUN complete. Would insert {inserts_attempted} condensed rows, "
              f"delete {deletes_attempted} source rows.")
    else:
        print(f"\nDone. Inserted {inserts_ok}/{inserts_attempted} condensed rows; "
              f"deleted {deletes_ok}/{deletes_attempted} source rows.")

    return 0


if __name__ == '__main__':
    sys.exit(main())
