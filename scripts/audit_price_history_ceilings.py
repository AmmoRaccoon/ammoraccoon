"""Read-only audit of price_history rows that violate per-caliber ceilings.

Prints, but does not modify, any data. Intended as a pre-check before any
retroactive cleanup of price_history rows whose price_per_round is above
the ceiling defined in scraper_lib.CALIBER_PRICE_CEILINGS (forward gate
used by scrapers to reject misparsed rows at insert time).

Reports:
  1. Per-caliber count of price_history rows whose price_per_round exceeds
     the caliber's ceiling, plus a sample of up to SAMPLE_SIZE rows.
  2. Count of orphaned price_history rows whose listing_id no longer
     exists in listings, plus a sample.

Required env:
  SUPABASE_URL, SUPABASE_KEY
"""

import os
import sys
from collections import defaultdict

from dotenv import load_dotenv
from supabase import create_client

# scraper_lib lives in the repo root (one level up from scripts/).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scraper_lib import ceiling_for_caliber, DEFAULT_CEILING  # noqa: E402

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

PAGE_SIZE = 1000
SAMPLE_SIZE = 5


def stream_price_history(sb):
    """Yield every price_history row joined with listings(caliber_normalized).

    Rows whose listings join is None are kept — those are the orphans.
    """
    start = 0
    while True:
        end = start + PAGE_SIZE - 1
        batch = (
            sb.table('price_history')
            .select('id,listing_id,price_per_round,recorded_at,listings(caliber_normalized)')
            .range(start, end)
            .execute()
            .data
        )
        if not batch:
            return
        for row in batch:
            yield row
        if len(batch) < PAGE_SIZE:
            return
        start += PAGE_SIZE


def main():
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    total_rows = 0
    over_ceiling_by_caliber = defaultdict(list)  # caliber -> list of rows
    orphan_rows = []

    for row in stream_price_history(sb):
        total_rows += 1
        joined = row.get('listings')
        listing_id = row.get('listing_id')
        ppr = row.get('price_per_round')

        if joined is None:
            # listing_id points at a row that no longer exists.
            orphan_rows.append(row)
            continue

        if ppr is None:
            continue
        try:
            p = float(ppr)
        except (TypeError, ValueError):
            continue

        caliber = joined.get('caliber_normalized')
        ceiling = ceiling_for_caliber(caliber)
        if p > ceiling:
            key = caliber if caliber else '(null caliber)'
            over_ceiling_by_caliber[key].append(row)

    print(f"Scanned {total_rows} price_history rows.\n")

    # --- over-ceiling report ---
    print("=" * 72)
    print("Rows over per-caliber price_per_round ceiling")
    print("=" * 72)
    print(f"{'caliber':<14} {'ceiling':>9} {'count':>7}  sample (ppr / listing_id / recorded_at)")
    print("-" * 72)

    total_over = 0
    for caliber in sorted(over_ceiling_by_caliber.keys()):
        rows = over_ceiling_by_caliber[caliber]
        ceiling = ceiling_for_caliber(caliber if caliber != '(null caliber)' else None)
        total_over += len(rows)
        print(f"{caliber:<14} ${ceiling:>7.2f} {len(rows):>7}")
        for r in rows[:SAMPLE_SIZE]:
            print(f"    ppr=${float(r['price_per_round']):.4f}  "
                  f"listing_id={r['listing_id']}  "
                  f"recorded_at={r['recorded_at']}")
    print("-" * 72)
    print(f"Total over-ceiling rows: {total_over}")
    print(f"(Default ceiling for unrecognized calibers: ${DEFAULT_CEILING:.2f})\n")

    # --- orphan report ---
    print("=" * 72)
    print("Orphaned price_history rows (listing_id not in listings)")
    print("=" * 72)
    print(f"Total orphans: {len(orphan_rows)}")
    for r in orphan_rows[:SAMPLE_SIZE]:
        print(f"    id={r['id']}  listing_id={r['listing_id']}  "
              f"ppr={r.get('price_per_round')}  recorded_at={r['recorded_at']}")
    if len(orphan_rows) > SAMPLE_SIZE:
        print(f"    ... and {len(orphan_rows) - SAMPLE_SIZE} more")
    print()

    print("READ-ONLY AUDIT COMPLETE — no rows were modified.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
