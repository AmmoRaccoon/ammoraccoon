"""match_ballistics_to_listings.py — populate manufacturer_ballistics_listing_matches.

For every row in manufacturer_ballistics, finds listings where ALL of the
following are equal on both sides:
    listings.manufacturer       == manufacturer_ballistics.brand
    listings.caliber_normalized == manufacturer_ballistics.caliber_normalized
    listings.grain              == manufacturer_ballistics.grain
    listings.bullet_type        == manufacturer_ballistics.bullet_type
…and records the match in manufacturer_ballistics_listing_matches.
Frontend reads from that table to render velocity / energy / downrange
on individual listing pages.

Unlike the rebate matcher, the join here is a strict 4-column equi-join
(no fuzzy URL match) — manufacturer ballistics are keyed on physical
specs, and listings already carry those same specs. If a listing's
grain or bullet_type is null, it will not match any ballistics row.

A listing CAN match multiple ballistics rows when the manufacturer
publishes the same cartridge under multiple SKUs (e.g., Federal AE9DP
50-pack and AE9DP100 100-pack share specs); the schema allows the
duplicates and the frontend picks one.

Stale-match handling mirrors the rebate matcher: every run deletes the
ballistics row's existing matches and re-inserts the current set, so
a listing that no longer matches (caliber reclassified, grain corrected,
brand re-normalized) drops out cleanly.

Required env:
  SUPABASE_URL, SUPABASE_KEY

Usage:
  python scripts/match_ballistics_to_listings.py --dry-run
  python scripts/match_ballistics_to_listings.py
"""

import argparse
import os
import sys
from collections import defaultdict

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_KEY']

PAGE = 1000

# When a ballistics row carries bullet_type='JHP', also accept listings
# whose bullet_type='HP'. Retailer scrapers that pulled the bullet type
# from a manufacturer's "Gold Dot Hollow Point" / "Hollow Point" title
# wrote 'HP' rather than 'JHP'. Same physical bullet either way.
BULLET_TYPE_ALIASES = {
    'JHP': ['JHP', 'HP'],
}


def fetch_ballistics(sb):
    rows = []
    start = 0
    while True:
        batch = (
            sb.table('manufacturer_ballistics')
            .select('id,external_id,source,brand,sku,product_line,caliber_normalized,grain,bullet_type')
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


def find_matching_listings(sb, brand, caliber, grain, bullet):
    """Listings where brand + caliber + grain + bullet_type all match exactly."""
    matches = []
    start = 0
    while True:
        batch = (
            sb.table('listings')
            .select('id,manufacturer,caliber_normalized,grain,bullet_type')
            .eq('manufacturer', brand)
            .eq('caliber_normalized', caliber)
            .eq('grain', grain)
            .in_('bullet_type', BULLET_TYPE_ALIASES.get(bullet, [bullet]))
            .range(start, start + PAGE - 1)
            .execute()
            .data
        )
        if not batch:
            break
        matches.extend(batch)
        if len(batch) < PAGE:
            break
        start += PAGE
    return matches


def compute_matches_for_ballistics(sb, bal):
    """Return list of (listing_id, reason) for one ballistics row."""
    # Skip rows with any join key null — equality match would never fire.
    if not all([bal.get('brand'), bal.get('caliber_normalized'),
                bal.get('grain') is not None, bal.get('bullet_type')]):
        return []

    listings = find_matching_listings(
        sb, bal['brand'], bal['caliber_normalized'],
        bal['grain'], bal['bullet_type'],
    )
    reason = (
        f"brand={bal['brand']} "
        f"cal={bal['caliber_normalized']} "
        f"gr={bal['grain']} "
        f"bullet={bal['bullet_type']}"
    )
    return [(l['id'], reason) for l in listings]


def write_matches(sb, ballistics_id, matches):
    """Delete existing matches for this ballistics row, then insert the new set."""
    sb.table('manufacturer_ballistics_listing_matches').delete().eq(
        'ballistics_id', ballistics_id,
    ).execute()
    if not matches:
        return 0
    rows = [
        {
            'ballistics_id': ballistics_id,
            'listing_id': lid,
            'match_reason': reason,
        }
        for lid, reason in matches
    ]
    inserted = 0
    CHUNK = 500
    for i in range(0, len(rows), CHUNK):
        sb.table('manufacturer_ballistics_listing_matches').insert(rows[i:i + CHUNK]).execute()
        inserted += min(CHUNK, len(rows) - i)
    return inserted


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true',
                        help='Print match counts; no DB writes.')
    args = parser.parse_args()

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    ballistics = fetch_ballistics(sb)
    print(f'Ballistics rows: {len(ballistics)}')
    if not ballistics:
        print('Nothing to match.')
        return 0

    grand_total = 0
    skipped_keys = 0

    for b in ballistics:
        # Skip-with-warn so the operator can spot incomplete ballistics rows.
        missing = [k for k in ('brand', 'caliber_normalized', 'grain', 'bullet_type')
                   if b.get(k) is None]
        if missing:
            skipped_keys += 1
            print(f'\n[id={b["id"]}] {b["source"]:<10} sku={b["sku"]!r:<12} '
                  f'SKIPPED — missing join key(s): {missing}')
            continue

        print(f'\n[id={b["id"]}] {b["source"]:<10} sku={b["sku"]!r:<12} '
              f'cal={b["caliber_normalized"]!r:<6} gr={b["grain"]:>3} '
              f'bullet={b["bullet_type"]!r}  line={b["product_line"]!r}')

        matches = compute_matches_for_ballistics(sb, b)
        print(f'  matched listings: {len(matches)}')
        for lid, _ in matches[:5]:
            print(f'    listing_id={lid}')
        if len(matches) > 5:
            print(f'    ... and {len(matches) - 5} more')

        if not args.dry_run:
            written = write_matches(sb, b['id'], matches)
            print(f'  wrote {written} rows')
            grand_total += written
        else:
            grand_total += len(matches)

    mode = 'DRY RUN' if args.dry_run else 'LIVE'
    print(f'\nDone ({mode}). {grand_total} listing match(es) '
          f'{"would be " if args.dry_run else ""}written. '
          f'{skipped_keys} ballistics row(s) skipped for missing join keys.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
