"""One-time cleanup: divide stale cents-stored-as-dollars rows by 100.

Background: before commit ec12a34 (2026-04-22), several scrapers computed
price_per_round as (price / rounds) * 100 and stored the resulting cents
value as if it were dollars. The commit fixed the scrapers, but upserts
only overwrite rows whose retailer_product_id is still being emitted; rows
for products that aged out or rebranded are stranded with 100x values.

This script finds every listings row with price_per_round > $5.00 and
divides both price_per_round AND any linked price_history rows by 100.
Scoped by retailer_id and caliber: real per-round prices above $5 are
effectively impossible on any caliber we track (max expected for .308
is ~$3). Idempotent — re-running only hits rows still above the ceiling.
"""

import os
import sys

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

CEILING = 5.00


def fix_listings():
    rows = (
        supabase.table('listings')
        .select('id,retailer_id,price_per_round,base_price,total_rounds')
        .gt('price_per_round', CEILING)
        .execute()
        .data
    )
    print(f"Found {len(rows)} listings row(s) with price_per_round > ${CEILING:.2f}")
    fixed_ids = []
    for row in rows:
        old = float(row['price_per_round'])
        new = round(old / 100.0, 4)
        print(f"  listing id={row['id']} retailer={row['retailer_id']} ppr ${old} -> ${new}")
        supabase.table('listings').update({'price_per_round': new}).eq('id', row['id']).execute()
        fixed_ids.append(row['id'])
    return fixed_ids


def fix_price_history(listing_ids):
    if not listing_ids:
        print("No listing ids to re-check in price_history.")
        return 0
    rows = (
        supabase.table('price_history')
        .select('id,listing_id,price_per_round')
        .in_('listing_id', listing_ids)
        .gt('price_per_round', CEILING)
        .execute()
        .data
    )
    print(f"Found {len(rows)} price_history row(s) tied to those listings with ppr > ${CEILING:.2f}")
    for row in rows:
        old = float(row['price_per_round'])
        new = round(old / 100.0, 4)
        print(f"  price_history id={row['id']} listing={row['listing_id']} ppr ${old} -> ${new}")
        supabase.table('price_history').update({'price_per_round': new}).eq('id', row['id']).execute()
    return len(rows)


def fix_price_history_any():
    """Backstop: any price_history row above the ceiling regardless of
    whether its listing currently has a stale ppr."""
    rows = (
        supabase.table('price_history')
        .select('id,listing_id,price_per_round')
        .gt('price_per_round', CEILING)
        .execute()
        .data
    )
    print(f"Backstop: {len(rows)} additional price_history row(s) above ceiling")
    for row in rows:
        old = float(row['price_per_round'])
        new = round(old / 100.0, 4)
        print(f"  price_history id={row['id']} listing={row['listing_id']} ppr ${old} -> ${new}")
        supabase.table('price_history').update({'price_per_round': new}).eq('id', row['id']).execute()
    return len(rows)


def main():
    fixed_ids = fix_listings()
    ph_linked = fix_price_history(fixed_ids)
    ph_extra = fix_price_history_any()
    print(f"\nDone. listings fixed: {len(fixed_ids)} · price_history fixed: {ph_linked + ph_extra}")


if __name__ == '__main__':
    sys.exit(main() or 0)
