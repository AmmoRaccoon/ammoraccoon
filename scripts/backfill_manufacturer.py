"""One-time backfill: populate listings.manufacturer for legacy rows.

Six scrapers used to drop the manufacturer field on the floor. This
script walks every row where manufacturer is NULL, extracts a brand
from the product_url using the same canonical alias table the scrapers
now use, and updates the row in place.

Idempotent — re-running only touches rows that are still NULL.
"""

import os
import sys
import time
from dotenv import load_dotenv
from supabase import create_client

# Make the project root importable so we can reuse scraper_lib.parse_brand.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scraper_lib import parse_brand

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

PAGE = 500


def fetch_null_rows():
    rows = []
    start = 0
    while True:
        end = start + PAGE - 1
        batch = (
            supabase.table('listings')
            .select('id,product_url,caliber')
            .is_('manufacturer', 'null')
            .range(start, end)
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


def main():
    rows = fetch_null_rows()
    print(f"Found {len(rows)} listings with NULL manufacturer")

    updated = 0
    still_null = 0
    for row in rows:
        # Scraped URLs typically include the brand in the slug; title copy
        # sometimes leaks into the caliber field for older rows, so we
        # feed both to the detector.
        text = f"{row.get('product_url') or ''} {row.get('caliber') or ''}"
        brand = parse_brand(text)
        if not brand:
            still_null += 1
            continue
        try:
            supabase.table('listings').update({'manufacturer': brand}).eq('id', row['id']).execute()
            updated += 1
            if updated % 25 == 0:
                print(f"  ...updated {updated}")
        except Exception as e:
            print(f"  failed to update id={row['id']}: {e}")
            still_null += 1

    print(f"\nDone. Updated: {updated} | still null (no brand detected): {still_null}")


if __name__ == '__main__':
    main()
