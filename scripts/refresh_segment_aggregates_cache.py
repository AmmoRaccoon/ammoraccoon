"""Refresh homepage_segment_aggregates_cache (migration 031).

Calls the refresh_homepage_segment_aggregates_cache() SQL function, which
atomically rebuilds the precompute table from the live
homepage_segment_aggregates() RPC. Runs at the end of every scrape_light.yml
cron tick so the cache follows each scrape wave; the homepage reads the
table (lib/pricing.js fetchSegmentAggregates) and only falls back to the
live RPC when the cache is missing or stale >24h.

The underlying RPC takes ~2.4-2.9s warm but can exceed the 8s service-role
statement_timeout on a cold cache — and a canceled first attempt still
warms shared buffers, so we retry up to 3 times before declaring failure.

Required env: SUPABASE_URL, SUPABASE_KEY (service role).
"""

import os
import sys
import time

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

ATTEMPTS = 3


def main():
    last_err = None
    for attempt in range(1, ATTEMPTS + 1):
        t0 = time.time()
        try:
            res = supabase.rpc('refresh_homepage_segment_aggregates_cache', {}).execute()
            rows = res.data
            elapsed = time.time() - t0
            print(f"cache refreshed: {rows} segment rows in {elapsed:.1f}s "
                  f"(attempt {attempt}/{ATTEMPTS})")
            if not rows:
                print("WARNING: refresh wrote 0 rows — the web fallback path "
                      "will serve the live RPC until the next refresh.")
                return 1
            return 0
        except Exception as e:
            last_err = e
            elapsed = time.time() - t0
            print(f"attempt {attempt}/{ATTEMPTS} failed after {elapsed:.1f}s: {e}")
            time.sleep(2)
    print(f"refresh FAILED after {ATTEMPTS} attempts: {last_err}")
    print("(cache keeps its previous rows; web falls back to the live RPC "
          "if they go stale >24h)")
    return 1


if __name__ == '__main__':
    sys.exit(main())
