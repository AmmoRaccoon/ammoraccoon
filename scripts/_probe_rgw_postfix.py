"""Probe: verify the RGW stock-fix run in the DB (read-only).

Expectation: listings updated by the fix run are no longer blanket-OOS —
a healthy mix (or majority) in_stock=true, per the live-site dry-run.
"""
import os
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])

rid = sb.table('retailers').select('id,last_scraped_at').eq('slug', 'recoilgunworks').execute().data[0]
cutoff = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
rows = (sb.table('listings')
        .select('caliber_normalized,in_stock,last_updated')
        .eq('retailer_id', rid['id'])
        .execute().data)
fresh = [r for r in rows if (r['last_updated'] or '') >= cutoff]
stale = [r for r in rows if (r['last_updated'] or '') < cutoff]
print(f"retailers.last_scraped_at: {rid['last_scraped_at']}")
print(f'total RGW listings: {len(rows)} | updated by this run: {len(fresh)} | stale: {len(stale)}')
by_cal = Counter()
in_by_cal = Counter()
for r in fresh:
    by_cal[r['caliber_normalized']] += 1
    if r['in_stock']:
        in_by_cal[r['caliber_normalized']] += 1
print(f'{"caliber":<10}{"fresh":>6}{"in_stock":>9}')
for cal in sorted(by_cal):
    print(f'{cal:<10}{by_cal[cal]:>6}{in_by_cal[cal]:>9}')
tot_in = sum(in_by_cal.values())
print(f'\nfresh in-stock total: {tot_in}/{len(fresh)}')
print('VERDICT:', 'FIXED - stock state recovered' if tot_in > 0 else 'STILL ALL OOS - investigate')
if stale:
    print(f'note: {len(stale)} rows not touched this run (skipped/no-round-count SKUs) - their stock state is from prior runs')
