"""Probe: confirm the TSUSA rewire actually landed in the DB (read-only).

Counts target-sports listings per caliber with last_updated inside the
one-off 2026-06-12 rewire run's window.
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

rid = sb.table('retailers').select('id,last_scraped_at').eq('slug', 'target-sports').execute().data[0]
print(f"retailers.last_scraped_at: {rid['last_scraped_at']}")
cutoff = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
rows, start = [], 0
while True:
    batch = (sb.table('listings')
             .select('caliber_normalized,in_stock')
             .eq('retailer_id', rid['id'])
             .gte('last_updated', cutoff)
             .range(start, start + 999)
             .execute().data)
    rows.extend(batch)
    if len(batch) < 1000:
        break
    start += 1000
fresh = Counter(r['caliber_normalized'] for r in rows)
in_stock = Counter(r['caliber_normalized'] for r in rows if r['in_stock'])
print(f'{len(rows)} listings updated in the last hour, by caliber:')
for cal in ('9mm', '380acp', '40sw', '38spl', '357mag', '22lr',
            '223-556', '308win', '762x39', '300blk'):
    print(f'  {cal:<10} {fresh.get(cal, 0):>4} fresh ({in_stock.get(cal, 0)} in stock)')
dark = [c for c in ('9mm', '380acp', '40sw', '38spl', '357mag', '22lr',
                    '223-556', '308win', '762x39', '300blk') if not fresh.get(c)]
print('\nVERDICT:', 'ALL 10 CALIBERS FRESH' if not dark else f'STILL DARK: {dark}')
