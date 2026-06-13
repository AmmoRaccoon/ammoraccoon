"""Probe: per-caliber last_updated freshness for PSA listings (read-only).

Decides whether the GHA vantage still gets 200s on the 5 covered calibers:
if their max(last_updated) tracks the 2h light-tier cadence, the wall split
(5 covered = 200, 5 missing = 503) is alive and the park is per-caliber,
not retailer-wide.
"""
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])

rid = sb.table('retailers').select('id,last_scraped_at').eq('slug', 'psa').execute().data[0]
print(f"retailers.last_scraped_at: {rid['last_scraped_at']}")
rows = (sb.table('listings')
        .select('caliber_normalized,last_updated,in_stock')
        .eq('retailer_id', rid['id'])
        .execute().data)
now = datetime.now(timezone.utc)
agg = defaultdict(lambda: {'n': 0, 'newest': None, 'in_stock': 0})
for r in rows:
    cal = r['caliber_normalized']
    a = agg[cal]
    a['n'] += 1
    a['in_stock'] += 1 if r['in_stock'] else 0
    ts = r['last_updated']
    if ts and (a['newest'] is None or ts > a['newest']):
        a['newest'] = ts
print(f'{"caliber":<10}{"rows":>5}{"in_stock":>9}  newest last_updated (age)')
for cal, a in sorted(agg.items()):
    age = ''
    if a['newest']:
        dt = datetime.fromisoformat(a['newest'].replace('Z', '+00:00'))
        age = f"({(now - dt).total_seconds() / 3600:.1f}h ago)"
    print(f'{cal:<10}{a["n"]:>5}{a["in_stock"]:>9}  {a["newest"]} {age}')
