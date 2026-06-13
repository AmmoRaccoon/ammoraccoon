"""Retailer-wide park of PSA (Jon-approved 2026-06-12, Option A).

Ammunition Depot pattern: evict price_history then listings (the rebate
and ballistics match tables are ON DELETE CASCADE), then is_active=false.
Scraper file stays in the repo as dead code; the cron step is removed
from scrape_light.yml in the same session.

Why: Cloudflare-walled — zero saves on ANY caliber since 2026-05-20
(all 149 listings frozen at in_stock=true; 403 on all 10 category
pages). Evidence: scripts/_probe_psa_walled.py + _probe_psa_freshness.py.
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])

r = sb.table('retailers').select('id,name,is_active').eq('slug', 'psa').execute().data[0]
rid = r['id']
print(f"retailer: [{rid}] {r['name']} is_active={r['is_active']}")

listing_rows = sb.table('listings').select('id').eq('retailer_id', rid).execute().data
ids = [row['id'] for row in listing_rows]
print(f'listings to evict: {len(ids)}')

ph_total = 0
for i in range(0, len(ids), 100):
    batch = ids[i:i + 100]
    res = sb.table('price_history').select('id', count='exact').in_('listing_id', batch).limit(1).execute()
    ph_total += res.count or 0
print(f'price_history rows attached: {ph_total}')

if '--apply' not in sys.argv:
    print('\nDRY RUN ONLY - rerun with --apply to evict.')
    sys.exit(0)

deleted_ph = 0
for i in range(0, len(ids), 100):
    batch = ids[i:i + 100]
    res = sb.table('price_history').delete(count='exact').in_('listing_id', batch).execute()
    deleted_ph += res.count or 0
print(f'deleted price_history rows: {deleted_ph}')

res = sb.table('listings').delete(count='exact').eq('retailer_id', rid).execute()
print(f'deleted listings: {res.count}')

sb.table('retailers').update({'is_active': False}).eq('id', rid).execute()

# Post-conditions.
left = sb.table('listings').select('id', count='exact').eq('retailer_id', rid).limit(1).execute()
r2 = sb.table('retailers').select('is_active').eq('id', rid).execute().data[0]
print(f'\nVERIFY: listings remaining={left.count}  is_active={r2["is_active"]}')
print('VERDICT:', 'PARKED CLEAN' if (left.count == 0 and r2['is_active'] is False) else 'CHECK MANUALLY')
