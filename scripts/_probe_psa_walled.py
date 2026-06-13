"""Probe: which PSA caliber category pages are Cloudflare-walled? (read-only)

2026-06-11 audit: PSA covers only 5 of 10 calibers in the DB, and exactly
the 5 missing calibers' category pages return Cloudflare 503. Re-prove
before parking: hit all 10 CALIBER_PATHS with the same Playwright shape
the scraper uses, and cross-check per-caliber listing counts in the DB.
"""
import os
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from supabase import create_client

load_dotenv()
sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])

SITE_BASE = 'https://palmettostatearmory.com'
CALIBER_PATHS = {
    '9mm':     '/9mm-ammo.html?product_list_limit=100',
    '380acp':  '/380-auto-ammo.html?product_list_limit=100',
    '40sw':    '/40-s-w-ammo.html?product_list_limit=100',
    '38spl':   '/38-special-ammo.html?product_list_limit=100',
    '357mag':  '/357-magnum-ammo.html?product_list_limit=100',
    '22lr':    '/22lr-ammo.html?product_list_limit=100',
    '223-556': '/223-5-56-ammo.html?product_list_limit=100',
    '308win':  '/308-7-62x51-ammo.html?product_list_limit=100',
    '762x39':  '/7-62x39-ammo.html?product_list_limit=100',
    '300blk':  '/300-blackout-ammo.html?product_list_limit=100',
}

rid = sb.table('retailers').select('id').eq('slug', 'psa').execute().data[0]['id']
rows = sb.table('listings').select('caliber_normalized').eq('retailer_id', rid).execute().data
db_counts = Counter(r['caliber_normalized'] for r in rows)
print('=== DB listings for PSA by caliber ===')
for cal in CALIBER_PATHS:
    print(f'  {cal:<8} {db_counts.get(cal, 0)}')

print('\n=== Live category page statuses (Playwright, scraper-shaped) ===')
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.set_extra_http_headers({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })
    for cal, path in CALIBER_PATHS.items():
        try:
            resp = page.goto(SITE_BASE + path, wait_until='domcontentloaded',
                             timeout=60000)
            status = resp.status if resp else '??'
            title = page.title()[:60]
            n = len(page.query_selector_all('.product-item'))
            print(f'  {cal:<8} HTTP {status}  cards={n:<4} title={title}')
        except Exception as e:
            print(f'  {cal:<8} ERROR {type(e).__name__}: {str(e)[:80]}')
    browser.close()
