"""Probe: RecoilGunWorks 0-in-stock anomaly (read-only, report-only).

2026-06-11 audit: all RGW listings read out-of-stock. The scraper decides
stock from product-page BODY TEXT ('out of stock' / 'sold out' /
'currently unavailable' substrings). Hypothesis to test: a site-wide
element (related-products rail, badge, etc.) injects one of those strings
into every page, flipping the heuristic to false-OOS globally.

Checks: DB stock breakdown, then 5 sample product pages — substring hits
vs the actual add-to-cart control state, plus where in the DOM the
matching text lives. Screenshots to ammoraccoon-web/tmp-screenshots/.
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

SHOT_DIR = Path(__file__).resolve().parents[2] / 'ammoraccoon-web' / 'tmp-screenshots'
SHOT_DIR.mkdir(exist_ok=True)

rid = sb.table('retailers').select('id,last_scraped_at').eq('slug', 'recoilgunworks').execute().data[0]
rows = (sb.table('listings')
        .select('product_url,in_stock,caliber_normalized,last_updated')
        .eq('retailer_id', rid['id'])
        .execute().data)
stock = Counter(r['in_stock'] for r in rows)
newest = max((r['last_updated'] or '') for r in rows) if rows else None
print(f"RGW: {len(rows)} listings | in_stock breakdown: {dict(stock)}")
print(f"newest last_updated: {newest}")
print(f"retailers.last_scraped_at: {rid['last_scraped_at']}")

samples = [r['product_url'] for r in rows[:50]][:5]
NEEDLES = ('out of stock', 'sold out', 'currently unavailable')

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.set_extra_http_headers({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })
    for i, url in enumerate(samples, 1):
        print(f'\n--- sample {i}: {url}')
        try:
            resp = page.goto(url, wait_until='domcontentloaded', timeout=60000)
            print(f'  HTTP {resp.status if resp else "??"}')
            page.wait_for_timeout(4000)
            body = (page.locator('body').inner_text() or '').lower()
            hits = [n for n in NEEDLES if n in body]
            print(f'  scraper-heuristic hits in body text: {hits or "none"}')
            # Where does the matching text live?
            for needle in hits:
                els = page.query_selector_all(f'text="{needle}"')
                for el in els[:3]:
                    cls = el.get_attribute('class') or ''
                    parent = el.evaluate(
                        'e => e.closest("section,div[class],form")?.className || "?"')
                    print(f'    "{needle}" in element class="{cls[:60]}" '
                          f'closest-block="{str(parent)[:70]}"')
            # The actual purchase control.
            btn = page.query_selector('#form-action-addToCart, input[id*="addToCart" i], '
                                      'button[id*="addToCart" i]')
            if btn:
                val = btn.get_attribute('value') or (btn.inner_text() or '')
                disabled = btn.get_attribute('disabled')
                print(f'  add-to-cart control: value/text="{val.strip()}" '
                      f'disabled={disabled!r}')
            else:
                print('  add-to-cart control: NOT FOUND')
            shot = SHOT_DIR / f'rgw-stock-sample-{i}.png'
            page.screenshot(path=str(shot), full_page=False)
            print(f'  screenshot: {shot.name}')
        except Exception as e:
            print(f'  ERROR {type(e).__name__}: {str(e)[:100]}')
    browser.close()
