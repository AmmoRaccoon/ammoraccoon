"""Dry-run verify of the candidate TSUSA category URLs (read-only, no DB).

For each OLD path: capture the 301 Location so the before/after table shows
what caliber the old ID lands on today.
For each NEW path: load in Playwright exactly like scraper_targetsports does,
then check (a) no redirect, (b) page <title> names the caliber, (c) product
titles pass the scraper's strict normalize_caliber gate at a high rate.
"""
import re
import sys
import requests
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scraper_lib import normalize_caliber  # noqa: E402

from playwright.sync_api import sync_playwright  # noqa: E402

BASE = 'https://www.targetsportsusa.com'
UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

OLD_PATHS = {
    '9mm':     '/9mm-luger-ammo-c-51.aspx',
    '380acp':  '/380-acp-ammo-c-49.aspx',
    '40sw':    '/40-sw-ammo-c-52.aspx',
    '38spl':   '/38-special-ammo-c-48.aspx',
    '357mag':  '/357-magnum-ammo-c-47.aspx',
    '22lr':    '/22-lr-ammo-c-65.aspx',
    '223-556': '/223-rem-5-56-nato-ammo-c-66.aspx',
    '308win':  '/308-win-7-62x51-ammo-c-67.aspx',
    '762x39':  '/7-62x39-ammo-c-71.aspx',
    '300blk':  '/300-aac-blackout-ammo-c-69.aspx',
}

NEW_PATHS = {
    '9mm':     ['/9mm-luger-ammo-c-51.aspx'],
    '380acp':  ['/380-acp-auto-ammo-c-50.aspx'],
    '40sw':    ['/40-sw-ammo-c-59.aspx'],
    '38spl':   ['/38-special-ammo-c-56.aspx'],
    '357mag':  ['/357-magnum-ammo-c-57.aspx'],
    '22lr':    ['/22-long-rifle-ammo-c-202.aspx'],
    '223-556': ['/223-remington-ammo-c-83.aspx', '/556mm-nato-ammo-c-2719.aspx'],
    '308win':  ['/308-winchester-ammo-c-101.aspx', '/762x51mm-nato-ammo-c-2720.aspx'],
    '762x39':  ['/762x39mm-ammo-c-108.aspx'],
    '300blk':  ['/300-aac-blackout-ammo-c-969.aspx'],
}

QS = '?pp=240&SortOrder=PriceAscending'

print('=== OLD URLs: where do they land today? ===')
for cal, path in OLD_PATHS.items():
    try:
        r = requests.get(BASE + path, headers={'User-Agent': UA},
                         timeout=30, allow_redirects=False)
        loc = r.headers.get('Location', '(no redirect)')
        print(f'  {cal:<8} {path:<38} -> {r.status_code} {loc}')
    except Exception as e:
        print(f'  {cal:<8} {path:<38} -> ERROR {e}')

print('\n=== NEW URLs: Playwright dry-run with the strict gate ===')
failures = []
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.set_extra_http_headers({'User-Agent': UA})
    for cal, paths in NEW_PATHS.items():
        for path in paths:
            url = BASE + path + QS
            resp = page.goto(url, wait_until='domcontentloaded', timeout=90000)
            status = resp.status if resp else '??'
            try:
                page.wait_for_selector('li a[href*="-p-"]', timeout=30000)
            except Exception:
                pass
            page.wait_for_timeout(3000)
            final_url = page.url
            redirected = final_url.split('?')[0] != (BASE + path)
            title = page.title()
            products = page.query_selector_all('li a[href*="-p-"]')
            names, gate_pass = [], 0
            for prod in products:
                h2 = prod.query_selector('h2')
                if not h2:
                    continue
                name = (h2.inner_text() or '').strip()
                if not name:
                    continue
                names.append(name)
                _, detected = normalize_caliber(name)
                if detected == cal:
                    gate_pass += 1
            pct = (100.0 * gate_pass / len(names)) if names else 0.0
            ok = (status == 200 and not redirected and gate_pass > 0)
            if not ok:
                failures.append((cal, path))
            print(f'\n[{cal}] {path}')
            print(f'  status={status} redirected={redirected} final={final_url[:90]}')
            print(f'  title: {title[:100]}')
            print(f'  cards-with-name={len(names)}  gate-pass={gate_pass} ({pct:.0f}%)')
            for n in names[:3]:
                print(f'    sample: {n[:80]}')
    browser.close()

print('\n=== VERDICT ===')
if failures:
    print('FAILURES:', failures)
else:
    print('All new URLs serve the right caliber with no redirect.')
