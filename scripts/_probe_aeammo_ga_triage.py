"""Triage probe for the zero-coverage alert's first catches (read-only).

AE Ammo: 308win + 762x39 saved zero. Georgia Arms: 22lr + 762x39 saved
zero. For each flagged category URL: status / redirect / title / product
card count / sample titles + strict-gate result. If a page is 404/empty,
also crawl the parent category (AE) or homepage nav + category list (GA)
to discover whether a replacement slug exists — drift vs genuine absence.
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scraper_lib import normalize_caliber  # noqa: E402
from playwright.sync_api import sync_playwright  # noqa: E402

UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

CHECKS = [
    # (label, caliber_norm, url, card_selector)
    ('AE 308win', '308win', 'https://aeammo.com/Ammo/Rifle-Ammo/308-Win-Ammo', 'li.product'),
    ('AE 762x39', '762x39', 'https://aeammo.com/Ammo/Rifle-Ammo/7.62x39-Ammo', 'li.product'),
    ('GA 22lr',   '22lr',   'https://www.georgia-arms.com/22-long-rifle/', '.productGrid li.product'),
    ('GA 762x39', '762x39', 'https://www.georgia-arms.com/7-62x39-1/', '.productGrid li.product'),
]

DISCOVERY = [
    ('AE rifle subcategories', 'https://aeammo.com/Ammo/Rifle-Ammo',
     'a[href*="/Ammo/Rifle-Ammo"]'),
    ('GA all category links', 'https://www.georgia-arms.com/',
     'nav a[href], .navPages a[href]'),
]

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.set_extra_http_headers({'User-Agent': UA})

    print('=== flagged category pages ===')
    for label, cal, url, sel in CHECKS:
        print(f'\n[{label}] {url}')
        try:
            resp = page.goto(url, wait_until='domcontentloaded', timeout=60000)
            status = resp.status if resp else '??'
            page.wait_for_timeout(5000)
            final = page.url
            redirected = final.rstrip('/').split('?')[0].lower() != url.rstrip('/').lower()
            print(f'  status={status} redirected={redirected}')
            if redirected:
                print(f'  final={final}')
            print(f'  title: {page.title()[:90]}')
            cards = page.query_selector_all(sel)
            print(f'  cards({sel}): {len(cards)}')
            names, gate = [], 0
            for c in cards[:20]:
                t = (c.inner_text() or '').strip().replace('\n', ' | ')
                if not t:
                    continue
                names.append(t)
                _, det = normalize_caliber(t)
                if det == cal:
                    gate += 1
            print(f'  gate-pass on sampled cards: {gate}/{len(names)}')
            for n in names[:4]:
                print(f'    card: {n[:100]}')
        except Exception as e:
            print(f'  ERROR {type(e).__name__}: {str(e)[:120]}')

    print('\n=== discovery (for any empty/404 page above) ===')
    for label, url, sel in DISCOVERY:
        print(f'\n[{label}] {url}')
        try:
            resp = page.goto(url, wait_until='domcontentloaded', timeout=60000)
            print(f'  status={resp.status if resp else "??"}')
            page.wait_for_timeout(4000)
            hrefs = set()
            for a in page.query_selector_all(sel):
                h = a.get_attribute('href') or ''
                t = (a.inner_text() or '').strip()
                if h:
                    hrefs.add((h[:90], t[:50]))
            for h, t in sorted(hrefs):
                if re.search(r'(?i)308|7\.?-?62|22|rimfire|rifle', h + ' ' + t):
                    print(f'    {h}   [{t}]')
        except Exception as e:
            print(f'  ERROR {type(e).__name__}: {str(e)[:120]}')
    browser.close()
