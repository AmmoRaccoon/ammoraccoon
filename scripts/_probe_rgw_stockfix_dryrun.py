"""Dry-run of the RGW stock-read fix (read-only, no DB writes).

Replays old (body-wide) vs new (.productView-scoped) stock logic on the
same 5 product pages the diagnosis sampled. Expected: old=OOS on all 5
(the bug), new=IN STOCK on all 5 (each has an enabled Add to Cart), and
the .productView container must exist on every page (fallback unused).
"""
from playwright.sync_api import sync_playwright

UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
NEEDLES = ('out of stock', 'sold out', 'currently unavailable')
SAMPLES = [
    'https://www.recoilgunworks.com/speer-lawman-380-auto-95gr-fmj/',
    'https://www.recoilgunworks.com/defense-380-auto-56gr-nxd/',
    'https://www.recoilgunworks.com/speer-gold-dot-le-38-spl-p-125gr-gdhp-free-ship-2-box-min/',
    'https://www.recoilgunworks.com/v-crown-380-auto-90gr-jhp/',
    'https://www.recoilgunworks.com/speer-le-gold-dot-223-rem-62gr-gdsp/',
]


def verdict(text):
    return not any(n in text for n in NEEDLES)


with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.set_extra_http_headers({'User-Agent': UA})
    flips, problems = 0, []
    for url in SAMPLES:
        resp = page.goto(url, wait_until='domcontentloaded', timeout=60000)
        page.wait_for_timeout(4000)
        body_text = (page.locator('body').inner_text() or '').lower()
        pv = page.query_selector('.productView')
        pv_text = (pv.inner_text() or '').lower() if pv else None
        old = verdict(body_text)
        new = verdict(pv_text) if pv_text is not None else old
        btn = page.query_selector('#form-action-addToCart, input[id*="addToCart" i], '
                                  'button[id*="addToCart" i]')
        cart_live = bool(btn) and not btn.get_attribute('disabled')
        name = url.rstrip('/').split('/')[-1]
        print(f'{name[:55]:<57} pv={"yes" if pv else "MISSING"} '
              f'old={"IN" if old else "OOS"} new={"IN" if new else "OOS"} '
              f'cart={"live" if cart_live else "absent/disabled"}')
        if pv is None:
            problems.append((name, 'no .productView'))
        if new != cart_live:
            problems.append((name, f'new verdict {new} != cart state {cart_live}'))
        if new and not old:
            flips += 1
    print(f'\n{flips}/5 flipped OOS->IN with the scoped read.')
    print('VERDICT:', 'CLEAN - fix behaves as diagnosed' if not problems else f'PROBLEMS: {problems}')
    browser.close()
