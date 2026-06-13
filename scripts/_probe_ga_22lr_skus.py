"""Probe: prove WHY Georgia Arms' 2 remaining .22 LR SKUs save zero
(read-only). Replays the scraper's exact two-step round-count recovery
(variant labels -> title parser) against the live product pages.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from playwright.sync_api import sync_playwright  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import scraper_georgiaarms as ga  # noqa: E402  (module import is safe: client built lazily? no - builds client, but read-only)

UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.set_extra_http_headers({'User-Agent': UA})
    page.goto('https://www.georgia-arms.com/22-long-rifle/',
              wait_until='domcontentloaded', timeout=60000)
    page.wait_for_timeout(5000)
    cards = page.query_selector_all('.productGrid li.product')
    items = []
    for c in cards:
        a = c.query_selector('h3.card-title a') or c.query_selector('a[href]')
        if a:
            items.append(((a.inner_text() or '').strip(),
                          a.get_attribute('href')))
    print(f'{len(items)} cards on the 22lr category page')
    for title, url in items:
        print(f'\nSKU: {title}')
        print(f'  url: {url}')
        n_variant = ga.fetch_smallest_variant_rounds(page, url)
        labels = page.query_selector_all('label.form-label[data-product-attribute-value]')
        print(f'  variant labels on product page: {len(labels)}')
        for l in labels[:5]:
            print(f'    label text: {(l.inner_text() or "").strip()[:60]!r}')
        print(f'  fetch_smallest_variant_rounds -> {n_variant}')
        print(f'  parse_rounds(title) -> {ga.parse_rounds(title)}')
    browser.close()
