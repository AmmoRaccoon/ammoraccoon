import time
from playwright.sync_api import sync_playwright

URL = "https://www.sgammo.com/catalog/pistol-ammo-sale/9mm-luger-ammo"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.set_extra_http_headers({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })

    page.goto(URL, wait_until='domcontentloaded', timeout=90000)
    time.sleep(10)

    html = page.content()

    idx = html.find('Per Round')
    if idx > 0:
        print("Found 'Per Round'")
        print(html[max(0, idx-1000):idx+500])
    else:
        print("'Per Round' not found")

    # Check table rows
    rows = page.query_selector_all('tr')
    print(f"\nFound {len(rows)} table rows")
    
    if rows:
        for i, row in enumerate(rows[:5]):
            print(f"\nRow {i}: {row.inner_text()[:200]}")

    browser.close()