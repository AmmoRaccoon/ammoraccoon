import time
from playwright.sync_api import sync_playwright

URL = "https://www.ammunitiondepot.com/ammo/9mm/?sort=price-asc&limit=96"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.set_extra_http_headers({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    page.goto(URL, wait_until='networkidle', timeout=60000)
    time.sleep(5)

    products = page.query_selector_all('.product-item')
    print(f"Found {len(products)} products")

    if products:
        print("\n--- FIRST PRODUCT HTML ---")
        print(products[0].inner_html()[:3000])
        print("\n--- SECOND PRODUCT HTML ---")
        if len(products) > 1:
            print(products[1].inner_html()[:3000])

    browser.close()