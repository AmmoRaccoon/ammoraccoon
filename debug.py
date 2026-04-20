import time
from playwright.sync_api import sync_playwright

URL = "https://palmettostatearmory.com/9mm-ammo.html?product_list_limit=100"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.set_extra_http_headers({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })

    page.goto(URL, wait_until='domcontentloaded', timeout=90000)
    time.sleep(8)

    products = page.query_selector_all('.product-item')
    print(f"Found {len(products)} products")

    # Find first in-stock product
    for i, product in enumerate(products[:10]):
        text = product.inner_text()
        if 'Out of Stock' not in text:
            print(f"\n--- IN STOCK PRODUCT {i} TEXT ---")
            print(text)
            print(f"\n--- IN STOCK PRODUCT {i} HTML ---")
            print(product.inner_html()[:2000])
            break

    browser.close()