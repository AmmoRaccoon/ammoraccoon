import time
from playwright.sync_api import sync_playwright

URL = "https://www.luckygunner.com/handgun/9mm-ammo"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.set_extra_http_headers({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })

    page.goto(URL, wait_until='domcontentloaded', timeout=90000)
    time.sleep(8)

    print(f"Title: {page.title()}")

    html = page.content()
    idx = html.find('per round')
    if idx < 0:
        idx = html.find('Per Round')
    if idx > 0:
        print("Found price data")
        print(html[max(0, idx-500):idx+500])
    else:
        print("No price data found")

    print("\n--- BODY SAMPLE ---")
    print(page.inner_text('body')[:2000])

    browser.close()