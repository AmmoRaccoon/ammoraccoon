import time
from playwright.sync_api import sync_playwright

URL = "https://www.targetsportsusa.com/9mm-luger-ammo-c-51.aspx?pp=240&SortOrder=PriceAscending"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.set_extra_http_headers({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })

    page.goto(URL, wait_until='domcontentloaded', timeout=90000)
    time.sleep(20)

    html = page.content()
    
    idx = html.find('Wolf Performance')
    if idx > 0:
        print("Found products!")
        print(html[idx:idx+2000])
    else:
        print("Products not found")
        print(f"Page length: {len(html)}")
        # Print last part of body text to see what loaded
        print("\n--- BODY TEXT (last 2000 chars) ---")
        body = page.inner_text('body')
        print(body[-2000:])

    browser.close()