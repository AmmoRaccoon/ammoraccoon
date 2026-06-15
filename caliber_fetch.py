"""Fetch-adapter library for the validation harness (#4 Step-3).

Produces the normalized `caliber_validate.Page` the step-2 evaluator consumes,
fetching each retailer in its REAL environment, dispatched on the config
`fetch` block (the discovery-vs-fetch axis from step 1). `Page` is IMPORTED
from caliber_validate so the field contract (status, requested_url,
landed_url, title, card_titles) can never drift — the adapter never reinvents
field names.

    fetch_page(config, path_query, selectors=None) -> Page

`config` is a loaded caliber_paths config dict (needs `base`, `platform`,
`fetch`; optional `selectors`); `path_query` is an entry['url'] (relative
'<path>?<query>').

MODES (config['fetch']['mode'])
  requests          urllib, no browser. For our 4 requests retailers this is
                    Shopify-JSON: cards come STRUCTURALLY from
                    /collections/<handle>/products.json (exactly what the
                    scraper fetches — gates 1/2/4/5), and the HTML collection
                    page is fetched once for the <title> (gate 3; the JSON
                    feed has no <title>).
  playwright-sync   sync Chromium.
  playwright-async  async Chromium (run via asyncio.run so the caller stays sync).

FLAGS
  stealth                     -> playwright_stealth + anti-automation launch
                                 arg (sportsmansguide).
  fresh_context_per_request   -> a fresh browser context per fetch
                                 (gunbuyer/wideners, Cloudflare). A single-URL
                                 fetch is ALWAYS a fresh context here, so the
                                 flag is naturally satisfied; it also adds the
                                 anti-automation arg. Replicating the wall this
                                 way is the whole point of fetch.mode being
                                 explicit (the Wideners trap: a naive probe
                                 200s, the real run is walled).

DOM card extraction uses the config `selectors` block. NONE are backfilled yet
(step 5), so Playwright fetches currently return card_titles=[] and the
evaluator deep-grades nothing (cheap gates 1-3 only) — by design. NO
per-retailer selectors are hardcoded here.

FIDELITY FLAGS (places the generic adapter is close-but-not-identical to a
specific scraper — called out, not guessed):
  - Per-retailer settle time is not in fetch{}; the adapter uses a generic
    domcontentloaded + SETTLE_MS. Gates 1-3 are ready at domcontentloaded;
    card-render settle gets tuned (or a settle_ms field added) when selectors
    land in step 5.
  - The deepest per-retailer context details (sportsmansguide's exact
    viewport/locale/headers; gunbuyer's exact context) are NOT fully
    generalized. The adapter replicates the wall-defeating factors named in the
    brief — UA + stealth + fresh-context + anti-automation arg. Walled
    retailers validate authoritatively only on GHA, where this is refined if a
    first run shows the wall still bites.
"""
import asyncio
import json
import re
import urllib.error
import urllib.request

from caliber_validate import Page

DEFAULT_UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
              '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36')
HTTP_TIMEOUT = 30        # seconds (urllib)
GOTO_TIMEOUT = 60000     # ms (Playwright goto)
SETTLE_MS = 4000         # ms settle after domcontentloaded (representative of
                         # the scrapers' 2-6s range; see fidelity flag above)


def _ua(config):
    return (config.get('fetch') or {}).get('user_agent') or DEFAULT_UA


def _abs(base, path_query):
    if path_query.startswith('http'):
        return path_query
    return base.rstrip('/') + '/' + path_query.lstrip('/')


def _extract_title(html):
    m = re.search(r'<title[^>]*>(.*?)</title>', html, re.IGNORECASE | re.DOTALL)
    return re.sub(r'\s+', ' ', m.group(1)).strip() if m else ''


def _selectors(config, override):
    sel = override or config.get('selectors') or {}
    if sel.get('product_card') and sel.get('product_title'):
        return sel
    return None


# --------------------------------------------------------------------------
# requests / Shopify-JSON
# --------------------------------------------------------------------------
def _http_get(url, accept, ua):
    """GET -> (status, landed_url, body_bytes). status is None on a network
    failure; an HTTP error code (e.g. 404) is returned as the status."""
    req = urllib.request.Request(url, headers={'User-Agent': ua, 'Accept': accept})
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            return resp.status, resp.geturl(), resp.read()
    except urllib.error.HTTPError as e:
        try:
            body = e.read()
        except Exception:
            body = b''
        return e.code, (e.url or url), body
    except Exception:
        return None, url, b''


def _fetch_requests(config, path_query):
    base = config['base']
    ua = _ua(config)

    if config.get('platform') == 'shopify':
        # Cards + status/redirect come from the JSON feed the scraper fetches.
        json_url = _abs(base, path_query) + '/products.json'
        jstatus, jlanded, jbody = _http_get(json_url, 'application/json', ua)
        card_titles = []
        try:
            data = json.loads(jbody) if jbody else {}
            card_titles = [(p.get('title') or '') for p in (data.get('products') or [])]
        except Exception:
            pass
        # Title comes from the HTML collection page (the JSON has no <title>).
        _, _, hbody = _http_get(_abs(base, path_query), 'text/html', ua)
        title = _extract_title(hbody.decode('utf-8', 'replace')) if hbody else ''
        return Page(status=jstatus, requested_url=json_url, landed_url=jlanded,
                    title=title, card_titles=card_titles)

    # Generic (non-Shopify) requests retailer — none exist today. Fetch the
    # HTML for gates 1-3; cards need an HTML parser we don't add here.
    url = _abs(base, path_query)
    status, landed, body = _http_get(url, 'text/html', ua)
    title = _extract_title(body.decode('utf-8', 'replace')) if body else ''
    return Page(status=status, requested_url=url, landed_url=landed,
                title=title, card_titles=[])


# --------------------------------------------------------------------------
# Playwright (sync + async)
# --------------------------------------------------------------------------
def _launch_args(stealth_on):
    return ['--disable-blink-features=AutomationControlled'] if stealth_on else []


def _fetch_playwright_sync(config, path_query, selectors):
    from playwright.sync_api import sync_playwright

    url = _abs(config['base'], path_query)
    fetch = config.get('fetch') or {}
    stealth_on = bool(fetch.get('stealth'))
    sel = _selectors(config, selectors)

    status, landed, title, card_titles = None, url, '', []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=_launch_args(stealth_on))
        context = browser.new_context(user_agent=_ua(config))
        if stealth_on:
            from playwright_stealth import Stealth
            Stealth().apply_stealth_sync(context)
        page = context.new_page()
        try:
            resp = page.goto(url, wait_until='domcontentloaded', timeout=GOTO_TIMEOUT)
            status = resp.status if resp else None
            page.wait_for_timeout(SETTLE_MS)
            landed = page.url
            title = page.title()
            if sel:
                for c in page.query_selector_all(sel['product_card']):
                    el = c.query_selector(sel['product_title'])
                    if el:
                        t = (el.inner_text() or '').strip()
                        if t:
                            card_titles.append(t)
        except Exception:
            pass
        finally:
            context.close()
            browser.close()
    return Page(status=status, requested_url=url, landed_url=landed,
                title=title, card_titles=card_titles)


async def _fetch_playwright_async(config, path_query, selectors):
    from playwright.async_api import async_playwright

    url = _abs(config['base'], path_query)
    fetch = config.get('fetch') or {}
    stealth_on = bool(fetch.get('stealth'))
    sel = _selectors(config, selectors)

    status, landed, title, card_titles = None, url, '', []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=_launch_args(stealth_on))
        context = await browser.new_context(user_agent=_ua(config))
        if stealth_on:
            from playwright_stealth import Stealth
            await Stealth().apply_stealth_async(context)
        page = await context.new_page()
        try:
            resp = await page.goto(url, wait_until='domcontentloaded', timeout=GOTO_TIMEOUT)
            status = resp.status if resp else None
            await page.wait_for_timeout(SETTLE_MS)
            landed = page.url
            title = await page.title()
            if sel:
                for c in await page.query_selector_all(sel['product_card']):
                    el = await c.query_selector(sel['product_title'])
                    if el:
                        t = (await el.inner_text() or '').strip()
                        if t:
                            card_titles.append(t)
        except Exception:
            pass
        finally:
            await context.close()
            await browser.close()
    return Page(status=status, requested_url=url, landed_url=landed,
                title=title, card_titles=card_titles)


# --------------------------------------------------------------------------
# Dispatch
# --------------------------------------------------------------------------
def fetch_page(config, path_query, selectors=None):
    """Fetch one category URL in the retailer's real environment and return a
    normalized Page. Sync API for the caller; async-mode is run internally."""
    mode = (config.get('fetch') or {}).get('mode')
    if mode == 'requests':
        return _fetch_requests(config, path_query)
    if mode == 'playwright-sync':
        return _fetch_playwright_sync(config, path_query, selectors)
    if mode == 'playwright-async':
        return asyncio.run(_fetch_playwright_async(config, path_query, selectors))
    raise ValueError(f"unknown/absent fetch.mode {mode!r} "
                     f"(config needs a fetch block — backfilled in step 5)")
