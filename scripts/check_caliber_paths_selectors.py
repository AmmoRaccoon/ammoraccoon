"""caliber_paths fetch/selectors <-> scraper parity check (#4 Step-3, step 5).

Read-only drift guard, same discipline as scripts/check_caliber_registry.py:
asserts that what each caliber_paths/<retailer>.json RECORDS about how it is
fetched and parsed still matches what scraper_<retailer>.py ACTUALLY does, so
the config and the scraper can't silently drift apart.

For every config that carries a `fetch` block it checks:

  fetch.mode parity   requests        -> scraper imports urllib/requests and
                                         imports NO playwright api
                      playwright-sync -> scraper imports playwright.sync_api
                      playwright-async-> scraper imports playwright.async_api
  fetch.stealth       True            -> scraper imports playwright_stealth
  selectors.product_card   -> appears VERBATIM as a string-literal arg to a
                              .query_selector_all(...) call in the scraper
  selectors.product_title  -> appears VERBATIM as a string-literal arg to a
                              .query_selector(...) call in the scraper

WHAT THIS GUARD IS (honest scope). The selectors are inlined as bare string
literals inside each scraper's scrape_caliber loop (there is no named constant
to compare against), so a *fully role-aware* automatic match — "this literal is
THE card selector, not the price one" — is not possible without re-encoding the
per-scraper knowledge a human used to read it. This guard does the lightest
HONEST thing instead of a fake role-matcher: it role-BINDS by call type (a
product_card must be a query_selector_all literal; a product_title must be a
query_selector literal) and asserts the recorded literal is present among them.

  -> It CATCHES drift: rename `.productGrid article.card` in the scraper and the
     config's now-stale literal is no longer among the scraper's query_selector_all
     args, so this FAILS until the config is updated. That is the stated job.
  -> It does NOT re-verify role assignment: a literal that is present but was
     mis-transcribed to the wrong field would still pass. That correctness rests
     on the one-time manual transcription recorded in
     scripts/_backfill_fetch_selectors.py (file+line per selector), not on this
     guard. Stated plainly so the guard is never trusted for more than it proves.

`fetch.fresh_context_per_request` is a BEHAVIORAL flag (a fresh new_context per
fetch — a code pattern, not a literal), so it is reported but NOT auto-asserted;
re-deriving it would be a heuristic dressed up as a proof.

Exit 0 = all parity checks pass. Any FAIL prints the difference and exits 1.
"""
import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import caliber_paths_io  # noqa: E402

PASS = []
FAIL = []
WARN = []


def fail(name, msg):
    FAIL.append(name)
    print(f'FAIL  {name}')
    print(f'        {msg}')


def ok(name):
    PASS.append(name)
    print(f'OK    {name}')


def scan_scraper(path):
    """Return (qsa_literals, qs_literals, imported_modules) for a scraper.

    qsa = string-literal first-args to .query_selector_all(...)
    qs  = string-literal first-args to .query_selector(...)   (NOT _all)
    imported_modules = dotted module names from `import x` / `from x import ...`
    """
    tree = ast.parse(path.read_text(encoding='utf-8'))
    qsa, qs, imports = set(), set(), set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                imports.add(a.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.add(node.module)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            attr = node.func.attr
            if attr in ('query_selector_all', 'query_selector') and node.args:
                arg0 = node.args[0]
                if isinstance(arg0, ast.Constant) and isinstance(arg0.value, str):
                    (qsa if attr == 'query_selector_all' else qs).add(arg0.value)
    return qsa, qs, imports


_PLAYWRIGHT = ('playwright.sync_api', 'playwright.async_api')


def check_fetch_mode(retailer, mode, imports):
    name = f'{retailer}: fetch.mode={mode}'
    has_pw = any(m in imports for m in _PLAYWRIGHT)
    if mode == 'requests':
        if ('urllib.request' in imports or 'requests' in imports) and not has_pw:
            ok(name)
        else:
            fail(name, f'expected urllib/requests import and no playwright; '
                       f'imports={sorted(imports)}')
    elif mode == 'playwright-sync':
        ok(name) if 'playwright.sync_api' in imports else \
            fail(name, 'scraper does not import playwright.sync_api')
    elif mode == 'playwright-async':
        ok(name) if 'playwright.async_api' in imports else \
            fail(name, 'scraper does not import playwright.async_api')
    else:
        fail(name, f'unknown fetch.mode {mode!r}')


def main():
    cfg_dir = ROOT / 'caliber_paths'
    configs = sorted(p for p in cfg_dir.glob('*.json')
                     if p.name != 'caliber_paths.schema.json')

    for path in configs:
        cfg = caliber_paths_io.load_config(str(path))
        retailer = cfg.get('retailer') or path.stem
        fetch = cfg.get('fetch')
        if not fetch:
            WARN.append(f'{retailer}: no fetch block (not yet backfilled)')
            print(f'WARN  {retailer}: no fetch block — skipped')
            continue

        scraper_path = ROOT / f'scraper_{retailer}.py'
        if not scraper_path.exists():
            fail(f'{retailer}: scraper file', f'{scraper_path.name} not found')
            continue

        qsa, qs, imports = scan_scraper(scraper_path)

        # fetch.mode
        check_fetch_mode(retailer, fetch.get('mode'), imports)

        # fetch.stealth (only assert the True case)
        if fetch.get('stealth'):
            name = f'{retailer}: fetch.stealth'
            ok(name) if 'playwright_stealth' in imports else \
                fail(name, 'stealth:true but scraper does not import playwright_stealth')

        # selectors
        sel = cfg.get('selectors') or {}
        card = sel.get('product_card')
        if card is not None:
            name = f'{retailer}: selectors.product_card {card!r}'
            ok(name) if card in qsa else fail(
                name, f'not a query_selector_all literal in scraper_{retailer}.py; '
                      f'scraper uses: {sorted(qsa)}')
        title = sel.get('product_title')
        if title is not None:
            name = f'{retailer}: selectors.product_title {title!r}'
            ok(name) if title in qs else fail(
                name, f'not a query_selector literal in scraper_{retailer}.py; '
                      f'scraper uses: {sorted(qs)}')

    print(f'\n{len(PASS)} OK, {len(FAIL)} FAIL, {len(WARN)} WARN')
    if WARN:
        for w in WARN:
            print(f'  WARN: {w}')
    return 1 if FAIL else 0


if __name__ == '__main__':
    sys.exit(main())
