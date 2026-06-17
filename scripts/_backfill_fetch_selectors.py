"""One-shot backfill of fetch{} + selectors{} into caliber_paths/<retailer>.json
(expansion #4, Step-3 validation harness, step 5).

Reads each of the 29 migrated scrapers ONCE (done by hand, transcribed below —
NOT inferred), records into its config:

  fetch{}      mode (requests | playwright-sync | playwright-async) + the
               wall flags actually present (stealth / fresh_context_per_request).
  selectors{}  the product_card (the category-grid query_selector_all literal)
               + product_title (the query_selector literal whose .inner_text is
               the product NAME fed to the caliber gate) — DOM/Playwright
               retailers only. The 5 requests retailers get NO selectors
               (Shopify cards come structurally from /products.json; buds parses
               schema.org microdata with regex — neither uses a DOM selector).

Every value is transcribed from what the scraper REALLY does (file+line cited
in the report), never guessed. Multi-leg title cases (a Python `or` fallback
chain) record the PRIMARY leg only — collapsing them into a CSS union would, in
at least one case (rivertown), reproduce a bug the scraper deliberately avoids.

user_agent is OMITTED for all 29 (conservative): every migrated scraper uses a
generic desktop-Chrome UA functionally equivalent to the harness DEFAULT_UA;
the version/mechanism differences are incidental, not load-bearing. See the
STOP-flag in the session report — flip ADD_USER_AGENT below if Jon wants the
literal "non-default" reading instead.

Writes via caliber_paths_io.dump_config so each diff is ONLY the new
fetch/selectors block (canonical key order, no reformatting of existing lines).
Self-validates: every config must still pass scraper_lib's hand-rolled loader
validation after the write.

    py scripts/_backfill_fetch_selectors.py            # check (dry) — prints diffs-to-be
    py scripts/_backfill_fetch_selectors.py --write    # write in place
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import caliber_paths_io  # noqa: E402
import scraper_lib  # noqa: E402

# Set True only if Jon opts into recording each scraper's verbatim UA. Left
# False per the conservative default (see module docstring + session report).
ADD_USER_AGENT = False

# ---------------------------------------------------------------------------
# THE TRANSCRIBED TABLE. Per retailer:
#   'fetch'     -> dict written verbatim into config['fetch'] (mode required).
#   'selectors' -> dict written verbatim into config['selectors'] (omit key
#                  entirely for requests retailers; omit 'product_title' for a
#                  retailer with no honest title selector — natchez).
# Selectors are the EXACT literal strings the scraper passes to
# query_selector_all (card) / query_selector (title). Source line in comments.
# ---------------------------------------------------------------------------
MAP = {
    # ----- requests (no DOM selectors) -----
    'buds':              {'fetch': {'mode': 'requests'}},               # requests; schema.org microdata via regex; platform generic-sitemap
    'blackbasin':        {'fetch': {'mode': 'requests'}},               # urllib; Shopify /products.json
    'fenix':             {'fetch': {'mode': 'requests'}},               # urllib; Shopify /products.json
    'freedommunitions':  {'fetch': {'mode': 'requests'}},               # urllib; Shopify /products.json
    'trueshot':          {'fetch': {'mode': 'requests'}},               # urllib; Shopify /products.json

    # ----- playwright-async -----
    'aeammo':   {'fetch': {'mode': 'playwright-async'},
                 'selectors': {'product_card': 'li.product',                       # :132
                               'product_title': 'h4.card-title a'}},               # :145
    'ammocom':  {'fetch': {'mode': 'playwright-async'},
                 'selectors': {'product_card': 'li.b-product-list-item',           # :124
                               'product_title': 'h2.b-product-list-item__product-name'}},  # :137
    'bulkammo': {'fetch': {'mode': 'playwright-async'},
                 'selectors': {'product_card': '#catalog-listing li.item',         # :129
                               'product_title': 'a.product-name'}},                # :144
    'natchez':  {'fetch': {'mode': 'playwright-async'},
                 'selectors': {'product_card': '.product__tile'}},                 # :134 (NO title selector — heuristic line-parse of tile.inner_text :146-160)
    'rivertown':{'fetch': {'mode': 'playwright-async'},
                 'selectors': {'product_card': 'li.product',                       # :125
                               'product_title': '.woocommerce-loop-product__title'}},  # :153 (PRIMARY leg; sequential fallback — union would pick wrong el)

    # ----- playwright-sync -----
    'ammodeport':         {'fetch': {'mode': 'playwright-sync'},
                           'selectors': {'product_card': '.product-item',          # :103
                                         'product_title': 'a.product-item-link'}},  # :113
    'buckinghorse':       {'fetch': {'mode': 'playwright-sync'},
                           'selectors': {'product_card': '.productGrid article.card',           # :113
                                         'product_title': 'h4.card-title a, .card-title a, h3 a'}},  # :126
    'classicfirearms':    {'fetch': {'mode': 'playwright-sync'},
                           'selectors': {'product_card': 'div.product-card.item',   # :185
                                         'product_title': 'h2.product-name a, .product-name a'}},  # :197
    'dancessportinggoods':{'fetch': {'mode': 'playwright-sync'},
                           'selectors': {'product_card': 'article.card',            # :141
                                         'product_title': 'h4.card-title a'}},       # :155
    'firearmsdepot':      {'fetch': {'mode': 'playwright-sync'},
                           'selectors': {'product_card': '.productGrid article.card',  # :190
                                         'product_title': 'h4.card-title a'}},          # :200
    'georgiaarms':        {'fetch': {'mode': 'playwright-sync'},
                           'selectors': {'product_card': '.productGrid li.product',     # :157
                                         'product_title': 'h4.card-title a, .card-title a, h3 a'}},  # :172
    'gorilla':            {'fetch': {'mode': 'playwright-sync'},
                           'selectors': {'product_card': 'li.product, ul.products li',   # :96
                                         'product_title': '.woocommerce-loop-product__title, h2, h3'}},  # :114
    'gritr':              {'fetch': {'mode': 'playwright-sync'},
                           'selectors': {'product_card': 'li.snize-product',         # :123
                                         'product_title': '.snize-title'}},          # :158 (inner_text FALLBACK; scraper PREFERS aria-label/title attr of a.snize-view-link)
    'gunbuyer':           {'fetch': {'mode': 'playwright-sync',
                                     'fresh_context_per_request': True},             # :138 fresh new_context per page (Cloudflare)
                           'selectors': {'product_card': '.products.list.items.product-items > li',  # :183
                                         'product_title': '.product-item-link'}},    # :191
    'luckygunner':        {'fetch': {'mode': 'playwright-sync'},
                           'selectors': {'product_card': 'li.item',                  # :129
                                         'product_title': 'h2 a, h3 a, .product-name a, a.product-name'}},  # :144
    'outdoorlimited':     {'fetch': {'mode': 'playwright-sync'},
                           'selectors': {'product_card': '.row_inner',              # :126
                                         'product_title': 'a.v-product__title'}},   # :137
    'recoilgunworks':     {'fetch': {'mode': 'playwright-sync'},
                           'selectors': {'product_card': '.productGrid li.product',  # :183
                                         'product_title': 'h3.card-title a'}},       # :202 (PRIMARY leg; sequential fallback to .card-figure__link)
    'sgammo':             {'fetch': {'mode': 'playwright-sync'},
                           'selectors': {'product_card': 'table.sgammo-product-list__table tr',  # :134
                                         'product_title': 'a'}},                     # :151 (first anchor in the row)
    'shadowsmith':        {'fetch': {'mode': 'playwright-sync'},
                           'selectors': {'product_card': 'ul.products li.product, li.product',  # :153
                                         'product_title': 'h2.woocommerce-loop-product__title, .woocommerce-loop-product__title'}},  # :184
    'sportsmansguide':    {'fetch': {'mode': 'playwright-sync',
                                     'stealth': True,                               # :401 Stealth().apply_stealth_sync
                                     'fresh_context_per_request': True},            # :386 fresh new_context per parent (bot wall)
                           'selectors': {'product_card': '.product-tile',           # :174
                                         'product_title': '.product-name span, .product-name'}},  # :202
    'targetsports':       {'fetch': {'mode': 'playwright-sync'},
                           'selectors': {'product_card': 'li a[href*="-p-"]',       # :134
                                         'product_title': 'h2'}},                    # :155
    'underwood':          {'fetch': {'mode': 'playwright-sync'},
                           'selectors': {'product_card': '.productGrid article.card',  # :137
                                         'product_title': 'h4.card-title a, h4.card-title'}},  # :173 (inner_text FALLBACK; scraper PREFERS aria-label attr of a.card-figure__link)
    'velocity':           {'fetch': {'mode': 'playwright-sync'},
                           'selectors': {'product_card': 'ul.products li.product, li.product',  # :113
                                         'product_title': '.woocommerce-loop-product__title, h2, h3'}},  # :144
    'ventura':            {'fetch': {'mode': 'playwright-sync'},
                           'selectors': {'product_card': '.productGrid article.card',  # :111
                                         'product_title': 'h4.card-title a, .card-title a, h3 a'}},  # :123
}

# Verbatim category-fetch UA per retailer (only used when ADD_USER_AGENT=True).
USER_AGENTS = {
    'buds':       'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'freedommunitions': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'trueshot':   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36',
    'ammocom':    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36',
    'bulkammo':   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36',
    'gunbuyer':   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    # header-override (set_extra_http_headers) retailers all use Chrome/120.0.0.0:
    'ammodeport': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'buckinghorse': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'classicfirearms': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'dancessportinggoods': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'firearmsdepot': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'georgiaarms': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'gorilla':    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'outdoorlimited': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'recoilgunworks': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'sgammo':     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'underwood':  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    # blackbasin/fenix use Chrome/124.0.0.0 (== harness DEFAULT_UA); aeammo,
    # natchez, rivertown, gritr, ventura, velocity, luckygunner, shadowsmith,
    # targetsports set NO UA (Playwright default). sportsmansguide sets
    # Chrome/124.0.0.0 (== default). None recorded even when ADD_USER_AGENT.
}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--write', action='store_true',
                    help='write configs in place (default: dry check)')
    args = ap.parse_args()

    cfg_dir = ROOT / 'caliber_paths'
    changed, unchanged, failed = [], 0, []

    for retailer in sorted(MAP):
        path = cfg_dir / f'{retailer}.json'
        if not path.exists():
            failed.append(f'{retailer}: config file missing')
            continue
        cfg = caliber_paths_io.load_config(str(path))
        before = caliber_paths_io.dump_config(cfg)

        spec = MAP[retailer]
        fetch = dict(spec['fetch'])
        if ADD_USER_AGENT and retailer in USER_AGENTS:
            fetch['user_agent'] = USER_AGENTS[retailer]
        cfg['fetch'] = fetch
        if 'selectors' in spec:
            cfg['selectors'] = dict(spec['selectors'])

        # Validate the merged config under the SAME hand-rolled loader rules
        # the scraper hits at runtime — fail loudly before writing anything.
        try:
            scraper_lib._validate_caliber_paths_cfg(cfg, retailer)
        except Exception as e:
            failed.append(f'{retailer}: validation FAILED after merge: {e}')
            continue

        after = caliber_paths_io.dump_config(cfg)
        if after == before:
            unchanged += 1
            continue
        changed.append(retailer)
        if args.write:
            with open(path, 'w', encoding='utf-8', newline='\n') as f:
                f.write(after)

    verb = 'wrote' if args.write else 'would write'
    print(f'\n{verb} {len(changed)} config(s); {unchanged} already current; '
          f'{len(failed)} failed.')
    for r in changed:
        print(f'  {verb}: {r}.json')
    if failed:
        print('\nFAILURES:')
        for f in failed:
            print(f'  {f}')
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
