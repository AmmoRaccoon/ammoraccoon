#!/usr/bin/env python3
"""Parity guard for the component classifier.

The Python is_likely_component (scraper_lib.py) must flag EXACTLY the same
listings the JS isLikelyComponent (ammoraccoon-web/lib/listingHelpers.js) does.
This catches drift between the two language copies of the ruleset — if someone
edits one and forgets the other, this fails and names the divergent IDs.

Input: a catalog dump produced by the web repo's probe, JSON shape:
    {"rows": [{"id", "product_url", "total_rounds", "manufacturer"}, ...],
     "js_flagged": [<id>, ...]}

Generate it (from the ammoraccoon-web repo):
    node --import ./scripts/_audit_loader.mjs \\
         scripts/probe-is-component-backfill.mjs --dump /tmp/parity_catalog.json

Then run (from this ammoraccoon repo):
    py scripts/parity_is_component.py /tmp/parity_catalog.json

Exit 0 on parity, 1 on any divergence. Stdlib only (no supabase-py needed —
catalog access lives on the web/anon side that builds the dump). Pairs with
migrations 028/029 and the write-time classification in with_stock_fields.
"""
import json
import os
import sys

# Import the real classifier from scraper_lib (repo root is this file's parent's
# parent). scraper_lib is stdlib-only, so this import is cheap and dependency-free.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scraper_lib import is_likely_component  # noqa: E402

# Real loaded-ammo rows the 2026-05-26 classifier patches un-flagged. These must
# NEVER be classified as components again (regression sentinels).
SENTINELS_NOT_COMPONENT = {54106, 643666, 265471}  # Armscor / Hornady BLACK / HSM


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else 'parity_catalog.json'
    if not os.path.exists(path):
        print(f'dump not found: {path}\nGenerate it first (see this file\'s docstring).')
        return 2

    with open(path, encoding='utf-8') as fh:
        dump = json.load(fh)

    rows = dump['rows']
    js_flagged = set(dump['js_flagged'])
    py_flagged = {
        r['id'] for r in rows
        if is_likely_component(r.get('product_url'), r.get('total_rounds'), r.get('manufacturer'))
    }

    only_js = sorted(js_flagged - py_flagged)   # JS flags, Python misses
    only_py = sorted(py_flagged - js_flagged)   # Python flags, JS misses
    sentinel_hits = sorted(SENTINELS_NOT_COMPONENT & py_flagged)

    print(f'rows scanned                         : {len(rows)}')
    print(f'JS flagged                           : {len(js_flagged)}')
    print(f'Python flagged                       : {len(py_flagged)}')
    print(f'flagged by JS only (Python misses)   : {only_js}')
    print(f'flagged by Python only (JS misses)   : {only_py}')
    print(f'sentinels wrongly flagged (want none): {sentinel_hits}')

    ok = not only_js and not only_py and not sentinel_hits
    print('\nPARITY OK' if ok else '\nPARITY FAILED')
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
