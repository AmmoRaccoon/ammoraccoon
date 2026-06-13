"""READ-ONLY — caliber-registry Phase A corpus replay (Python side).

Loads the corpus exported by ammoraccoon-web/scripts/gen-calibers/
replay-detect.mjs (every live listing product_url + caliber display
string) and replays scraper_lib.normalize_caliber (hand-maintained
branches) against caliber_registry_gen.normalize_caliber_gen (registry-
driven loop). Zero verdict changes required.

The function's production inputs are scraped product titles, which the
DB doesn't store — product URLs embed the same naming and are the best
full-scale corpus available; the transcription itself is verbatim and
the built-in edge corpus in check_caliber_registry.py covers the
alias/display vocabulary.
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import scraper_lib  # noqa: E402
import caliber_registry_gen as gen  # noqa: E402

corpus_path = ROOT.parent / 'ammoraccoon-web' / 'scripts' / 'gen-calibers' / '_corpus.json'
corpus = json.loads(corpus_path.read_text(encoding='utf-8'))
print(f'corpus: {len(corpus)} inputs from {corpus_path.name}')

diffs = []
for text in corpus + [None, '']:
    old = scraper_lib.normalize_caliber(text)
    new = gen.normalize_caliber_gen(text)
    if old != new:
        diffs.append((text, old, new))

print(f'normalize_caliber: {len(diffs)} verdict changes')
for text, old, new in diffs[:5]:
    print(f'  DIFF: {str(text)[:90]!r} hand={old} gen={new}')
print('VERDICT: ZERO verdict changes (Python)' if not diffs else 'VERDICT: DIFFS FOUND (Python) — STOP')
sys.exit(0 if not diffs else 1)
