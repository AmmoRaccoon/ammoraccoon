"""Caliber-registry parity check, Python side (Phase A, read-only).

Compares every table in caliber_registry_gen.py against the hand-maintained
original it twins, value-level (dict/tuple equality — key order is not
load-bearing for any of these consumers):

  CALIBERS                     vs scraper_lib.CALIBERS
  CALIBER_PRICE_FLOORS         vs scraper_lib.CALIBER_PRICE_FLOORS (+ default)
  CALIBER_TO_FLOOR_KEY         vs scraper_lib._CALIBER_TO_FLOOR_KEY
  CALIBER_PRICE_CEILINGS       vs scraper_lib.CALIBER_PRICE_CEILINGS (+ default)
  AUDIT_EXPECTED_RANGES        vs scripts/caliber_audit.py EXPECTED_RANGES (+ default)
  REBATE_*_CALIBERS            vs scripts/match_manufacturer_rebates_to_listings.py
                                  (AST-extracted — importing that script builds a
                                  Supabase client at module level)
  BALLISTICS_CALIBER_NORMALIZE vs the UNION of the six ballistics scrapers'
                                  CALIBER_NORMALIZE maps, plus per-source
                                  consistency (no alias may map differently)
                                  and a per-source report of union-only
                                  aliases (the accepted D2 delta — those are
                                  measured live by _replay_ballistics_maps.py)

Also replays normalize_caliber vs normalize_caliber_gen over a built-in
edge-case corpus (the full live-URL corpus replay is a separate script).

Exit 0 = all parity checks pass. Any FAIL prints the difference and exits 1.
"""
import ast
import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import scraper_lib  # noqa: E402
import caliber_registry_gen as gen  # noqa: E402

PASS = []
FAIL = []


def check(name, actual, expected):
    if actual == expected:
        PASS.append(name)
        print(f'OK    {name}')
    else:
        FAIL.append(name)
        print(f'FAIL  {name}')
        if isinstance(actual, dict) and isinstance(expected, dict):
            for k in sorted(set(actual) | set(expected), key=str):
                if actual.get(k) != expected.get(k):
                    print(f'        {k!r}: hand={actual.get(k)!r} gen={expected.get(k)!r}')
        else:
            print(f'        hand={actual!r}')
            print(f'        gen ={expected!r}')


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def ast_assignments(path, names):
    """Extract module-level literal assignments without importing."""
    tree = ast.parse(Path(path).read_text(encoding='utf-8'))
    out = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 \
                and isinstance(node.targets[0], ast.Name) \
                and node.targets[0].id in names:
            out[node.targets[0].id] = ast.literal_eval(node.value)
    return out


# --- scraper_lib tables ------------------------------------------------------
check('CALIBERS', scraper_lib.CALIBERS, gen.CALIBERS)
check('CALIBER_PRICE_FLOORS', scraper_lib.CALIBER_PRICE_FLOORS, gen.CALIBER_PRICE_FLOORS)
check('DEFAULT_FLOOR', scraper_lib.DEFAULT_FLOOR, gen.DEFAULT_FLOOR)
check('CALIBER_TO_FLOOR_KEY', scraper_lib._CALIBER_TO_FLOOR_KEY, gen.CALIBER_TO_FLOOR_KEY)
check('CALIBER_PRICE_CEILINGS', scraper_lib.CALIBER_PRICE_CEILINGS, gen.CALIBER_PRICE_CEILINGS)
check('DEFAULT_CEILING', scraper_lib.DEFAULT_CEILING, gen.DEFAULT_CEILING)

# --- caliber_audit ranges (AST — module reads env at import) ----------------
audit = ast_assignments(ROOT / 'scripts' / 'caliber_audit.py',
                        {'EXPECTED_RANGES', 'DEFAULT_RANGE'})
check('AUDIT_EXPECTED_RANGES', audit['EXPECTED_RANGES'], gen.AUDIT_EXPECTED_RANGES)
check('AUDIT_DEFAULT_RANGE', audit['DEFAULT_RANGE'], gen.AUDIT_DEFAULT_RANGE)

# --- rebate matcher tuples (AST — module builds a client at import) ---------
rebate = ast_assignments(ROOT / 'scripts' / 'match_manufacturer_rebates_to_listings.py',
                         {'HANDGUN_CALIBERS', 'RIFLE_CALIBERS', 'RIMFIRE_CALIBERS'})
check('REBATE_HANDGUN_CALIBERS', rebate['HANDGUN_CALIBERS'], gen.REBATE_HANDGUN_CALIBERS)
check('REBATE_RIFLE_CALIBERS', rebate['RIFLE_CALIBERS'], gen.REBATE_RIFLE_CALIBERS)
check('REBATE_RIMFIRE_CALIBERS', rebate['RIMFIRE_CALIBERS'], gen.REBATE_RIMFIRE_CALIBERS)

# --- ballistics maps: union identity + per-source consistency ---------------
BALLISTICS = [
    ('hornady', ROOT / 'scraper_hornady_ballistics.py'),
    ('winchester', ROOT / 'scraper_winchester_ballistics.py'),
    ('kinetic', ROOT / 'scraper_kinetic_ballistics.py'),
    ('magtech', ROOT / 'scripts' / 'scraper_magtech_ballistics.py'),
    ('pmc', ROOT / 'scripts' / 'scraper_pmc_ballistics.py'),
    ('sb', ROOT / 'scripts' / 'scraper_sb_ballistics.py'),
]
union = {}
conflicts = []
per_source = {}
for name, path in BALLISTICS:
    m = ast_assignments(path, {'CALIBER_NORMALIZE'})['CALIBER_NORMALIZE']
    per_source[name] = m
    for alias, slug in m.items():
        if alias in union and union[alias] != slug:
            conflicts.append((alias, union[alias], slug))
        union[alias] = slug
check('BALLISTICS union-of-six == gen map', union, gen.BALLISTICS_CALIBER_NORMALIZE)
check('BALLISTICS no cross-source alias conflicts', conflicts, [])
print('\n  D2 delta preview (aliases the union ADDS per source — live impact')
print('  measured by scripts/_replay_ballistics_maps.py):')
for name, m in per_source.items():
    added = sorted(set(gen.BALLISTICS_CALIBER_NORMALIZE) - set(m))
    print(f'    {name:<11} +{len(added)} aliases not in its own map')

# --- normalize_caliber edge-case replay (built-in corpus) --------------------
corpus = set()
corpus.update(gen.CALIBERS.values())
corpus.update(gen.BALLISTICS_CALIBER_NORMALIZE.keys())
for specs in gen.NORMALIZE_SPECS.values():
    corpus.update(v for k, v in specs if k == 'sub')
corpus.update([
    '', 'Hornady Black 5.45x39mm 60gr', '5.45x39mm', 'x39mm',
    '42745562759273', 'Winchester USA 9 mm Luger', '7.62x51 M80 ball',
    '308 and 7.62x39 combo', '.223 Rem 55gr', '5.56 NATO M193',
    '38 Special +P 130gr', '22 Long Rifle high velocity',
])
diffs = [(t, scraper_lib.normalize_caliber(t), gen.normalize_caliber_gen(t))
         for t in corpus
         if scraper_lib.normalize_caliber(t) != gen.normalize_caliber_gen(t)]
if scraper_lib.normalize_caliber(None) != gen.normalize_caliber_gen(None):
    diffs.append((None, scraper_lib.normalize_caliber(None), gen.normalize_caliber_gen(None)))
check(f'normalize_caliber edge corpus ({len(corpus) + 1} inputs)', diffs, [])

# --- summary -----------------------------------------------------------------
print(f'\n{len(PASS)} parity checks passed, {len(FAIL)} failed.')
sys.exit(1 if FAIL else 0)
