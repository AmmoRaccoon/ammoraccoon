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
import hashlib
import importlib.util
import re
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


def ast_import_aliases(path, module):
    """Map {local_name: imported_name} for `from <module> import a as local`."""
    tree = ast.parse(Path(path).read_text(encoding='utf-8'))
    out = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == module:
            for alias in node.names:
                out[alias.asname or alias.name] = alias.name
    return out


# --- registry FRESHNESS (gen artifacts vs calibers.json, the real source) ----
# The value-parity checks below became gen-vs-gen self-comparisons after the
# Phase B cutover (consumers import the gen module). THIS is the check that
# catches a half-regeneration or a calibers.json edit without `npm run
# gen:calibers`: the sha256 of the CRLF-normalized source must match the sha
# stamped into every generated artifact (same normalization as the generator,
# ammoraccoon-web/scripts/gen-calibers/index.mjs). Body-level hand-edits of a
# gen file (sha header left intact) are caught by the generator's --check
# byte-compare, which the web repo's CI runs.
source_sha = hashlib.sha256(
    (ROOT / 'calibers.json').read_text(encoding='utf-8').replace('\r\n', '\n').encode('utf-8')
).hexdigest()
check('FRESHNESS caliber_registry_gen.py sha == sha256(calibers.json)',
      gen.REGISTRY_SHA256, source_sha)
_sql_head = (ROOT / 'migrations' / 'gen' / 'caliber_floors.values.sql').read_text(encoding='utf-8')
_sql_sha = re.search(r'Registry sha256: ([0-9a-f]{64})', _sql_head)
check('FRESHNESS caliber_floors.values.sql sha == sha256(calibers.json)',
      _sql_sha.group(1) if _sql_sha else None, source_sha)

# --- scraper_lib tables ------------------------------------------------------
check('CALIBERS', scraper_lib.CALIBERS, gen.CALIBERS)
check('CALIBER_PRICE_FLOORS', scraper_lib.CALIBER_PRICE_FLOORS, gen.CALIBER_PRICE_FLOORS)
check('DEFAULT_FLOOR', scraper_lib.DEFAULT_FLOOR, gen.DEFAULT_FLOOR)
check('CALIBER_TO_FLOOR_KEY', scraper_lib._CALIBER_TO_FLOOR_KEY, gen.CALIBER_TO_FLOOR_KEY)
check('CALIBER_PRICE_CEILINGS', scraper_lib.CALIBER_PRICE_CEILINGS, gen.CALIBER_PRICE_CEILINGS)
check('DEFAULT_CEILING', scraper_lib.DEFAULT_CEILING, gen.DEFAULT_CEILING)

# --- caliber_audit ranges ----------------------------------------------------
# Cutover-aware (Phase B). Pre-cutover (Phase A): caliber_audit held hand
# literals EXPECTED_RANGES/DEFAULT_RANGE — AST-extract and compare value-for-
# value against gen. Post-cutover (Phase B step 1, 2026-06-12): caliber_audit
# does `from caliber_registry_gen import AUDIT_EXPECTED_RANGES as
# EXPECTED_RANGES, AUDIT_DEFAULT_RANGE as DEFAULT_RANGE`, so the literal is
# gone and the value IS the gen value by construction — verify the WIRING
# (the import binds the right gen name to the right local) instead of a
# vacuous self-compare. Missing both a literal AND the import is a real
# failure: the consumer lost its ranges. (AST, not import — caliber_audit
# reads SUPABASE_URL at module load.)
audit_path = ROOT / 'scripts' / 'caliber_audit.py'
audit_lits = ast_assignments(audit_path, {'EXPECTED_RANGES', 'DEFAULT_RANGE'})
if 'EXPECTED_RANGES' in audit_lits or 'DEFAULT_RANGE' in audit_lits:
    check('AUDIT_EXPECTED_RANGES (hand literal == gen)',
          audit_lits.get('EXPECTED_RANGES'), gen.AUDIT_EXPECTED_RANGES)
    check('AUDIT_DEFAULT_RANGE (hand literal == gen)',
          audit_lits.get('DEFAULT_RANGE'), gen.AUDIT_DEFAULT_RANGE)
else:
    aliases = ast_import_aliases(audit_path, 'caliber_registry_gen')
    check('caliber_audit cut over: EXPECTED_RANGES <- gen.AUDIT_EXPECTED_RANGES',
          aliases.get('EXPECTED_RANGES'), 'AUDIT_EXPECTED_RANGES')
    check('caliber_audit cut over: DEFAULT_RANGE <- gen.AUDIT_DEFAULT_RANGE',
          aliases.get('DEFAULT_RANGE'), 'AUDIT_DEFAULT_RANGE')

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
# Cutover-aware (Phase B step 4). Pre-cutover: each source held its own
# CALIBER_NORMALIZE literal — the union-of-six had to equal gen with no
# cross-source conflicts. Post-cutover (2026-06-12): all six do
# `from caliber_registry_gen import BALLISTICS_CALIBER_NORMALIZE as
# CALIBER_NORMALIZE`, so there are no per-source literals to union — verify
# each source is WIRED to the union (the row-set impact of the switch was the
# 2 approved D2 adds, proven separately by scripts/_replay_ballistics_maps.py).
# Mixed states are handled so the suite stays green at every step.
union_from_literals = {}
conflicts = []
literal_sources = []
cutover_sources = []
for name, path in BALLISTICS:
    lits = ast_assignments(path, {'CALIBER_NORMALIZE'})
    if 'CALIBER_NORMALIZE' in lits:
        literal_sources.append(name)
        for alias, slug in lits['CALIBER_NORMALIZE'].items():
            if alias in union_from_literals and union_from_literals[alias] != slug:
                conflicts.append((alias, union_from_literals[alias], slug))
            union_from_literals[alias] = slug
    else:
        aliases = ast_import_aliases(path, 'caliber_registry_gen')
        cutover_sources.append((name, aliases.get('CALIBER_NORMALIZE')))

if len(literal_sources) == len(BALLISTICS):
    # Pre-cutover: full union-of-six must equal gen, no conflicts.
    check('BALLISTICS union-of-six == gen map', union_from_literals, gen.BALLISTICS_CALIBER_NORMALIZE)
    check('BALLISTICS no cross-source alias conflicts', conflicts, [])
else:
    for name, imported in cutover_sources:
        check(f'BALLISTICS {name} cut over: CALIBER_NORMALIZE <- gen.BALLISTICS_CALIBER_NORMALIZE',
              imported, 'BALLISTICS_CALIBER_NORMALIZE')
    if literal_sources:
        # Any source NOT yet cut over must still be a non-conflicting subset
        # of the gen union (no divergent hand value snuck in mid-migration).
        check('BALLISTICS remaining-literal no cross-source conflicts', conflicts, [])
        subset_ok = all(gen.BALLISTICS_CALIBER_NORMALIZE.get(a) == s
                        for a, s in union_from_literals.items())
        check(f'BALLISTICS remaining literals ({", ".join(literal_sources)}) subset of gen union',
              subset_ok, True)

# --- normalize_caliber wiring ------------------------------------------------
# Post-cutover, scraper_lib.normalize_caliber IS normalize_caliber_gen (the
# alias at scraper_lib.py:50), so the replay below compares a function to
# itself — kept only as an edge-corpus smoke of the gen detector. The real
# assertion is the WIRING: scraper_lib must bind normalize_caliber to the gen
# function (losing the alias would silently fork detection again).
_sl_aliases = ast_import_aliases(ROOT / 'scraper_lib.py', 'caliber_registry_gen')
check('scraper_lib cut over: normalize_caliber <- gen.normalize_caliber_gen',
      _sl_aliases.get('normalize_caliber'), 'normalize_caliber_gen')

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
