"""READ-ONLY — caliber-registry Phase A ballistics replay (D2 proof).

For each of the six ballistics scrapers: run its real --dry-run crawl ONCE
(no DB writes; supabase=None), recording every key its code looks up in
CALIBER_NORMALIZE. Then diff, offline, the per-source hand map against the
generated union map (caliber_registry_gen.BALLISTICS_CALIBER_NORMALIZE)
over exactly the recorded keys.

Row-set identity follows from key-verdict identity: a parsed product's
caliber comes only from the map verdict (None = row dropped), so the old
and new maps produce identical row sets iff no observed key changes
verdict. Single-fetch design avoids the two-crawl nondeterminism a naive
"run twice and diff rows" would suffer.

Diff classes per source:
  changed  slugA -> slugB   (NEVER acceptable — would be a parity failure)
  added    None  -> slug    (the accepted-D2 class: union recognizes a
                             cartridge the source map dropped; ZERO required
                             for "identical row sets" — any non-zero count
                             is reported, not hidden)
  removed  slug  -> None    (must be impossible — union is a superset)
"""
import importlib.util
import inspect
import sys
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402
load_dotenv(ROOT / '.env')

import caliber_registry_gen as gen  # noqa: E402

SOURCES = [
    ('hornady', ROOT / 'scraper_hornady_ballistics.py'),
    ('winchester', ROOT / 'scraper_winchester_ballistics.py'),
    ('kinetic', ROOT / 'scraper_kinetic_ballistics.py'),
    ('magtech', ROOT / 'scripts' / 'scraper_magtech_ballistics.py'),
    ('pmc', ROOT / 'scripts' / 'scraper_pmc_ballistics.py'),
    ('sb', ROOT / 'scripts' / 'scraper_sb_ballistics.py'),
]


class RecordingDict(dict):
    def __init__(self, base, log):
        super().__init__(base)
        self._log = log

    def get(self, key, default=None):
        self._log.add(key)
        return super().get(key, default)

    def __getitem__(self, key):
        self._log.add(key)
        return super().__getitem__(key)

    def __contains__(self, key):
        self._log.add(key)
        return super().__contains__(key)


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


report = []
any_changed = 0
any_added = 0
any_errors = 0

for name, path in SOURCES:
    print(f'\n{"=" * 70}\nREPLAY {name} ({path.name})\n{"=" * 70}', flush=True)
    try:
        mod = load_module(path, f'replay_{name}')
        orig = dict(mod.CALIBER_NORMALIZE)
        observed = set()
        mod.CALIBER_NORMALIZE = RecordingDict(orig, observed)

        sig = inspect.signature(mod.scrape_source)
        rows = 0
        errors = []
        for source in mod.SOURCES:
            try:
                if 'target_calibers' in sig.parameters:
                    rows += mod.scrape_source(source, None, True)
                else:
                    rows += mod.scrape_source(source, True)
            except Exception as e:  # noqa: BLE001 — fetch flake is a report item, not a crash
                errors.append(f'{source}: {type(e).__name__}: {e}')

        changed = sorted(k for k in observed
                         if orig.get(k) is not None
                         and gen.BALLISTICS_CALIBER_NORMALIZE.get(k) is not None
                         and orig.get(k) != gen.BALLISTICS_CALIBER_NORMALIZE.get(k))
        added = sorted(k for k in observed
                       if orig.get(k) is None
                       and gen.BALLISTICS_CALIBER_NORMALIZE.get(k) is not None)
        removed = sorted(k for k in observed
                         if orig.get(k) is not None
                         and gen.BALLISTICS_CALIBER_NORMALIZE.get(k) is None)
        report.append((name, rows, len(observed), changed, added, removed, errors))
        any_changed += len(changed)
        any_added += len(added)
        any_errors += len(errors)
    except Exception:
        traceback.print_exc()
        report.append((name, 0, 0, [], [], [], ['module-level failure']))
        any_errors += 1

print(f'\n\n{"=" * 70}\nBALLISTICS REPLAY SUMMARY (old per-source map vs generated union map)\n{"=" * 70}')
for name, rows, nkeys, changed, added, removed, errors in report:
    status = 'IDENTICAL' if not (changed or added or removed) else 'DIFFS'
    print(f'{name:<11} rows={rows:<5} observed-keys={nkeys:<4} '
          f'changed={len(changed)} added={len(added)} removed={len(removed)}  -> {status}')
    for k in changed:
        print(f'    CHANGED  {k!r}: {dict.get(globals()["gen"].BALLISTICS_CALIBER_NORMALIZE, k)!r}')
    for k in added:
        print(f'    ADDED    {k!r} -> {gen.BALLISTICS_CALIBER_NORMALIZE[k]!r} (row would now be emitted)')
    for k in removed:
        print(f'    REMOVED  {k!r}')
    for e in errors:
        print(f'    FETCH-ERROR {e}')

print()
if any_changed or any_added:
    print('VERDICT: ROW SETS NOT IDENTICAL — report to Jon, do not proceed.')
    sys.exit(1)
if any_errors:
    print('VERDICT: zero map diffs on observed keys, but fetch errors occurred — report.')
    sys.exit(0)
print('VERDICT: IDENTICAL ROW SETS on all six sources.')
sys.exit(0)
