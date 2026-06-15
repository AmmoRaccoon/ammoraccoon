"""Canonical (de)serialization for caliber_paths/<retailer>.json configs.

The write-side counterpart to scraper_lib.load_caliber_paths (the read side).
Both operate on caliber_paths/. The Step-3 validation harness writes measured
`validation` blocks back into these configs; this module guarantees those
writes are a CLEAN GIT DIFF (only the lines that changed move) by emitting ONE
canonical form: schema key order, 2-space indent, UTF-8 literal, trailing
newline.

The one-time normalization (`python caliber_paths_io.py --write`) runs every
existing config through `dump_config` once so all 29 share the canonical form;
after that, harness writes never reformat unrelated lines. Normalization is
NO-SEMANTIC-CHANGE by construction and self-checks json-equality before writing
(refuses to write a file whose parsed data would differ).

Key ordering only reorders keys of KNOWN schema objects (top-level config,
entry, validation, discovery, fetch, selectors). Free-key maps (`calibers`,
whose keys are caliber slugs) and all arrays preserve their existing order —
caliber order and the .223-then-5.56 leg order are meaningful and untouched.
Any unknown key is preserved (appended after the known ones) so nothing is
ever dropped.
"""
import argparse
import json
import os
import sys

CALIBER_PATHS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 'caliber_paths')
_SCHEMA_FILE = 'caliber_paths.schema.json'

# Canonical key order per known schema object (mirrors caliber_paths.schema.json).
_TOP_ORDER = ['retailer', 'retailer_slug', 'platform', 'base', 'comment',
              'discovery', 'fetch', 'selectors', 'calibers', 'parent_paths']
_DISCOVERY_ORDER = ['method', 'sitemap', 'category_url_pattern']
_FETCH_ORDER = ['mode', 'stealth', 'fresh_context_per_request', 'user_agent']
_SELECTORS_ORDER = ['product_card', 'product_title']
_ENTRY_ORDER = ['path', 'query', 'status', 'expect_landed', 'title_filter',
                'type', 'source', 'validation', 'approved_by', 'approved_at']
_VALIDATION_ORDER = ['method', 'validated_at', 'status', 'redirect',
                     'title_match', 'gate_pass_pct', 'n_products', 'note']


def _ordered(d, order):
    """Return a dict with `order` keys first (present ones only), then any
    remaining keys in their existing order. Never drops or invents keys."""
    out = {k: d[k] for k in order if k in d}
    for k in d:
        if k not in out:
            out[k] = d[k]
    return out


def _canon_entry(e):
    if not isinstance(e, dict):
        return e
    e = dict(e)
    if isinstance(e.get('validation'), dict):
        e['validation'] = _ordered(e['validation'], _VALIDATION_ORDER)
    return _ordered(e, _ENTRY_ORDER)


def canon_config(cfg):
    """Return a deep copy of `cfg` with known-object keys in canonical order.
    Free-key maps and arrays keep their existing order."""
    if not isinstance(cfg, dict):
        return cfg
    c = dict(cfg)
    if isinstance(c.get('discovery'), dict):
        c['discovery'] = _ordered(c['discovery'], _DISCOVERY_ORDER)
    if isinstance(c.get('fetch'), dict):
        c['fetch'] = _ordered(c['fetch'], _FETCH_ORDER)
    if isinstance(c.get('selectors'), dict):
        c['selectors'] = _ordered(c['selectors'], _SELECTORS_ORDER)
    if isinstance(c.get('calibers'), dict):
        c['calibers'] = {cal: [_canon_entry(e) for e in entries]
                         for cal, entries in c['calibers'].items()}
    if isinstance(c.get('parent_paths'), list):
        c['parent_paths'] = [_canon_entry(e) for e in c['parent_paths']]
    return _ordered(c, _TOP_ORDER)


def dump_config(cfg):
    """Serialize a config to the canonical string form (trailing newline)."""
    return json.dumps(canon_config(cfg), indent=2, ensure_ascii=False) + '\n'


def load_config(path):
    """Read a config file to a raw dict (no loader transforms)."""
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def iter_config_files():
    """Sorted absolute paths of every caliber_paths/<retailer>.json (the
    schema file itself is excluded)."""
    return sorted(
        os.path.join(CALIBER_PATHS_DIR, fn)
        for fn in os.listdir(CALIBER_PATHS_DIR)
        if fn.endswith('.json') and fn != _SCHEMA_FILE
    )


def _normalize(write):
    """Normalize every config to the canonical form. Returns exit code.
    Self-checks json-equality (parsed data identical) before writing — a
    file whose data would change is a BUG in the canonicalizer and aborts."""
    changed, unchanged, broke = [], 0, []
    for path in iter_config_files():
        name = os.path.basename(path)
        with open(path, encoding='utf-8') as f:
            original_text = f.read()
        data = json.loads(original_text)
        canonical_text = dump_config(data)
        # Semantic safety gate: canonical form must parse to identical data.
        if json.loads(canonical_text) != data:
            broke.append(name)
            continue
        if canonical_text == original_text:
            unchanged += 1
            continue
        changed.append(name)
        if write:
            with open(path, 'w', encoding='utf-8', newline='\n') as f:
                f.write(canonical_text)

    if broke:
        print(f"ABORT: canonicalizer would change parsed data for: {broke}")
        return 1
    verb = 'rewrote' if write else 'would reformat'
    print(f"{verb} {len(changed)} file(s); {unchanged} already canonical; "
          f"0 semantic changes (json-equality verified for all "
          f"{len(changed) + unchanged}).")
    for name in changed:
        print(f"  {verb}: {name}")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--write', action='store_true',
                    help='rewrite configs in place (default: check only)')
    args = ap.parse_args()
    return _normalize(write=args.write)


if __name__ == '__main__':
    sys.exit(main())
