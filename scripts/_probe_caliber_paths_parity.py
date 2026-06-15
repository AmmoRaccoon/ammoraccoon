"""Caliber-paths loader parity probe (expansion #4, Step 2). Read-only.

Proves load_caliber_paths('<retailer>') / load_parent_paths('<retailer>')
reconstruct the EXACT url set of a scraper's inline literal — the gate for each
per-scraper migration (old-literal vs file-loaded must fetch the identical URL
set). The scraper is NOT imported (it builds a Supabase client at module load);
the literal is AST-extracted. No DB, no network, no browser.

Handles every inline CALIBER_PATHS shape:
  - dict -> str                          (outdoorlimited)
  - dict -> list[str]                    (natchez, targetsports)
  - dict -> (path, re.compile|None)      (gritr; URL is the tuple's first elt)
  - dict -> f-string url                 (buds; resolved via module constants)
  - list[str] / list[(path, type)]       (firearmsdepot; -> load_parent_paths)

f-string / absolute URLs are relativized against the config `base` so an
absolute inline url compares equal to the loader's relative `url`.

Bare-handle Shopify scrapers (trueshot, freedommunitions) store BARE
collection handles inline (e.g. 'pistol-9mm') but build the fetch URL as
'/collections/<handle>/products.json' at runtime; the config (blackbasin
precedent) stores the full '/collections/<handle>' path. Pass the optional
4th arg INLINE_PREFIX='/collections/' to prepend it to each inline handle
so the comparison is the FETCHED URL, not the raw literal. Default '' is a
no-op for every other retailer.

Symmetrically, fenix stores the FULL '/collections/<handle>/products.json'
inline but the config (same precedent) stores '/collections/<handle>' and
the scraper appends '/products.json' at runtime. Pass the optional 5th arg
INLINE_SUFFIX='/products.json' to STRIP it from each inline value so the
comparison recovers the stored config path. Default '' is a no-op. Prefix
and suffix can be combined; both transform the inline literal back into the
stored config path.

Usage:
    py scripts/_probe_caliber_paths_parity.py <retailer> <scraper_file> [literal_name] [inline_prefix] [inline_suffix]
Examples:
    py scripts/_probe_caliber_paths_parity.py outdoorlimited scraper_outdoorlimited.py
    py scripts/_probe_caliber_paths_parity.py buds scraper_buds.py CALIBER_FILTER_URLS
    py scripts/_probe_caliber_paths_parity.py firearmsdepot scraper_firearmsdepot.py PARENT_PATHS
    py scripts/_probe_caliber_paths_parity.py trueshot scraper_trueshot.py COLLECTION_HANDLES /collections/
    py scripts/_probe_caliber_paths_parity.py fenix scraper_fenix.py CALIBER_PATHS "" /products.json

Exit 0 = identical url set. Exit 1 = any divergence (prints the first one).
"""
import ast
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scraper_lib import load_caliber_paths, load_parent_paths  # noqa: E402


def _resolve_str(node, ns):
    """Resolve an AST node to a string via a namespace of known string consts.
    Handles Constant(str), Name (ns lookup), JoinedStr (f-string) and BinOp
    string concat. Raises ValueError if it can't resolve to a pure string."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        if node.id in ns:
            return ns[node.id]
        raise ValueError(f"unknown name {node.id!r}")
    if isinstance(node, ast.JoinedStr):
        parts = []
        for v in node.values:
            if isinstance(v, ast.Constant):
                parts.append(str(v.value))
            elif isinstance(v, ast.FormattedValue):
                parts.append(_resolve_str(v.value, ns))
            else:
                raise ValueError("unsupported f-string part")
        return ''.join(parts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return _resolve_str(node.left, ns) + _resolve_str(node.right, ns)
    raise ValueError("not a resolvable string node")


def _build_string_ns(tree):
    """Namespace of every module-level `NAME = <string>` (incl. f-strings that
    reference earlier string constants), resolved top-to-bottom."""
    ns = {}
    for node in tree.body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)):
            try:
                ns[node.targets[0].id] = _resolve_str(node.value, ns)
            except Exception:
                pass  # not a plain string — ignore
    return ns


def _find_assign(tree, name):
    for node in tree.body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == name):
            return node.value
    raise SystemExit(f'literal {name!r} not found')


def _value_urls(node, ns):
    """A dict value node -> [url_str]. str/f-string -> [it]; list -> each elt;
    tuple (path, regex) -> [first elt]."""
    if isinstance(node, ast.List):
        return [_resolve_str(el, ns) for el in node.elts]
    if isinstance(node, ast.Tuple):
        return [_resolve_str(node.elts[0], ns)]
    return [_resolve_str(node, ns)]


def _rel(u, base):
    return u[len(base):] if base and u.startswith(base) else u


def main():
    retailer = sys.argv[1] if len(sys.argv) > 1 else 'targetsports'
    scraper = sys.argv[2] if len(sys.argv) > 2 else 'scraper_targetsports.py'
    literal_name = sys.argv[3] if len(sys.argv) > 3 else 'CALIBER_PATHS'
    # Optional prefix prepended to each inline value before comparison —
    # for bare-handle Shopify scrapers whose runtime URL adds '/collections/'.
    inline_prefix = sys.argv[4] if len(sys.argv) > 4 else ''
    # Optional suffix stripped from each inline value before comparison —
    # for scrapers (fenix) whose inline literal embeds a runtime-appended
    # suffix like '/products.json' that the stored config path omits.
    inline_suffix = sys.argv[5] if len(sys.argv) > 5 else ''

    tree = ast.parse((ROOT / scraper).read_text(encoding='utf-8'))
    ns = _build_string_ns(tree)
    node = _find_assign(tree, literal_name)

    cfg = json.loads((ROOT / 'caliber_paths' / f'{retailer}.json')
                     .read_text(encoding='utf-8'))
    base = cfg.get('base', '')

    def _xform_inline(u):
        """Transform an inline literal value back into the stored config
        path: prepend inline_prefix and strip a trailing inline_suffix so
        the comparison is path-for-path with the loader output."""
        v = inline_prefix + _rel(u, base)
        if inline_suffix and v.endswith(inline_suffix):
            v = v[:-len(inline_suffix)]
        return v

    fails = []
    if isinstance(node, ast.Dict):
        inline = {_resolve_str(k, ns): [_xform_inline(u) for u in _value_urls(v, ns)]
                  for k, v in zip(node.keys, node.values)}
        loaded = {cal: [e['url'] for e in entries]
                  for cal, entries in load_caliber_paths(retailer).items()}
        kind = 'calibers'
        if set(inline) != set(loaded):
            only_i = sorted(set(inline) - set(loaded))
            only_l = sorted(set(loaded) - set(inline))
            if only_i:
                fails.append(f'calibers only in scraper literal: {only_i}')
            if only_l:
                fails.append(f'calibers only in config file: {only_l}')
        for cal in sorted(set(inline) & set(loaded)):
            if inline[cal] != loaded[cal]:
                fails.append(f"[{cal}] differs:\n    literal: {inline[cal]}\n"
                             f"    loaded : {loaded[cal]}")
        n_inline = sum(len(v) for v in inline.values())
        n_loaded = sum(len(v) for v in loaded.values())
        print(f'{retailer}: {len(loaded)} calibers, {n_loaded} active URLs '
              f'(literal: {len(inline)} calibers, {n_inline} URLs)')
    elif isinstance(node, ast.List):
        inline = []
        for el in node.elts:
            url = _resolve_str(el.elts[0], ns) if isinstance(el, ast.Tuple) \
                else _resolve_str(el, ns)
            inline.append(_rel(url, base))
        loaded = [e['url'] for e in load_parent_paths(retailer)]
        kind = 'parent_paths'
        if inline != loaded:
            fails.append(f'parent_paths differ:\n    literal: {inline}\n'
                         f'    loaded : {loaded}')
        print(f'{retailer}: {len(loaded)} parent paths '
              f'(literal: {len(inline)})')
    else:
        raise SystemExit(f'{literal_name}: unsupported literal node '
                         f'{type(node).__name__}')

    if fails:
        print(f'FAIL ({kind}) — url set NOT identical:')
        for f in fails:
            print('  ' + f)
        sys.exit(1)
    print(f'OK ({kind}) — config-loaded url set is byte-identical to the '
          f'inline literal.')
    sys.exit(0)


if __name__ == '__main__':
    main()
