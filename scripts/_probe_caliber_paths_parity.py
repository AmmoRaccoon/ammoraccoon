"""Caliber-paths loader parity probe (expansion #4, Step 2). Read-only.

Proves load_caliber_paths('<retailer>') reconstructs the EXACT url set of a
scraper's inline CALIBER_PATHS literal — the gate for each per-scraper
migration (old-literal vs file-loaded must fetch the identical URL set). The
scraper is NOT imported (it builds a Supabase client at module load); the
literal is AST-extracted. No DB, no network, no browser.

Usage:
    py scripts/_probe_caliber_paths_parity.py <retailer> <scraper_file> [literal_name]
Example:
    py scripts/_probe_caliber_paths_parity.py targetsports scraper_targetsports.py

Exit 0 = identical url set. Exit 1 = any divergence (prints the first one).
"""
import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scraper_lib import load_caliber_paths  # noqa: E402


def inline_url_set(scraper_file, literal_name='CALIBER_PATHS'):
    """AST-extract the scraper's caliber->URL literal, normalized to
    {caliber: [url, ...]} (a dict->str map becomes single-element lists so the
    simple shape compares too). Tuple/regex shapes (gritr) are not literal-
    evaluable and are out of scope for this probe."""
    tree = ast.parse((ROOT / scraper_file).read_text(encoding='utf-8'))
    for node in tree.body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == literal_name):
            raw = ast.literal_eval(node.value)
            return {k: (list(v) if isinstance(v, (list, tuple)) else [v])
                    for k, v in raw.items()}
    raise SystemExit(f'{scraper_file}: no {literal_name} literal found')


def loader_url_set(retailer):
    return {cal: [e['url'] for e in entries]
            for cal, entries in load_caliber_paths(retailer).items()}


def main():
    retailer = sys.argv[1] if len(sys.argv) > 1 else 'targetsports'
    scraper = sys.argv[2] if len(sys.argv) > 2 else 'scraper_targetsports.py'
    literal_name = sys.argv[3] if len(sys.argv) > 3 else 'CALIBER_PATHS'

    inline = inline_url_set(scraper, literal_name)
    loaded = loader_url_set(retailer)

    fails = []
    if set(inline) != set(loaded):
        only_inline = sorted(set(inline) - set(loaded))
        only_loaded = sorted(set(loaded) - set(inline))
        if only_inline:
            fails.append(f'calibers only in scraper literal: {only_inline}')
        if only_loaded:
            fails.append(f'calibers only in config file: {only_loaded}')
    for cal in sorted(set(inline) & set(loaded)):
        if inline[cal] != loaded[cal]:
            fails.append(f"[{cal}] url list differs:\n"
                         f"    literal: {inline[cal]}\n"
                         f"    loaded : {loaded[cal]}")

    total_urls = sum(len(v) for v in loaded.values())
    print(f'{retailer}: {len(loaded)} calibers, {total_urls} active URLs '
          f'(scraper literal: {len(inline)} calibers, '
          f'{sum(len(v) for v in inline.values())} URLs)')
    if fails:
        print('FAIL — url set NOT identical:')
        for f in fails:
            print('  ' + f)
        sys.exit(1)
    print('OK — config-loaded url set is byte-identical to the inline literal.')
    sys.exit(0)


if __name__ == '__main__':
    main()
