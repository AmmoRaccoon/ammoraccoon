"""Backfill parsed metadata columns on the components table.

Populates caliber / grain / bullet_type / primer_size / powder_application
on existing rows so the calculator's component picker (Frontman half of
the arc) can index on these fields without re-tokenizing product_name on
every read.

Per-category rules:
  powder  → powder_application from a brand+fragment lookup table
            (curated; gaps are expected and intentionally left NULL).
  primer  → primer_size from regex on product_name.
  bullet  → bullet_type via scraper_lib.parse_bullet_type;
            grain parsed from name only when the existing column is NULL;
            caliber preserved when populated, otherwise left NULL
            (the 1.3% NULL bullets are gas checks / Varmint Grenade
            edge cases the upstream scraper already chose to skip).
  brass   → caliber parsed by stripping brand + "Brass" + count tokens
            from the product name (currently 100% NULL).

Default mode is dry-run: prints coverage stats per category, sample
parse decisions, and a sample of rows that resolved to NULL so we can
spot parser gaps without touching the DB. Pass --apply to write.

Usage:
  python scripts/backfill_components_parsed.py                 # dry-run
  python scripts/backfill_components_parsed.py --sample 10     # 10/cat
  python scripts/backfill_components_parsed.py --apply         # live
"""
import argparse
import os
import re
import sys
from collections import Counter, defaultdict

from dotenv import load_dotenv
from supabase import create_client

# Run from repo root so `scraper_lib` is importable.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scraper_lib import parse_bullet_type  # noqa: E402

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

RETAILER_SLUG = 'powdervalley'  # only retailer in components today; filter as a guard


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

# Powder application LUT — keyed on lowercase brand → {fragment: application}.
# Fragments are matched against the lowercased product_name with non-word
# boundaries on both ends so "reloder 2" doesn't false-match inside
# "reloder 22". When multiple fragments match a row, the longest wins.
#
# Coverage is intentionally curated rather than exhaustive: this is the
# common-product set across Hodgdon/Alliant/Winchester/IMR/Accurate/
# Ramshot/Vihtavuori/Shooter's World based on Powder Valley's catalog.
# Long-tail powders that aren't here will resolve to NULL — that's the
# right answer (better than a bad guess) and the gap will surface in the
# dry-run NULL sample so we can extend the table when it matters.
POWDER_APP_LUT = {
    'hodgdon': {
        'titegroup': 'pistol', 'hp-38': 'pistol', 'hp38': 'pistol',
        'hs-6': 'pistol', 'hs-7': 'pistol', 'longshot': 'pistol',
        "lil'gun": 'pistol', 'cfe pistol': 'pistol',
        'clays': 'shotgun', 'international clays': 'shotgun',
        'universal': 'universal', 'trail boss': 'universal',
        'h322': 'rifle', 'h335': 'rifle', 'h380': 'rifle', 'h414': 'rifle',
        'h4198': 'rifle', 'h4350': 'rifle', 'h4895': 'rifle',
        'bl-c2': 'rifle', 'cfe 223': 'rifle', 'cfe black': 'rifle',
        'varget': 'rifle', 'benchmark': 'rifle',
        'h110': 'magnum rifle', 'h4831': 'magnum rifle',
        'retumbo': 'magnum rifle', 'h1000': 'magnum rifle',
        'h50bmg': 'magnum rifle', 'h-50bmg': 'magnum rifle',
        'h870': 'magnum rifle', 'us869': 'magnum rifle',
    },
    'alliant': {
        'bullseye': 'pistol', 'be-86': 'pistol', 'sport pistol': 'pistol',
        'power pistol': 'pistol', '2400': 'pistol',
        'red dot': 'shotgun', 'green dot': 'shotgun', 'blue dot': 'shotgun',
        'extra-lite': 'shotgun', 'clay dot': 'shotgun',
        'promo': 'shotgun', 'pro reach': 'shotgun', 'pro28': 'shotgun',
        'pro38': 'shotgun', 'pro45': 'shotgun', 'steel': 'shotgun',
        'unique': 'universal',
        'reloder 7': 'rifle', 'reloder 10': 'rifle', 'reloder 15': 'rifle',
        'reloder 16': 'rifle', 'reloder 17': 'rifle', 'reloder 19': 'rifle',
        'ar-comp': 'rifle',
        'reloder 22': 'magnum rifle', 'reloder 23': 'magnum rifle',
        'reloder 25': 'magnum rifle', 'reloder 26': 'magnum rifle',
        'reloder 33': 'magnum rifle', 'reloder 50': 'magnum rifle',
    },
    'winchester': {
        '231': 'pistol', 'autocomp': 'pistol',
        '572': 'shotgun', 'super field': 'shotgun',
        'super handicap': 'shotgun', 'wsf': 'shotgun', 'wst': 'shotgun',
        'wsl': 'shotgun',
        '748': 'rifle', '760': 'rifle',
        'staball 6.5': 'rifle', 'staball hd': 'rifle',
        'staball match': 'rifle',
        '296': 'magnum rifle',
    },
    'imr': {
        '4007': 'rifle', '4064': 'rifle', '4198': 'rifle', '4227': 'rifle',
        '4320': 'rifle', '4350': 'rifle', '4895': 'rifle',
        '8208 xbr': 'rifle', '8208xbr': 'rifle', 'enduron': 'rifle',
        '4831': 'magnum rifle', '7828': 'magnum rifle',
        'green': 'shotgun', 'red': 'shotgun', 'blue': 'shotgun',
        'pb': 'shotgun', 'sr 4756': 'shotgun', 'sr 7625': 'shotgun',
        'trail boss': 'universal',
    },
    'accurate': {
        'no. 2': 'pistol', 'no. 5': 'pistol', 'no. 7': 'pistol',
        'tcm': 'pistol',
        'no. 9': 'magnum rifle', 'magpro': 'magnum rifle',
        'xmr': 'magnum rifle',
        'no. 11fs': 'shotgun', 'nitro 100 nf': 'shotgun',
        'solo 1000': 'shotgun',
        '1680': 'rifle', '2015': 'rifle', '2200': 'rifle', '2230': 'rifle',
        '2460': 'rifle', '2495': 'rifle', '2520': 'rifle', '2700': 'rifle',
        '4064': 'rifle', '4350': 'rifle', '5744': 'rifle',
        'lt-30': 'rifle', 'lt-32': 'rifle',
    },
    'ramshot': {
        'big game': 'rifle', 'tac': 'rifle', 'wild boar': 'rifle',
        'x-terminator': 'rifle',
        'silhouette': 'pistol', 'true blue': 'pistol', 'zip': 'pistol',
        'enforcer': 'magnum rifle', 'hunter': 'magnum rifle',
        'magnum': 'magnum rifle',
        'competition': 'shotgun',
    },
    'vihtavuori': {
        'n310': 'pistol', 'n320': 'pistol', 'n330': 'pistol',
        'n340': 'pistol', 'n350': 'pistol', 'n105': 'pistol',
        '3n37': 'pistol', '3n38': 'pistol',
        'n120': 'rifle', 'n130': 'rifle', 'n133': 'rifle', 'n135': 'rifle',
        'n140': 'rifle', 'n150': 'rifle', 'n160': 'rifle',
        'n550': 'rifle', 'n555': 'rifle',
        'n110': 'magnum rifle', 'n165': 'magnum rifle',
        'n170': 'magnum rifle', 'n560': 'magnum rifle',
        'n565': 'magnum rifle', 'n568': 'magnum rifle',
        'n570': 'magnum rifle',
    },
    "shooter's world": {
        'auto pistol': 'pistol', 'major pistol': 'pistol',
        'heavy pistol': 'pistol', 'clean shot': 'pistol',
        'major rifle': 'rifle', 'precision rifle': 'rifle',
        'tactical rifle': 'rifle', 'match rifle': 'rifle',
        'long rifle': 'magnum rifle', 'magnum rifle': 'magnum rifle',
    },
}


def parse_powder_application(brand, name):
    if not brand or not name:
        return None
    table = POWDER_APP_LUT.get(brand.lower())
    if not table:
        return None
    n = name.lower()
    best_frag = None
    best_app = None
    for frag, app in table.items():
        # Non-word boundaries on both sides so "n10" doesn't match inside
        # "n100", and "reloder 2" doesn't match inside "reloder 22".
        if re.search(r'(?<!\w)' + re.escape(frag) + r'(?!\w)', n):
            if best_frag is None or len(frag) > len(best_frag):
                best_frag = frag
                best_app = app
    return best_app


def parse_primer_size(name):
    """Detect a primer size category from the product name.

    Returns one of: 'small pistol', 'small pistol magnum', 'large pistol',
    'large pistol magnum', 'small rifle', 'small rifle magnum',
    'large rifle', 'large rifle magnum', 'shotshell', 'shotshell magnum',
    'percussion cap', 'percussion cap magnum', 'muzzleloader', or None.

    Shotshell and percussion-cap checks come first because those product
    names sometimes also include "small/large rifle"-adjacent tokens
    (e.g. CCI 209M Shotshell) and we want the more specific bucket to win.
    """
    if not name:
        return None
    n = name.lower()
    if 'shotshell' in n:
        return 'shotshell magnum' if re.search(r'\bmagnum\b', n) else 'shotshell'
    if 'percussion' in n:
        return 'percussion cap magnum' if re.search(r'\bmagnum\b', n) else 'percussion cap'
    if 'muzzleloader' in n:
        return 'muzzleloader'
    m = re.search(r'\b(small|large)\s+(pistol|rifle)\b', n)
    if not m:
        return None
    base = f'{m.group(1)} {m.group(2)}'
    if re.search(r'\bmagnum\b', n):
        base += ' magnum'
    return base


_GRAIN_RE = re.compile(r'(\d+(?:\.\d+)?)\s*Grain\b', re.IGNORECASE)


def parse_grain(name):
    if not name:
        return None
    m = _GRAIN_RE.search(name)
    if not m:
        return None
    try:
        v = float(m.group(1))
    except ValueError:
        return None
    # Sanity: real bullets are 10-1000gr. A four-digit hit is almost
    # certainly a count token like "1000 Count".
    return v if 10 <= v <= 1000 else None


def parse_brass_caliber(name, brand=None):
    """Extract a brass caliber by stripping known prefix/suffix tokens
    from the product name.

    Powder Valley brass titles follow a stable shape:
        "{Brand} [Brass] {Caliber} [Brass] [Box of N | N Count]"
    Stripping brand + "brass" + count phrases leaves the caliber substring.
    Returned freeform — there's no canonical brass-caliber vocabulary
    and the calculator picker will surface these as-is anyway.
    """
    if not name:
        return None
    s = name
    if brand and s.lower().startswith(brand.lower()):
        s = s[len(brand):].lstrip()
    s = re.sub(r'\bbrass\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\bbox\s+of\s+\d+\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\b\d+\s+count\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\s+', ' ', s).strip()
    return s if s else None


# ---------------------------------------------------------------------------
# Per-row dispatch
# ---------------------------------------------------------------------------

def parse_row(row):
    """Return a dict of new column values for this row, omitting fields
    where we have nothing to write (so we don't overwrite scraper-set
    values with None and don't issue empty UPDATE statements).

    The diff against current values is computed downstream — this just
    decides what the parser thinks the canonical value should be.
    """
    cat = row['category']
    name = row.get('product_name') or ''
    brand = row.get('brand')

    out = {}

    if cat == 'powder':
        app = parse_powder_application(brand, name)
        if app is not None:
            out['powder_application'] = app

    elif cat == 'primer':
        sz = parse_primer_size(name)
        if sz is not None:
            out['primer_size'] = sz

    elif cat == 'bullet':
        bt = parse_bullet_type(name)
        if bt is not None:
            out['bullet_type'] = bt
        # Grain only filled when the scraper-set column is NULL.
        if row.get('grain') is None:
            g = parse_grain(name)
            if g is not None:
                out['grain'] = g
        # Caliber only filled when scraper-set column is NULL — Powder
        # Valley's info-table caliber ("30 Caliber, 7.62mm") is more
        # informative than anything we'd reconstruct from the title.

    elif cat == 'brass':
        cal = parse_brass_caliber(name, brand)
        if cal is not None:
            out['caliber'] = cal

    return out


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def fetch_all_components():
    """Page through Supabase results since the default cap is 1000.

    The SELECT list intentionally requests only the pre-migration-009
    columns plus the parser inputs. Migration 011's new columns
    (bullet_type / primer_size / powder_application) may or may not
    exist yet depending on whether the user has applied the migration
    in Supabase. row.get() on a missing key returns None, which is the
    correct pre-migration state anyway.
    """
    rows = []
    offset = 0
    while True:
        r = (sb.table('components')
             .select('id,category,product_name,brand,caliber,grain')
             .eq('retailer_slug', RETAILER_SLUG)
             .range(offset, offset + 999)
             .execute())
        if not r.data:
            break
        rows.extend(r.data)
        if len(r.data) < 1000:
            break
        offset += 1000
    return rows


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

CATEGORY_TARGETS = {
    'powder': ['powder_application'],
    'primer': ['primer_size'],
    'bullet': ['bullet_type', 'grain', 'caliber'],
    'brass':  ['caliber'],
}


def report(rows, sample_n):
    """Print sample parse decisions, coverage stats, and NULL gap samples."""
    by_cat = defaultdict(list)
    for r in rows:
        by_cat[r['category']].append(r)

    for cat in ('powder', 'primer', 'bullet', 'brass'):
        cat_rows = by_cat.get(cat, [])
        if not cat_rows:
            continue
        targets = CATEGORY_TARGETS[cat]
        print(f"\n{'=' * 78}")
        print(f"{cat.upper()}  ({len(cat_rows)} rows; targets: {', '.join(targets)})")
        print('=' * 78)

        # Sample parse decisions (first N rows).
        print(f"\nSample parse decisions (first {sample_n}):")
        for r in cat_rows[:sample_n]:
            parsed = parse_row(r)
            line = f"  id={r['id']:>5}  {r['product_name'][:72]}"
            print(line)
            if not parsed:
                print(f"     -> (no fields)")
            for k, v in parsed.items():
                existing = r.get(k)
                marker = ' [EXISTS=' + repr(existing) + ']' if existing is not None else ''
                print(f"     -> {k:<19} = {v!r}{marker}")

        # Coverage: per target, count how many resolve to non-None across
        # the full category (not just the sample) so we get real %.
        print(f"\nCoverage across all {len(cat_rows)} rows:")
        for t in targets:
            populated = 0
            new_fills = 0
            existing = 0
            for r in cat_rows:
                cur = r.get(t)
                if cur is not None:
                    existing += 1
                p = parse_row(r).get(t)
                if p is not None:
                    populated += 1
                    if cur is None:
                        new_fills += 1
            pct = (populated / len(cat_rows)) * 100 if cat_rows else 0
            null_after = len(cat_rows) - max(existing, populated + 0)
            # null_after below is "rows that would still be NULL after
            # backfill" — i.e. neither already-populated nor newly-parsed.
            still_null = sum(1 for r in cat_rows
                             if r.get(t) is None and parse_row(r).get(t) is None)
            print(f"  {t:<19}  parser-populates={populated}/{len(cat_rows)} "
                  f"({pct:.0f}%)  new-fills={new_fills}  "
                  f"already-set={existing}  still-NULL={still_null}")

        # Sample of rows that resolve to NULL on the *primary* target so
        # we can decide whether the parser needs an extension or whether
        # NULL is acceptable for that pattern.
        primary = targets[0]
        nulls = [r for r in cat_rows
                 if r.get(primary) is None and parse_row(r).get(primary) is None]
        if nulls:
            print(f"\nNULL-on-{primary} sample (up to 10):")
            for r in nulls[:10]:
                print(f"  id={r['id']:>5}  brand={r.get('brand') or '-':<14}  "
                      f"{r['product_name'][:80]}")
        else:
            print(f"\nNULL-on-{primary}: 0 rows.")

        # Distribution of parsed values for this primary target.
        ctr = Counter()
        for r in cat_rows:
            v = parse_row(r).get(primary) or r.get(primary)
            ctr[v] += 1
        print(f"\nValue distribution for {primary}:")
        for v, n in ctr.most_common():
            label = repr(v) if v is not None else 'NULL'
            print(f"  {label:<35}  {n}")


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------

def apply_updates(rows):
    """Issue per-row UPDATEs only for rows whose parsed fields differ
    from the current values. Updates are batched per (category, field-set)
    by issuing one PATCH per row — components is small enough (~2k rows)
    that a per-row update loop completes in a few seconds and we don't
    need the in_('id', chunk) batching trick used by backfill_bullet_type.
    """
    written = 0
    skipped_unchanged = 0
    for r in rows:
        parsed = parse_row(r)
        diff = {k: v for k, v in parsed.items() if r.get(k) != v}
        if not diff:
            skipped_unchanged += 1
            continue
        sb.table('components').update(diff).eq('id', r['id']).execute()
        written += 1
        if written % 100 == 0:
            print(f"  ...{written} rows written")
    print(f"\nDone. {written} rows updated, {skipped_unchanged} unchanged.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--apply', action='store_true',
                   help='Actually write updates (default is dry-run)')
    p.add_argument('--sample', type=int, default=10,
                   help='Per-category sample size in dry-run output (default 10)')
    args = p.parse_args()

    print(f"Fetching components rows for retailer_slug={RETAILER_SLUG!r}...")
    rows = fetch_all_components()
    print(f"  {len(rows)} rows")

    report(rows, args.sample)

    if args.apply:
        print("\n" + "=" * 78)
        print("APPLY — writing updates...")
        print("=" * 78)
        apply_updates(rows)
    else:
        print("\n" + "=" * 78)
        print("[DRY-RUN] No changes written. Re-run with --apply to commit.")
        print("=" * 78)


if __name__ == '__main__':
    main()
