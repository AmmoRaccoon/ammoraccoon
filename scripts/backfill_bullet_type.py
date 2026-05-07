"""Backfill bullet_type on rows where it's NULL but the parser can derive one.

Covers two tables:
  - listings  (per-retailer ammo SKUs; parses on product_url since
    listings has no `title` column)
  - components (reloading components; parses on product_name)

Fill-NULL-only policy: never overwrites a non-NULL bullet_type even if
the current parser would now disagree. Existing values were either set
by the in-scrape parse_bullet_type call (so re-parsing the same URL
slug should yield the same result anyway) or set explicitly by a
scraper from authoritative source data — both should be preserved.

Skips Gorilla (retailer_id=29) on listings because Gorilla's source
slugs carry no bullet-type info — anything derived from a "9mm-147gr"
slug there would be a guess. Components rows are unfiltered (only one
retailer in components today, Powder Valley, and its product_name
strings are richly typed).

Default mode is dry-run. Pass --apply to write.

Usage:
    python scripts/backfill_bullet_type.py            # dry-run
    python scripts/backfill_bullet_type.py --apply    # live
    python scripts/backfill_bullet_type.py --table listings    # only listings
    python scripts/backfill_bullet_type.py --table components  # only components
"""
import argparse
import os
import sys
from collections import defaultdict, Counter
from dotenv import load_dotenv
from supabase import create_client

# Run from repo root so `scraper_lib` is importable.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scraper_lib import parse_bullet_type, BULLET_TYPES  # noqa: E402

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

SKIP_RETAILER_IDS = {29}  # Gorilla — source data has no bullet-type info.


def fetch_all(table, select, **filters):
    """Page through Supabase results since the default cap is 1000."""
    rows = []
    offset = 0
    while True:
        q = sb.table(table).select(select)
        for k, v in filters.items():
            if v is None:
                q = q.is_(k, 'null')
            else:
                q = q.eq(k, v)
        r = q.range(offset, offset + 999).execute()
        if not r.data:
            break
        rows.extend(r.data)
        if len(r.data) < 1000:
            break
        offset += 1000
    return rows


# ---------------------------------------------------------------------------
# listings backfill
# ---------------------------------------------------------------------------

def collect_listings_candidates():
    """Return [(id, retailer_id, product_url, derived_bullet_type)] for every
    NULL-bullet_type listings row whose URL parses to a non-None type."""
    print('Fetching NULL bullet_type listings...')
    null_rows = fetch_all('listings', 'id,retailer_id,product_url',
                          bullet_type=None)
    print(f'  {len(null_rows)} listings with NULL bullet_type')

    candidates = []
    skip_no_url = 0
    skip_excluded = 0
    skip_no_match = 0
    for r in null_rows:
        if r['retailer_id'] in SKIP_RETAILER_IDS:
            skip_excluded += 1
            continue
        url = r.get('product_url')
        if not url:
            skip_no_url += 1
            continue
        bt = parse_bullet_type(url)
        if bt is None or bt not in BULLET_TYPES:
            skip_no_match += 1
            continue
        candidates.append((r['id'], r['retailer_id'], url, bt))

    print(f'  {len(candidates)} listings would update')
    print(f'  {skip_no_match} no parser match (slug ambiguous)')
    print(f'  {skip_no_url} no product_url')
    print(f'  {skip_excluded} excluded retailer (Gorilla)')
    return candidates


def report_listings(candidates):
    if not candidates:
        print('\n(listings) Nothing to update.')
        return

    retailers = {r['id']: r['slug']
                 for r in sb.table('retailers').select('id,slug').execute().data}

    by_retailer = defaultdict(Counter)
    by_type = Counter()
    for _, rid, _, bt in candidates:
        by_retailer[rid][bt] += 1
        by_type[bt] += 1

    print('\n' + '=' * 78)
    print(f'LISTINGS — DRY-RUN — {len(candidates)} row(s) would be updated')
    print('=' * 78)

    print('\nBy target bullet_type:')
    for bt, n in by_type.most_common():
        print(f'  {bt:<12}  {n:>5}')

    print('\nBy retailer:')
    print(f'  {"id":>3}  {"slug":<22}  {"rows":>5}  type-breakdown (top 5)')
    print(f'  {"-"*3}  {"-"*22}  {"-"*5}  {"-"*40}')
    for rid in sorted(by_retailer.keys(),
                      key=lambda k: -sum(by_retailer[k].values())):
        slug = retailers.get(rid, '?')
        types = by_retailer[rid]
        total = sum(types.values())
        breakdown = ', '.join(f'{bt}={n}' for bt, n in types.most_common(5))
        print(f'  {rid:>3}  {slug:<22}  {total:>5}  {breakdown}')

    # 30-row sample, balanced across (retailer, bt)
    print('\n30-row sample (varied retailers + types):')
    seen_combo = set()
    sample = []
    for cid, rid, url, bt in candidates:
        key = (rid, bt)
        if key in seen_combo and len(sample) < 30:
            continue
        seen_combo.add(key)
        sample.append((cid, rid, url, bt))
        if len(sample) >= 30:
            break
    for cid, rid, url, bt in sample:
        slug = retailers.get(rid, '?')[:18]
        print(f'  id={cid:>6} [{slug:<18}] -> {bt:<8}  ...{url[-90:]}')


def apply_listings(candidates):
    if not candidates:
        print('\n(listings) Nothing to apply.')
        return

    groups = defaultdict(list)
    for cid, _, _, bt in candidates:
        groups[bt].append(cid)

    print(f'\nLISTINGS APPLY — {len(groups)} grouped update(s) covering '
          f'{len(candidates)} row(s)...')
    BATCH = 200
    total_written = 0
    for bt, ids in groups.items():
        for i in range(0, len(ids), BATCH):
            chunk = ids[i:i + BATCH]
            r = sb.table('listings').update({'bullet_type': bt}) \
                .in_('id', chunk).execute()
            n = len(r.data) if r.data else 0
            total_written += n
            print(f'  {bt:<10}  ids[{i}:{i+len(chunk)}]  -> {n} rows updated')
    print(f'\nlistings done — {total_written} rows written.')


# ---------------------------------------------------------------------------
# components backfill
# ---------------------------------------------------------------------------

def collect_components_candidates():
    """Same shape as listings but reads components where category='bullet'
    AND bullet_type IS NULL. Parses on product_name (component table has
    a real title column, unlike listings)."""
    print('\nFetching NULL bullet_type components (category=bullet)...')
    null_rows = fetch_all('components',
                          'id,product_name,brand,bullet_type',
                          category='bullet', bullet_type=None)
    print(f'  {len(null_rows)} components with NULL bullet_type')

    candidates = []
    skip_no_match = 0
    for r in null_rows:
        name = r.get('product_name') or ''
        if not name:
            skip_no_match += 1
            continue
        bt = parse_bullet_type(name)
        if bt is None or bt not in BULLET_TYPES:
            skip_no_match += 1
            continue
        candidates.append((r['id'], r.get('brand'), name, bt))

    print(f'  {len(candidates)} components would update')
    print(f'  {skip_no_match} no parser match')
    return candidates


def report_components(candidates):
    if not candidates:
        print('\n(components) Nothing to update.')
        return

    by_type = Counter(bt for _, _, _, bt in candidates)
    by_brand = defaultdict(Counter)
    for _, brand, _, bt in candidates:
        by_brand[brand or '(no brand)'][bt] += 1

    print('\n' + '=' * 78)
    print(f'COMPONENTS — DRY-RUN — {len(candidates)} row(s) would be updated')
    print('=' * 78)

    print('\nBy target bullet_type:')
    for bt, n in by_type.most_common():
        print(f'  {bt:<12}  {n:>5}')

    print('\nBy brand:')
    print(f'  {"brand":<20}  {"rows":>5}  type-breakdown (top 5)')
    print(f'  {"-"*20}  {"-"*5}  {"-"*40}')
    for brand in sorted(by_brand.keys(),
                        key=lambda k: -sum(by_brand[k].values())):
        types = by_brand[brand]
        total = sum(types.values())
        breakdown = ', '.join(f'{bt}={n}' for bt, n in types.most_common(5))
        print(f'  {brand:<20}  {total:>5}  {breakdown}')

    print('\n20-row sample (varied brand + type):')
    seen_combo = set()
    sample = []
    for cid, brand, name, bt in candidates:
        key = (brand, bt)
        if key in seen_combo and len(sample) < 20:
            continue
        seen_combo.add(key)
        sample.append((cid, brand, name, bt))
        if len(sample) >= 20:
            break
    for cid, brand, name, bt in sample:
        print(f'  id={cid:>5} [{(brand or "?")[:14]:<14}] -> {bt:<8}  {name[:80]}')


def apply_components(candidates):
    if not candidates:
        print('\n(components) Nothing to apply.')
        return

    groups = defaultdict(list)
    for cid, _, _, bt in candidates:
        groups[bt].append(cid)

    print(f'\nCOMPONENTS APPLY — {len(groups)} grouped update(s) covering '
          f'{len(candidates)} row(s)...')
    BATCH = 200
    total_written = 0
    for bt, ids in groups.items():
        for i in range(0, len(ids), BATCH):
            chunk = ids[i:i + BATCH]
            r = sb.table('components').update({'bullet_type': bt}) \
                .in_('id', chunk).execute()
            n = len(r.data) if r.data else 0
            total_written += n
            print(f'  {bt:<10}  ids[{i}:{i+len(chunk)}]  -> {n} rows updated')
    print(f'\ncomponents done — {total_written} rows written.')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--apply', action='store_true',
                        help='Actually write updates (default is dry-run)')
    parser.add_argument('--table', choices=['listings', 'components', 'both'],
                        default='both',
                        help='Which table(s) to process (default both)')
    args = parser.parse_args()

    listings_cands = []
    components_cands = []

    if args.table in ('listings', 'both'):
        listings_cands = collect_listings_candidates()
        report_listings(listings_cands)

    if args.table in ('components', 'both'):
        components_cands = collect_components_candidates()
        report_components(components_cands)

    if args.apply:
        if listings_cands:
            apply_listings(listings_cands)
        if components_cands:
            apply_components(components_cands)
    else:
        total = len(listings_cands) + len(components_cands)
        print('\n' + '=' * 78)
        print(f'[DRY-RUN] {total} total row(s) would be updated. '
              f'Re-run with --apply to commit.')
        print('=' * 78)


if __name__ == '__main__':
    main()
