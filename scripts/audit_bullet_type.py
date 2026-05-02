"""Read-only diagnostic of bullet_type quality across the listings table.

No mutations. Pulls every row in `listings`, joins to retailers for
display labels, and prints four sections:

1. In-stock NULL count, per retailer.
2. Distinct bullet_type values + counts across the whole table (catches
   case/spelling drift between scrapers).
3. Rows where the product_url slug contains an obvious bullet-type token
   (fmj, jhp, hp, sp, etc.) but bullet_type is NULL — per retailer with
   sample URLs.
4. Per-retailer summary table.

The product_url is used as a proxy for the product title since the
listings table doesn't store the original title.
"""
import os
import re
from collections import defaultdict
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# Canonical bullet-type tokens to look for in the slug. Each tuple is
# (regex, label_for_report). Order matters — longer / more-specific
# patterns first so "vmax" doesn't get pre-eaten by a future "v" alias.
# All patterns are matched against the lowercased product_url path with
# `\b` boundaries; `-`, `_`, `.`, `/` all act as boundaries in regex so
# slug tokens like `-fmj-` and path segments like `/fmj-bt/` both hit.
SLUG_PATTERNS = [
    (re.compile(r'\bfullmetaljacket\b|\bfull[-_]metal[-_]jacket\b'), 'FMJ'),
    (re.compile(r'\bhollowpoint\b|\bhollow[-_]point\b'), 'HP-family'),
    (re.compile(r'\bsoftpoint\b|\bsoft[-_]point\b'), 'SP'),
    (re.compile(r'\bfmjbt\b|\bfmj[-_]bt\b'), 'FMJ'),
    (re.compile(r'\bbthp\b|\bhpbt\b'), 'HP-family'),
    (re.compile(r'\bjhp\b'), 'JHP'),
    (re.compile(r'\bfmj\b'), 'FMJ'),
    (re.compile(r'\btmj\b'), 'TMJ'),
    (re.compile(r'\botm\b|\botsm\b'), 'OTM'),
    (re.compile(r'\bvmax\b|\bv[-_]max\b'), 'HP-family'),
    (re.compile(r'\bamax\b|\ba[-_]max\b'), 'HP-family'),
    (re.compile(r'\beldx\b|\beld[-_]x\b|\beld[-_]m\b'), 'HP-family'),
    (re.compile(r'\bsjhp\b'), 'JHP'),
    (re.compile(r'\bsjsp\b'), 'SP'),
    (re.compile(r'\brnfp\b'), 'FP'),
    (re.compile(r'\blrn\b'), 'FP'),
    (re.compile(r'\b(?:hp)\b'), 'HP'),
    (re.compile(r'\b(?:sp)\b'), 'SP'),
    (re.compile(r'\b(?:fp)\b'), 'FP'),
]


def fetch_all(table, select):
    """Page through a Supabase table since the default limit is 1000."""
    rows = []
    offset = 0
    while True:
        r = sb.table(table).select(select).range(offset, offset + 999).execute()
        if not r.data:
            break
        rows.extend(r.data)
        if len(r.data) < 1000:
            break
        offset += 1000
    return rows


def detect_slug_type(url):
    """Return the first bullet-type label whose regex hits the URL slug, or None."""
    if not url:
        return None
    # Restrict to the path portion so domain words can't false-match
    # (e.g. nothing currently risky but cheap insurance).
    path = url.split('://', 1)[-1].split('/', 1)[-1].lower()
    for pat, label in SLUG_PATTERNS:
        if pat.search(path):
            return label
    return None


def main():
    print("Fetching retailers and listings (this may take a few seconds)...")
    retailers = {r['id']: r for r in fetch_all('retailers', 'id,slug,name,is_active')}
    listings = fetch_all('listings', 'id,retailer_id,product_url,bullet_type,in_stock')

    active_ids = {rid for rid, r in retailers.items() if r.get('is_active')}
    listings = [L for L in listings if L['retailer_id'] in active_ids]

    print(f"Active retailers: {len(active_ids)}")
    print(f"Listings on active retailers: {len(listings)}")
    print(f"  in-stock: {sum(1 for L in listings if L['in_stock'])}")
    print(f"  out-of-stock: {sum(1 for L in listings if not L['in_stock'])}")

    # --- Section 2: distinct bullet_type values across the whole table.
    print("\n" + "=" * 78)
    print("2. DISTINCT bullet_type VALUES (all listings on active retailers)")
    print("=" * 78)
    val_counts = defaultdict(lambda: [0, 0])  # value -> [total, in_stock]
    for L in listings:
        v = L['bullet_type']
        val_counts[v][0] += 1
        if L['in_stock']:
            val_counts[v][1] += 1
    # Sort by total count desc, NULL last.
    sorted_vals = sorted(val_counts.items(),
                         key=lambda kv: (kv[0] is None, -kv[1][0]))
    print(f"  {'value':<25}  {'total':>8}  {'in_stock':>8}")
    print(f"  {'-' * 25}  {'-' * 8}  {'-' * 8}")
    for v, (tot, ins) in sorted_vals:
        label = 'NULL' if v is None else repr(v)
        print(f"  {label:<25}  {tot:>8}  {ins:>8}")

    # --- Section 1 + 4: per-retailer null count + summary table.
    per_retailer = defaultdict(lambda: {
        'total': 0, 'in_stock': 0, 'null_total': 0, 'null_in_stock': 0,
        'slug_says_but_null_total': 0, 'slug_says_but_null_in_stock': 0,
        'slug_say_samples': [],
        'distinct_values': defaultdict(int),
    })
    for L in listings:
        rid = L['retailer_id']
        bt = L['bullet_type']
        ins = L['in_stock']
        bucket = per_retailer[rid]
        bucket['total'] += 1
        if ins:
            bucket['in_stock'] += 1
        if bt is None:
            bucket['null_total'] += 1
            if ins:
                bucket['null_in_stock'] += 1
            slug_label = detect_slug_type(L.get('product_url'))
            if slug_label:
                bucket['slug_says_but_null_total'] += 1
                if ins:
                    bucket['slug_says_but_null_in_stock'] += 1
                if len(bucket['slug_say_samples']) < 5:
                    bucket['slug_say_samples'].append(
                        (slug_label, L.get('product_url') or '<no url>')
                    )
        else:
            bucket['distinct_values'][bt] += 1

    print("\n" + "=" * 78)
    print("1 + 4. PER-RETAILER bullet_type COVERAGE (active retailers only)")
    print("=" * 78)
    print(f"  {'id':>3}  {'slug':<22}  {'in-stock':>8}  {'null-IS':>8}  {'null%-IS':>8}  "
          f"{'slug-says-but-null-IS':>22}")
    print(f"  {'-'*3}  {'-'*22}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*22}")
    for rid in sorted(per_retailer.keys()):
        b = per_retailer[rid]
        slug = retailers[rid]['slug']
        if b['in_stock'] == 0:
            null_pct = '-'
        else:
            null_pct = f"{(b['null_in_stock'] / b['in_stock']) * 100:5.1f}%"
        print(f"  {rid:>3}  {slug:<22}  {b['in_stock']:>8}  {b['null_in_stock']:>8}  "
              f"{null_pct:>8}  {b['slug_says_but_null_in_stock']:>22}")

    # --- Section 3: title-says-X-but-NULL detail per retailer.
    print("\n" + "=" * 78)
    print("3. SLUG-SAYS-BULLET-TYPE-BUT-NULL — per retailer detail")
    print("    (product_url slug is the proxy; listings has no title column)")
    print("=" * 78)
    for rid in sorted(per_retailer.keys()):
        b = per_retailer[rid]
        if b['slug_says_but_null_total'] == 0:
            continue
        slug = retailers[rid]['slug']
        print(f"\n  [id={rid:>3} {slug}]  "
              f"slug-says: {b['slug_says_but_null_total']} total / "
              f"{b['slug_says_but_null_in_stock']} in-stock")
        for label, url in b['slug_say_samples']:
            print(f"      [{label:<10}] {url}")

    # --- Section 2b: distinct values per retailer, just for retailers
    # whose set of values differs from the canonical FMJ/JHP/HP/OTM/TMJ/SP/FP.
    print("\n" + "=" * 78)
    print("2b. PER-RETAILER DISTINCT bullet_type SETS (non-NULL only)")
    print("    Spot near-duplicates / case drift across scrapers.")
    print("=" * 78)
    canonical = {'FMJ', 'JHP', 'HP', 'OTM', 'TMJ', 'SP', 'FP'}
    for rid in sorted(per_retailer.keys()):
        b = per_retailer[rid]
        slug = retailers[rid]['slug']
        vals = dict(b['distinct_values'])
        unusual = [v for v in vals if v not in canonical]
        if vals:
            tagged = (' [HAS UNUSUAL: ' + ', '.join(repr(v) for v in unusual) + ']') if unusual else ''
            pretty = ', '.join(f'{v}={n}' for v, n in
                               sorted(vals.items(), key=lambda kv: -kv[1]))
            print(f"  id={rid:>3} {slug:<22}  {pretty}{tagged}")
        else:
            print(f"  id={rid:>3} {slug:<22}  (no non-NULL bullet_type values)")


if __name__ == '__main__':
    main()
