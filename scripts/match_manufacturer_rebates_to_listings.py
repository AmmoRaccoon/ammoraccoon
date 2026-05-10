"""match_manufacturer_rebates_to_listings.py — populate manufacturer_rebate_listing_matches.

For every active rebate in manufacturer_rebates, walks its eligible-product
rows, finds listings where:
  manufacturer = rebate.brand AND product_url ILIKE '%<keyword>%'
…and records the per-listing rebate amount in manufacturer_rebate_listing_matches.
Frontend reads from that table to render rebate-eligible badges; the scraper
just writes the rebates and tiers.

Active rebate, per the schema doc:
    valid_through       >= current_date
    AND submit_by       >= current_date
    AND last_seen_active_at > now() - interval '48 hours'

Keyword derivation: lowercased product_line with spaces -> hyphens. If the row
has match_pattern set, that wins (operator override for stubborn cases).

If a listing matches multiple eligible products in the same rebate, the
HIGHEST amount wins (consumer-friendly — surface the best deal available).

Stale-match handling: every active-rebate run deletes the rebate's existing
matches and re-inserts the current set. That keeps the cache consistent with
the current rebate definition + listing inventory. Only the active rebate's
matches are touched per run; expired rebates' historical matches remain
until separately purged.

Required env:
  SUPABASE_URL, SUPABASE_KEY

Usage:
  python scripts/match_manufacturer_rebates_to_listings.py --dry-run
  python scripts/match_manufacturer_rebates_to_listings.py
"""

import argparse
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_KEY']

ACTIVE_FRESHNESS = timedelta(hours=48)
PAGE = 1000

# Caliber sets per firearm_type. Listings without a caliber_normalized
# value won't match any non-NULL gate — defensible: if we can't classify
# the listing, we don't claim a rebate covers it.
HANDGUN_CALIBERS = ('9mm', '380acp', '40sw', '38spl', '357mag')
RIFLE_CALIBERS   = ('223-556', '308win', '762x39', '300blk')
RIMFIRE_CALIBERS = ('22lr',)
SHOTSHELL_BULLET_TYPES = ('Slug', 'Buckshot', 'Birdshot')


def derive_keyword(product_line: str, match_pattern: str | None) -> str:
    if match_pattern:
        return match_pattern
    return product_line.lower().replace(' ', '-')


def fetch_active_rebates(sb):
    today = datetime.now(timezone.utc).date().isoformat()
    cutoff = (datetime.now(timezone.utc) - ACTIVE_FRESHNESS).isoformat()
    rows = (
        sb.table('manufacturer_rebates')
        .select('id,external_id,source,brand,title,amount_max_per_unit,'
                'firearm_type,valid_through,submit_by,last_seen_active_at')
        .gte('valid_through', today)
        .gte('submit_by', today)
        .gte('last_seen_active_at', cutoff)
        .execute()
        .data
    )
    return rows


def fetch_eligible_products(sb, rebate_id):
    return (
        sb.table('manufacturer_rebate_eligible_products')
        .select('product_line,amount_override,match_pattern')
        .eq('rebate_id', rebate_id)
        .execute()
        .data
    )


def find_matching_listings(sb, brand, keyword, firearm_type):
    """Listings where manufacturer = brand AND product_url ILIKE '%keyword%'
    AND (firearm_type gate, if rebate.firearm_type is set).

    The firearm_type gate is what stops a turkey-shotshell rebate from
    claiming Winchester Super-X 9mm listings are eligible — see the
    2026-05-09 rebate-id-5 audit and migration 012.
    """
    matches = []
    start = 0
    while True:
        q = (
            sb.table('listings')
            .select('id,product_url,manufacturer')
            .eq('manufacturer', brand)
            .ilike('product_url', f'%{keyword}%')
        )
        if firearm_type == 'shotshell':
            q = q.or_(
                'caliber_normalized.like.12ga%,'
                'caliber_normalized.like.16ga%,'
                'caliber_normalized.like.20ga%,'
                f'bullet_type.in.({",".join(SHOTSHELL_BULLET_TYPES)})'
            )
        elif firearm_type == 'handgun':
            q = q.in_('caliber_normalized', list(HANDGUN_CALIBERS))
        elif firearm_type == 'rifle':
            q = q.in_('caliber_normalized', list(RIFLE_CALIBERS))
        elif firearm_type == 'rimfire':
            q = q.in_('caliber_normalized', list(RIMFIRE_CALIBERS))
        # firearm_type IS NULL -> no extra gate (backward-compat)

        batch = q.range(start, start + PAGE - 1).execute().data
        if not batch:
            break
        matches.extend(batch)
        if len(batch) < PAGE:
            break
        start += PAGE
    return matches


def compute_matches_for_rebate(sb, rebate):
    """Return list of (listing_id, amount, reason) — best amount wins per listing."""
    eligible = fetch_eligible_products(sb, rebate['id'])
    if not eligible:
        return []

    # listing_id -> (best_amount, best_reason)
    best = {}
    fallback_amount = rebate.get('amount_max_per_unit')
    firearm_type = rebate.get('firearm_type')

    for row in eligible:
        product_line = row['product_line']
        keyword = derive_keyword(product_line, row.get('match_pattern'))
        amount = row.get('amount_override')
        if amount is None:
            amount = fallback_amount
        if amount is None:
            # No tier amount and no rebate-level fallback — skip; we won't
            # invent an amount we can't source.
            continue

        listings = find_matching_listings(sb, rebate['brand'], keyword, firearm_type)
        reason = f'url_contains:{keyword}' + (
            f';firearm_type:{firearm_type}' if firearm_type else ''
        )
        for l in listings:
            current = best.get(l['id'])
            if current is None or float(amount) > current[0]:
                best[l['id']] = (float(amount), reason)

    return [(lid, amt, reason) for lid, (amt, reason) in best.items()]


def write_matches(sb, rebate_id, matches):
    """Delete old matches for this rebate, insert new set. Single transaction
    isn't available via supabase-py, so worst-case window is the gap between
    delete and insert. Only this rebate's matches are touched."""
    sb.table('manufacturer_rebate_listing_matches').delete().eq('rebate_id', rebate_id).execute()
    if not matches:
        return 0
    rows = [
        {
            'rebate_id': rebate_id,
            'listing_id': lid,
            'matched_amount': amt,
            'match_reason': reason,
        }
        for lid, amt, reason in matches
    ]
    # supabase-py rejects very large single inserts; chunk to be safe.
    inserted = 0
    CHUNK = 500
    for i in range(0, len(rows), CHUNK):
        sb.table('manufacturer_rebate_listing_matches').insert(rows[i:i + CHUNK]).execute()
        inserted += min(CHUNK, len(rows) - i)
    return inserted


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true',
                        help='Print match counts; no DB writes.')
    args = parser.parse_args()

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    rebates = fetch_active_rebates(sb)
    print(f'Active rebates: {len(rebates)}')
    if not rebates:
        print('Nothing to match.')
        return 0

    grand_total = 0
    for r in rebates:
        print(f'\n[{r["external_id"]}] {r["source"]:<10} {r["brand"]:<12} {r["title"]!r}')
        eligible = fetch_eligible_products(sb, r['id'])
        print(f'  eligible products: {len(eligible)}')

        matches = compute_matches_for_rebate(sb, r)
        print(f'  matched listings: {len(matches)}')

        if matches:
            # Per-amount breakdown for the dry-run report.
            by_amount = defaultdict(int)
            for _, amt, _ in matches:
                by_amount[amt] += 1
            for amt in sorted(by_amount.keys(), reverse=True):
                print(f'    ${amt:.2f}: {by_amount[amt]} listings')

            # Per-keyword breakdown — useful to see which product lines drive matches.
            by_reason = defaultdict(int)
            for _, _, reason in matches:
                by_reason[reason] += 1
            print('  by keyword (best-amount per listing):')
            for reason, n in sorted(by_reason.items(), key=lambda x: -x[1]):
                print(f'    {n:>4}  {reason}')

        # write_matches always runs in live mode, even when matches is
        # empty — the function's contract is "delete this rebate's
        # existing matches, then insert the current set." Skipping it
        # for empty sets used to leave stale matches behind whenever a
        # rebate's matches went from non-empty to empty (the exact
        # symptom that left 141 false matches on rebate id=5 stranded
        # after the 2026-05-09 firearm_type-gating fix made the new
        # match set empty).
        if not args.dry_run:
            written = write_matches(sb, r['id'], matches)
            print(f'  wrote {written} rows to manufacturer_rebate_listing_matches '
                  f'(stale rows for this rebate were cleared first)')
            grand_total += written
        else:
            grand_total += len(matches)

    mode = 'DRY RUN' if args.dry_run else 'LIVE'
    print(f'\nDone ({mode}). {grand_total} listing match(es) '
          f'{"would be " if args.dry_run else ""}written.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
