"""scraper_recheck.py — shared listing stock re-check job.

Re-fetches the product_url of listings we already track and updates their real
stock status directly, independent of the category crawl. Fixes the "frozen
in_stock=true" problem (web repo investigation 2026-06-05/06): category-crawl
scrapers never revisit a listing once it rotates off the category page, so an
OOS or delisted item stays in_stock=true in the DB forever.

ONE shared job, not 34 per-scraper passes. The default stock extractor is the
shared schema.org JSON-LD reader scraper_lib.recheck_product_stock.

Scope (MVP): the stale tail — listings with in_stock=true AND last_updated
older than --max-age-days (default 2). ~2,000 rows, small per retailer.

Safety model (safer policy, 2026-06-06):
  * Flip a listing to OOS ONLY on an explicit schema.org OutOfStock / SoldOut /
    Discontinued signal OR a 404/410 dead page. A 200 page with NO parseable
    Product JSON-LD is UNDETERMINED (leave as-is) for EVERY retailer — a missing
    schema block is not a reliable OOS signal (proven on Brownells in-stock PDPs
    whose availability lives outside JSON-LD). Anti-bot 403/429, 5xx and network
    errors are undetermined too. Never wrongly flip an in-stock listing.
  * EXCLUDE_SLUGS are skipped entirely (not fetched) this round:
      - ammocom        : product_url is a category#anchor, not a real PDP, so
                         its JSON-LD belongs to a DIFFERENT product (unsafe)
      - outdoorlimited : no schema (would only ever return undetermined)
      - target-sports  : availability via microdata, not Product JSON-LD
      - buds           : anti-bot 403 on plain HTTP
      - parked/walled  : aeammo, ammodeport, wideners, midwayusa, academy,
                         bereli, bulkmunitions
    All need a bespoke adapter later.

Retailers run concurrently (independent hosts) with a per-retailer politeness
delay between requests.

DRY-RUN BY DEFAULT: writes nothing. Pass --commit to write.
"""

import argparse
import os
import sys
import time
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from supabase import create_client

from scraper_lib import (
    recheck_product_stock, RECHECK_USER_AGENT, now_iso, sanity_check_ppr,
)

load_dotenv()

# Retailers to leave untouched this round (need a bespoke adapter later).
EXCLUDE_SLUGS = {
    'outdoorlimited', 'buds', 'ammocom',
    # ammocom MUST stay excluded: its product_url is a category#anchor, not a
    # real PDP, so a fetch returns a category page whose JSON-LD belongs to some
    # OTHER product — acting on it could flip/confirm the wrong listing.
    # outdoorlimited (no schema) / target-sports (availability via microdata,
    # not Product JSON-LD) / buds (anti-bot 403) would only ever return
    # undetermined under the safer policy; excluding them avoids wasted fetches
    # and flags them for a bespoke adapter later.
    'target-sports',
    'aeammo', 'ammodeport', 'wideners', 'midwayusa',
    'academy', 'bereli', 'bulkmunitions',
}

DEFAULT_DELAY_SEC = 2.0
PAGE = 1000


def fetch_stale_listings(supabase, cutoff_iso):
    """All in-stock listings whose last_updated is older than the cutoff."""
    rows = []
    start = 0
    while True:
        resp = (supabase.table('listings')
                .select('id, retailer_id, product_url, in_stock, last_updated, '
                        'base_price, price_per_round, total_rounds, '
                        'caliber_normalized')
                .eq('in_stock', True)
                .lt('last_updated', cutoff_iso)
                .order('retailer_id')
                .range(start, start + PAGE - 1)
                .execute())
        batch = resp.data or []
        rows.extend(batch)
        if len(batch) < PAGE:
            break
        start += PAGE
    return rows


def process_retailer(slug, listings, delay, timeout):
    """Re-check one retailer's listings sequentially (own session + politeness
    delay). Returns a list of result records."""
    out = []
    session = requests.Session()
    for i, lst in enumerate(listings):
        if i > 0:
            time.sleep(delay)
        res = recheck_product_stock(lst['product_url'],
                                    timeout=timeout, session=session)
        out.append(_classify(slug, lst, res))
    session.close()
    return out


def _classify(slug, lst, res):
    rec = {
        'listing_id': lst['id'], 'slug': slug, 'url': lst['product_url'],
        'stored_in_stock': lst['in_stock'], 'last_updated': lst['last_updated'],
        # carry last-known price fields for the commit path (carry-forward
        # on OOS flips, ppr recompute on in-stock refresh)
        'base_price': lst.get('base_price'),
        'price_per_round': lst.get('price_per_round'),
        'total_rounds': lst.get('total_rounds'),
        # carried for the commit-path re-validation (per-caliber floor/ceiling)
        'caliber_normalized': lst.get('caliber_normalized'),
        'status': res['status'], 'reason': res['reason'],
        'price': res['price'], 'determinable': res['determinable'],
        'new_in_stock': res['in_stock'],
    }
    if not res['determinable']:
        rec['category'] = 'undetermined'
    elif res['in_stock'] is False:
        rec['category'] = 'flip_oos'
    else:
        rec['category'] = 'confirm_instock'
    return rec


def _excluded_record(slug, lst):
    return {
        'listing_id': lst['id'], 'slug': slug, 'url': lst['product_url'],
        'stored_in_stock': lst['in_stock'], 'last_updated': lst['last_updated'],
        'status': None, 'reason': 'excluded-retailer', 'price': None,
        'determinable': False, 'new_in_stock': None, 'category': 'excluded',
    }


def commit_change(supabase, rec, now):
    """LIVE write for one confirmed/flipped listing. Only called with --commit.

    FLIP -> OOS: set the stock flag only (do NOT rewrite the price). The
      price_history row carries the last-known price forward so /history marks
      the OOS transition at a sane price.
    CONFIRM in-stock: refresh last_seen_in_stock + last_updated, and refresh the
      price when the page gave us a fresh one.

    Returns 'written' | 'flag_only' | 'skipped_reval' so the caller can report.

    PRICE RE-VALIDATION (price-honesty audit, 2026-06-19): the price we are about
    to vouch for is re-checked through the SAME guard the 39 scrapers use
    (sanity_check_ppr — arithmetic-consistency + per-caliber floor/ceiling).
    Recheck previously bumped last_updated and reused the stored price WITHOUT
    re-validating, keeping out-of-band / self-contradictory rows perpetually
    "fresh" (the second masking bug). Honest-blank on failure — we never guess
    which field is wrong:
      * CONFIRM in-stock fails -> skip the bump entirely (don't touch
        last_updated, don't write price_history). The 6h freshness gate ages the
        row out of every customer surface honestly.
      * FLIP -> OOS fails -> the OOS flip is still honest (the item is gone), so
        flip the stock flag but skip the price_history insert (don't record a
        bad price).
    """
    new_in = rec['new_in_stock']
    update = {
        'in_stock': new_in,
        'stock_level': 'In Stock' if new_in else 'Out of Stock',
        'last_updated': now,
    }
    # Default history price = last-known (carry-forward).
    ph_price = rec.get('base_price')
    ph_ppr = rec.get('price_per_round')
    if new_in:
        update['last_seen_in_stock'] = now
        # In-stock refresh: adopt a fresh live price when we parsed one.
        if rec.get('price') is not None and rec.get('total_rounds'):
            ph_price = round(rec['price'], 2)
            ph_ppr = round(rec['price'] / rec['total_rounds'], 4)
            update['base_price'] = ph_price
            update['price_per_round'] = ph_ppr

    # Re-validate the price we're about to vouch for. caliber_normalized is
    # carried from the stale-listings query so the per-caliber floor/ceiling
    # applies (without it the .38spl / .357 floor cases slip past DEFAULT_FLOOR).
    price_ok = sanity_check_ppr(
        ph_ppr, ph_price, rec.get('total_rounds'),
        context=f"recheck {rec.get('slug', '')}",
        caliber=rec.get('caliber_normalized'),
    )
    if not price_ok:
        if new_in:
            # Untrustworthy price on a confirm-in-stock: do NOT refresh. Skip the
            # bump so the row stales out via the 6h freshness gate.
            print(f"    [reval-skip] #{rec['listing_id']} [{rec.get('slug','')}] "
                  f"confirm-in-stock FAILED re-validation "
                  f"(ppr={ph_ppr}, price={ph_price}, rounds={rec.get('total_rounds')}, "
                  f"cal={rec.get('caliber_normalized')}) -> skip bump, let it stale")
            return 'skipped_reval'
        # Flip -> OOS with a bad price: flag-only, no price_history row.
        print(f"    [reval-flag-only] #{rec['listing_id']} [{rec.get('slug','')}] "
              f"flip->OOS with untrustworthy price -> flip stock flag, skip price_history")
        supabase.table('listings').update(update).eq('id', rec['listing_id']).execute()
        return 'flag_only'

    supabase.table('listings').update(update).eq('id', rec['listing_id']).execute()
    # Record the observation. Skip only if we have no price at all (avoids a
    # null-price history row); the listing flag update above still lands.
    if ph_price is not None and ph_ppr is not None:
        supabase.table('price_history').insert({
            'listing_id': rec['listing_id'],
            'price': ph_price,
            'price_per_round': ph_ppr,
            'in_stock': new_in,
            'recorded_at': now,
        }, returning='minimal').execute()
    return 'written'


def main():
    ap = argparse.ArgumentParser(description='Shared listing stock re-check job.')
    ap.add_argument('--commit', action='store_true',
                    help='Write changes to the DB. Default is a dry run.')
    ap.add_argument('--max-age-days', type=float, default=2.0,
                    help='Re-check in-stock listings older than this (default 2).')
    ap.add_argument('--delay', type=float, default=DEFAULT_DELAY_SEC,
                    help=f'Per-retailer politeness delay (default {DEFAULT_DELAY_SEC}s).')
    ap.add_argument('--timeout', type=float, default=20.0)
    ap.add_argument('--workers', type=int, default=24)
    ap.add_argument('--limit-per-retailer', type=int, default=None,
                    help='Cap listings per retailer (testing only).')
    args = ap.parse_args()

    supabase = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])
    cutoff = datetime.now(timezone.utc).timestamp() - args.max_age_days * 86400
    cutoff_iso = datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()

    mode = 'LIVE COMMIT' if args.commit else 'DRY RUN (no DB writes)'
    print(f'[{datetime.now().isoformat()}] scraper_recheck — {mode}')
    print(f'  cutoff: in_stock=true AND last_updated < {cutoff_iso}')

    slug_by_id = {r['id']: r['slug']
                  for r in (supabase.table('retailers').select('id, slug').execute().data or [])}
    stale = fetch_stale_listings(supabase, cutoff_iso)
    print(f'  stale in-stock listings: {len(stale)}')

    buckets = defaultdict(list)
    for lst in stale:
        buckets[slug_by_id.get(lst['retailer_id'], f'id:{lst["retailer_id"]}')].append(lst)

    records = []
    todo = {}
    for slug, items in buckets.items():
        if args.limit_per_retailer:
            items = items[:args.limit_per_retailer]
        if slug in EXCLUDE_SLUGS:
            records.extend(_excluded_record(slug, lst) for lst in items)
        else:
            todo[slug] = items

    process_n = sum(len(v) for v in todo.values())
    print(f'  retailers to process: {len(todo)} ({process_n} listings) | '
          f'excluded-untouched: {len(stale) - process_n}')
    print(f'  concurrency: {min(args.workers, len(todo))} workers | delay {args.delay}s/retailer\n')

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=min(args.workers, max(1, len(todo)))) as ex:
        futs = {ex.submit(process_retailer, slug, items, args.delay, args.timeout): slug
                for slug, items in todo.items()}
        done = 0
        for fut in as_completed(futs):
            records.extend(fut.result())
            done += 1
            print(f'    [{done}/{len(futs)}] finished {futs[fut]}')
    elapsed = time.time() - t0

    # ---- report ----
    by_cat = Counter(r['category'] for r in records)
    print('\n================ DRY-RUN RESULTS ================' if not args.commit
          else '\n================ COMMIT RESULTS ================')
    print(f'  elapsed: {elapsed/60:.1f} min | total listings: {len(records)}')
    print(f'  FLIP -> OOS            : {by_cat.get("flip_oos", 0)}')
    print(f'  CONFIRM in-stock       : {by_cat.get("confirm_instock", 0)}')
    print(f'  CANNOT determine       : {by_cat.get("undetermined", 0)}')
    print(f'  EXCLUDED (untouched)   : {by_cat.get("excluded", 0)}')

    print('\n  per-retailer (flip / confirm / undetermined / excluded):')
    rslugs = sorted({r['slug'] for r in records})
    for s in sorted(rslugs, key=lambda s: -sum(1 for r in records if r['slug'] == s and r['category'] == 'flip_oos')):
        rs = [r for r in records if r['slug'] == s]
        c = Counter(r['category'] for r in rs)
        print(f'    {s:<20} n={len(rs):<4} flip={c.get("flip_oos",0):<4} '
              f'confirm={c.get("confirm_instock",0):<4} undet={c.get("undetermined",0):<4} excl={c.get("excluded",0)}')

    print('\n  CANNOT-determine reasons:')
    for reason, n in Counter(r['reason'] for r in records if r['category'] == 'undetermined').most_common(12):
        print(f'    {n:<5} {reason}')

    flips = [r for r in records if r['category'] == 'flip_oos']
    print(f'\n  ===== 10 SPOT-CHECK FLIPS (stored in_stock=true -> would set OOS) =====')
    for r in flips[:10]:
        print(f'    #{r["listing_id"]} [{r["slug"]}] stored=in_stock'
              f' | live: HTTP {r["status"]} {r["reason"]}'
              f' | price={r["price"]} -> DECISION: set OUT OF STOCK')
        print(f'        {r["url"]}')

    print('\n  ===== watch listings (#1438 must FLIP->OOS; #210010 must be LEFT AS-IS) =====')
    for wid in (1438, 210010):
        rec = next((r for r in records if r['listing_id'] == wid), None)
        if rec:
            print(f'    #{wid} [{rec["slug"]}] stored in_stock={rec["stored_in_stock"]} | live HTTP '
                  f'{rec["status"]} {rec["reason"]} | category={rec["category"]} | new_in_stock={rec["new_in_stock"]}')
            continue
        one = (supabase.table('listings')
               .select('id, retailer_id, product_url, in_stock')
               .eq('id', wid).maybe_single().execute().data)
        if not one:
            print(f'    #{wid}: not found')
            continue
        slug = slug_by_id.get(one['retailer_id'])
        res = recheck_product_stock(one['product_url'], timeout=args.timeout)
        if res['determinable'] and res['in_stock'] is False:
            decision = 'FLIP -> OOS'
        elif res['determinable'] and res['in_stock'] is True:
            decision = 'CONFIRM in-stock'
        else:
            decision = 'LEAVE AS-IS (undetermined)'
        print(f'    #{wid} [{slug}] stored in_stock={one["in_stock"]} | live HTTP {res["status"]} '
              f'{res["reason"]} | (not in stale set) -> {decision}')

    if not args.commit:
        print('\n  DRY RUN — no DB writes performed.')
        return 0

    # LIVE path (only with --commit).
    now = now_iso()
    written = 0
    flag_only = 0
    skipped_reval = 0
    for r in records:
        if r['category'] in ('flip_oos', 'confirm_instock'):
            try:
                outcome = commit_change(supabase, r, now)
                if outcome == 'skipped_reval':
                    skipped_reval += 1
                elif outcome == 'flag_only':
                    flag_only += 1
                else:
                    written += 1
            except Exception as e:
                print(f'    [DB-ERR] #{r["listing_id"]}: {e}')
    print(f'\n  wrote {written} listing updates (+ price_history rows).')
    print(f'  flag-only (OOS flip, bad price withheld): {flag_only}')
    print(f'  skipped re-validation (confirm-in-stock left to stale): {skipped_reval}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
