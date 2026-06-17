"""Wide validation run driver (#4 Step-3) — fetch -> evaluate -> record.

For each active entry in each named retailer's caliber_paths config:
  load config -> caliber_fetch.fetch_page (real env, dispatched on fetch.mode)
  -> caliber_validate.evaluate (5 gates) -> [--write] log_telemetry (always)
  + write_validation (change-of-record only).

HONESTY BOUNDARY (do not cross): this RECORDS verdicts only. It NEVER flips an
entry['status'] — no candidate->active, no active->parked, no auto-replacement.
A verdict in validation{} is a recorded measurement; a status change is a
separate human-reviewed diff. There is deliberately NO status-write code here.

Default (no --write) is a DRY preview: fetch + evaluate + print, NO disk writes
(no telemetry, no config write-back) — safe to eyeball verdicts first.

    py scripts/run_caliber_validation.py                       # dry, wave-1 four
    py scripts/run_caliber_validation.py --write               # write, wave-1 four
    py scripts/run_caliber_validation.py fenix ventura --write # explicit subset

Authoritative validation is the CI/real-env run; a LOCAL run is fine for
non-walled retailers (the wave-1 proof batch) but a walled retailer (gunbuyer,
sportsmansguide) only validates authoritatively on GHA.
"""
import argparse
import datetime
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import caliber_paths_io  # noqa: E402
import caliber_fetch  # noqa: E402
import caliber_writeback  # noqa: E402
from caliber_validate import evaluate, PER_CALIBER, PARENT  # noqa: E402

# Wave-1 proof batch: one per fetch mode, all healthy/non-walled, all deep-gateable.
WAVE1 = ['fenix', 'ventura', 'rivertown', 'luckygunner']


def entry_url(e):
    return e['path'] + ('?' + e['query'] if e.get('query') else '')


def iter_active(cfg):
    """Yield (caliber, entry_kind, entry) for every status==active entry."""
    for cal, entries in (cfg.get('calibers') or {}).items():
        for e in entries:
            if e.get('status') == 'active':
                yield cal, PER_CALIBER, e
    for e in (cfg.get('parent_paths') or []):
        if e.get('status') == 'active':
            yield None, PARENT, e


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('retailers', nargs='*', help='retailer keys (default: wave-1 four)')
    ap.add_argument('--write', action='store_true',
                    help='write telemetry + change-of-record validation{} (default: dry)')
    ap.add_argument('--json', action='store_true',
                    help='emit ONE JSON array of result rows to stdout (machine-readable; '
                         'suppresses the human table). For the parallel wide-run workers.')
    args = ap.parse_args()
    retailers = args.retailers or WAVE1
    human = not args.json

    now = datetime.datetime.now()
    validated_at = now.strftime('%Y-%m-%d')
    ts = now.isoformat(timespec='seconds')

    if human:
        print(f"{'MODE':5} {'retailer':13} {'cal':8} {'verdict':12} {'st':>4} "
              f"{'n':>4} {'gate%':>6} title wrote  note")
    verdicts = Counter()
    rows = []
    json_rows = []
    for r in retailers:
        path = ROOT / 'caliber_paths' / f'{r}.json'
        cfg = caliber_paths_io.load_config(str(path))
        mode = (cfg.get('fetch') or {}).get('mode', '?')
        for cal, kind, e in iter_active(cfg):
            pq = entry_url(e)
            tf = e.get('title_filter')
            page = caliber_fetch.fetch_page(cfg, pq)
            rec = evaluate(page, cal, kind, title_filter=tf)
            wrote = None
            if args.write:
                caliber_writeback.log_telemetry(rec, retailer=r, caliber=cal,
                                                path_query=pq, timestamp=ts)
                wrote = caliber_writeback.write_validation(
                    str(path), cal, pq, rec, validated_at=validated_at)
            verdicts[rec['verdict']] += 1
            rows.append((r, cal, pq, rec, page))
            json_rows.append({
                'retailer': r, 'mode': mode, 'caliber': cal, 'url': pq,
                'verdict': rec['verdict'], 'status': rec['status'],
                'redirect': rec['redirect'], 'title_match': rec['title_match'],
                'gate_pass_pct': rec['gate_pass_pct'], 'n_products': rec['n_products'],
                'note': rec['note'], 'page_title': page.title,
                'landed_url': page.landed_url, 'n_cards': len(page.card_titles),
                'sample_cards': [t.strip()[:70] for t in page.card_titles[:3]],
                'wrote': wrote,
            })
            if human:
                wmark = ('W' if wrote else '.') if args.write else '-'
                print(f"{mode[:5]:5} {r:13} {str(cal):8} {rec['verdict']:12} "
                      f"{str(rec['status']):>4} {str(rec['n_products']):>4} "
                      f"{str(rec['gate_pass_pct']):>6} {str(rec['title_match'])[:5]:5} "
                      f"{wmark:5} {rec['note'][:64]}")

    if human:
        print("\n--- plausibility detail (page <title> + sample card titles) ---")
        for r, cal, pq, rec, page in rows:
            samples = ' | '.join(t.strip()[:48] for t in page.card_titles[:2])
            print(f"  {r}/{cal} {pq}")
            print(f"      title={page.title[:80]!r}")
            print(f"      cards[{len(page.card_titles)}]: {samples}")
        print(f"\nverdicts: {dict(verdicts)}  ({sum(verdicts.values())} entries)")
    else:
        print(json.dumps(json_rows, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    sys.exit(main())
