"""Synthesize wave-2 DRY results: per-retailer summary + every non-PASS entry
grouped by likely cause, so FAILs can be cause-proven before any write.
Read-only. Run: py scripts/_wave2_synth.py"""
import json
import re
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DRY = ROOT / '_validation_telemetry' / 'wave2dry'


def load(f):
    b = f.read_bytes()
    try:
        return json.loads(b.decode('utf-8'))
    except UnicodeDecodeError:
        return json.loads(b.decode('cp1252'))   # Windows stdout-redirect encoding


rows, missing = [], []
for f in sorted(DRY.glob('*.json')):
    try:
        rows.extend(load(f))
    except Exception as e:
        missing.append(f'{f.name}: {e}')

byret = {}
for r in rows:
    byret.setdefault(r['retailer'], []).append(r)


def bucket(r):
    if r['verdict'] != 'FAIL':
        return None
    if r['status'] is None:
        return 'UNREACHABLE'
    if isinstance(r['status'], int) and r['status'] >= 400:
        return f"HTTP_{r['status']}"
    if r['redirect']:
        return 'REDIRECT'
    if r['n_products'] == 0:
        return 'ZERO_PRODUCTS'
    return 'LOW_GATE_WITH_CARDS'   # cards present but gate<30% — phrasing/cross-list/harness-gap?


print(f"=== TOTALS: {dict(Counter(r['verdict'] for r in rows))}  "
      f"({len(rows)} entries, {len(byret)} retailers) ===")
if missing:
    print("UNREADABLE:", missing)

print("\n=== per-retailer (P / NR / F) ===")
for ret in sorted(byret):
    c = Counter(r['verdict'] for r in byret[ret])
    fb = Counter(bucket(r) for r in byret[ret] if bucket(r))
    flag = ''
    if c.get('PASS', 0) == 0:
        flag = '  <-- NO PASS'
    print(f"  {ret:20} P={c.get('PASS',0):>2} NR={c.get('NEEDS_REVIEW',0):>2} "
          f"F={c.get('FAIL',0):>2}  {dict(fb) if fb else ''}{flag}")

print("\n=== FAILs grouped by cause-bucket ===")
fails = [r for r in rows if r['verdict'] == 'FAIL']
groups = {}
for r in fails:
    groups.setdefault(bucket(r), []).append(r)
for b in sorted(groups):
    print(f"\n--- {b}  ({len(groups[b])}) ---")
    for r in groups[b]:
        cal = str(r['caliber'])
        sample = (r['sample_cards'][0][:55] if r['sample_cards'] else '')
        print(f"  {r['retailer']:20}/{cal:8} gate={str(r['gate_pass_pct']):>5} "
              f"n={str(r['n_products']):>3} tm={str(r['title_match'])[:5]:5}")
        print(f"      landed: {r['landed_url']}")
        if sample:
            print(f"      card[{r['n_cards']}]: {sample}")

print("\n=== NEEDS_REVIEW (compact) ===")
for r in [r for r in rows if r['verdict'] == 'NEEDS_REVIEW']:
    cal = str(r['caliber'])
    sample = (r['sample_cards'][0][:38] if r['sample_cards'] else '')
    print(f"  {r['retailer']:20}/{cal:8} gate={str(r['gate_pass_pct']):>5} "
          f"n={str(r['n_products']):>3} tm={str(r['title_match'])[:5]:5} | "
          f"{r['note'][:42]:42} | {sample}")
