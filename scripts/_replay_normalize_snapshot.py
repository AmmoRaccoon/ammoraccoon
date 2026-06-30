"""Before/after corpus replay for normalize_caliber — proves a detector
change is ADDITIVE-ONLY.

(_replay_normalize_caliber.py is the Phase-A hand-vs-gen check, now
degenerate since scraper_lib re-exports normalize_caliber_gen. This is
the snapshot/compare tool for validating an INTENTIONAL registry edit.)

Imports the SAME function the scrapers run: caliber_registry_gen.
normalize_caliber_gen.

Usage:
  py -3 scripts/_replay_normalize_snapshot.py --snapshot <out.json> [--corpus <path>]
  py -3 scripts/_replay_normalize_snapshot.py --compare  <before.json> [--show N]

Compare asserts: every previously-non-None verdict is UNCHANGED, and the
only deltas are None -> ALLOWED_NEW. Anything else => FAIL (exit 1).
"""
import argparse
import json
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from caliber_registry_gen import normalize_caliber_gen as normalize_caliber

DEFAULT_CORPUS = r"C:\Users\jonca\OneDrive\Desktop\ammoraccoon-web\scripts\gen-calibers\_corpus.json"
ALLOWED_NEW = {"380acp", "300blk", "308win", "38spl", "223-556"}


def load_corpus(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def verdicts(corpus):
    out = {}
    for inp in corpus:
        _disp, norm = normalize_caliber(inp)
        out[inp] = norm
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", default=DEFAULT_CORPUS)
    ap.add_argument("--snapshot")
    ap.add_argument("--compare")
    ap.add_argument("--show", type=int, default=0)
    args = ap.parse_args()

    corpus = load_corpus(args.corpus)
    print(f"corpus: {len(corpus)} inputs")
    now = verdicts(corpus)

    if args.snapshot:
        with open(args.snapshot, "w", encoding="utf-8") as f:
            json.dump(now, f)
        nonnull = sum(1 for v in now.values() if v)
        print(f"snapshot written: {args.snapshot}  ({nonnull} non-None / {len(now)})")
        return 0

    if args.compare:
        with open(args.compare, encoding="utf-8") as f:
            before = json.load(f)
        flips, bad_new = [], []
        pickups = Counter()
        samples = {}
        for inp, after in now.items():
            bef = before.get(inp)
            if bef == after:
                continue
            if bef is not None:
                flips.append((inp, bef, after))
            elif after in ALLOWED_NEW:
                pickups[after] += 1
                samples.setdefault(after, []).append(inp)
            else:
                bad_new.append((inp, after))

        print("=== pickups (None -> caliber) ===")
        for cal in sorted(pickups):
            print(f"  {cal}: {pickups[cal]}")
        print(f"  total pickups: {sum(pickups.values())}")
        if args.show:
            for cal in sorted(samples):
                print(f"  --- {cal} sample inputs ---")
                for s in samples[cal][:args.show]:
                    print(f"      {s[:110]}")

        ok = not flips and not bad_new
        if flips:
            print(f"\nFAIL: {len(flips)} existing verdict(s) FLIPPED:")
            for inp, b, a in flips[:40]:
                print(f"  {b} -> {a} : {inp[:100]}")
        if bad_new:
            print(f"\nFAIL: {len(bad_new)} None -> UNEXPECTED caliber:")
            for inp, a in bad_new[:40]:
                print(f"  None -> {a} : {inp[:100]}")
        print("\nVERDICT:", "PASS (additive-only)" if ok else "FAIL")
        return 0 if ok else 1

    print("nothing to do — pass --snapshot or --compare")
    return 2


if __name__ == "__main__":
    sys.exit(main())
