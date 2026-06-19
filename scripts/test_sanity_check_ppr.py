"""Unit tests for the write-time price-integrity gate (price-honesty audit,
2026-06-19). Covers BOTH holes Track 2 closes:

  FIX 1 — sanity_check_ppr's new arithmetic-consistency check (scraper_lib.py):
    (a) the 4 self-contradictory live cases now REJECT;
    (b) a real penny-rounded listing (per-round ~2-10% off box/rounds) still
        PASSES — grounded in real rows from the 107-listing legit cluster;
    (c) the safety no-op (missing/<=0 price or rounds) still PASSES.

  FIX 2 — scraper_recheck.commit_change re-validates through the same guard:
    (d) the 4 internally-consistent-but-out-of-band cases are caught on the
        recheck path via the per-caliber floor/ceiling (caliber carried
        through — without it the .38spl/.357 floor cases slip past DEFAULT_FLOOR);
    plus the commit-path branching: confirm-in-stock that fails -> skip the
    bump (no write at all); flip->OOS that fails -> flag-only (no price_history).

All stored values are a 2026-06-19 live snapshot of the 8 audit IDs (hardcoded
so the test is deterministic and survives the rows being fixed). Run:
    py scripts/test_sanity_check_ppr.py
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scraper_lib import (  # noqa: E402
    sanity_check_ppr, DEFAULT_FLOOR,
    PPR_INCONSISTENCY_REL, PPR_INCONSISTENCY_ABS,
)
import scraper_recheck  # noqa: E402

# --- 2026-06-19 live snapshots (id, caliber, base_price, total_rounds, ppr) ---
# The 4 SELF-CONTRADICTORY rows (ppr disagrees with base/rounds) — Fix 1 target.
SELF_CONTRADICTORY = [
    (3527686, '308win', 1.08, 500, 1.08),    # base_price holds the per-round
    (137911,  '9mm',   28.39,  50, 1.42),    # real 20ct; rounds=50 wrong
    (138274,  '22lr',    245, 100, 0.123),   # real 2000rd; rounds=100 wrong
    (4041506, '300blk',  110,  50, 0.88),    # ppr wrong; base+count correct
]
# The 4 INTERNALLY-CONSISTENT-BUT-OUT-OF-BAND rows (all 3 fields agree, but the
# per-round number is impossible) — caught by the EXISTING floor/ceiling once
# re-validation runs on the recheck path. Fix 2 target.
OUT_OF_BAND = [
    (159086, '223-556', 259,  20, 12.95),    # $12.95/rd .223 -> over ceiling
    (210003, '357mag', 58.82, 499, 0.1179),  # below .357 floor
    (482490, '38spl',  88.26, 501, 0.1762),  # below .38spl floor (NOT default)
    (32362,  '357mag', 14.91, 200, 0.0746),  # below .357 floor (already OOS)
]
# Real penny-rounded LEGIT rows from the 107-listing cluster (2-10% off) — must
# PASS (displayed price is correct; retailer surfaced a rounded per-round).
LEGIT_PENNY_ROUNDED = [
    (308259, '22lr',     11, 100, 0.10),     # exp 0.11, 9.1% off, $0.01 abs
    (54965,  '357mag', 34.99, 50, 0.66),     # exp 0.6998, 5.7% off, $0.04 abs
    (3059984, '223-556', 12, 20, 0.55),      # exp 0.60, 8.3% off, $0.05 abs
]


class TestArithmeticConsistency(unittest.TestCase):
    """FIX 1 — the new base/rounds <-> ppr agreement check."""

    def test_thresholds_are_the_proven_track1_values(self):
        self.assertEqual(PPR_INCONSISTENCY_REL, 0.25)
        self.assertEqual(PPR_INCONSISTENCY_ABS, 0.10)

    def test_a_self_contradictory_cases_reject(self):
        for _id, cal, base, rounds, ppr in SELF_CONTRADICTORY:
            self.assertFalse(
                sanity_check_ppr(ppr, base, rounds, context=f'id={_id}', caliber=cal),
                msg=f'id={_id} ({cal}) should REJECT: ppr={ppr} vs {base}/{rounds}')

    def test_b_legit_penny_rounded_passes(self):
        for _id, cal, base, rounds, ppr in LEGIT_PENNY_ROUNDED:
            self.assertTrue(
                sanity_check_ppr(ppr, base, rounds, context=f'id={_id}', caliber=cal),
                msg=f'id={_id} ({cal}) should PASS: ppr={ppr} vs {base}/{rounds}')

    def test_c_safety_noop_missing_or_zero_fields(self):
        # In-band ppr; arithmetic must be SKIPPED (not rejected) when we cannot
        # compute base/rounds. Never reject what we cannot disprove.
        self.assertTrue(sanity_check_ppr(0.50, None, 50, caliber='9mm'))
        self.assertTrue(sanity_check_ppr(0.50, 25, None, caliber='9mm'))
        self.assertTrue(sanity_check_ppr(0.50, 25, 0, caliber='9mm'))
        self.assertTrue(sanity_check_ppr(0.50, 0, 50, caliber='9mm'))

    def test_consistent_in_band_row_passes(self):
        self.assertTrue(sanity_check_ppr(0.50, 25.0, 50, caliber='9mm'))

    def test_both_gates_required_large_rel_small_abs_passes(self):
        # .22 LR: 0.10 vs 0.13 = 23% rel but only 3c abs -> must PASS (the abs
        # gate immunises cheap-caliber rounding). 0.13/100rd = $13 box.
        self.assertTrue(sanity_check_ppr(0.10, 13.0, 100, caliber='22lr'))


class TestOutOfBandCaughtByExistingGuard(unittest.TestCase):
    """FIX 2 (guard level) — the consistent-but-out-of-band rows are caught by
    the EXISTING floor/ceiling check, which only fails to fire today because
    recheck never re-runs the guard."""

    def test_d_out_of_band_cases_reject_with_caliber(self):
        for _id, cal, base, rounds, ppr in OUT_OF_BAND:
            self.assertFalse(
                sanity_check_ppr(ppr, base, rounds, context=f'id={_id}', caliber=cal),
                msg=f'id={_id} ({cal}) should REJECT: ppr={ppr} out of band')

    def test_caliber_carry_through_is_required(self):
        # 482490 (.38spl, $0.1762/rd) is consistent AND above DEFAULT_FLOOR
        # (0.15) -> WITHOUT caliber it wrongly PASSES; only the per-caliber
        # .38spl floor (0.20) catches it. This is why Fix 2 must select+carry
        # caliber_normalized.
        self.assertGreater(0.1762, DEFAULT_FLOOR)
        self.assertTrue(sanity_check_ppr(0.1762, 88.26, 501))            # no caliber -> slips
        self.assertFalse(sanity_check_ppr(0.1762, 88.26, 501, caliber='38spl'))


# --- Fake Supabase recorder for commit_change branching (Fix 2 write path) ---
class _FakeQuery:
    def __init__(self, calls, table):
        self._calls, self._table, self._op, self._payload = calls, table, None, None

    def update(self, data):
        self._op, self._payload = 'update', data
        return self

    def insert(self, data, returning=None):
        self._op, self._payload = 'insert', data
        return self

    def eq(self, *a, **k):
        return self

    def execute(self):
        self._calls.append((self._table, self._op, self._payload))
        return None


class _FakeSupabase:
    def __init__(self):
        self.calls = []

    def table(self, name):
        return _FakeQuery(self.calls, name)


def _rec(listing_id, cal, base, rounds, ppr, new_in, *, price=None, slug='test'):
    return {
        'listing_id': listing_id, 'slug': slug,
        'base_price': base, 'price_per_round': ppr, 'total_rounds': rounds,
        'caliber_normalized': cal, 'price': price, 'new_in_stock': new_in,
    }


class TestRecheckCommitBranching(unittest.TestCase):
    """FIX 2 (write path) — honest-blank branching in commit_change."""
    NOW = '2026-06-19T00:00:00+00:00'

    def _run(self, rec):
        sb = _FakeSupabase()
        outcome = scraper_recheck.commit_change(sb, rec, self.NOW)
        return outcome, sb.calls

    def test_confirm_instock_failed_reval_skips_everything(self):
        # 482490-style: confirm in-stock but stored price is out of band ->
        # skip the bump entirely so the 6h freshness gate ages it out.
        outcome, calls = self._run(_rec(482490, '38spl', 88.26, 501, 0.1762, True))
        self.assertEqual(outcome, 'skipped_reval')
        self.assertEqual(calls, [])  # NO listings update, NO price_history

    def test_flip_oos_failed_reval_is_flag_only(self):
        # 4041506-style self-contradictory row flipping OOS: flip the stock flag
        # (honest — item is gone) but DO NOT record a bad price.
        outcome, calls = self._run(_rec(4041506, '300blk', 110, 50, 0.88, False))
        self.assertEqual(outcome, 'flag_only')
        tables = [c[0] for c in calls]
        self.assertIn('listings', tables)
        self.assertNotIn('price_history', tables)
        # the one write flips in_stock False
        upd = next(c[2] for c in calls if c[0] == 'listings')
        self.assertFalse(upd['in_stock'])

    def test_passing_confirm_writes_listing_and_history(self):
        outcome, calls = self._run(_rec(1, '9mm', 25.0, 50, 0.50, True))
        self.assertEqual(outcome, 'written')
        tables = [c[0] for c in calls]
        self.assertIn('listings', tables)
        self.assertIn('price_history', tables)

    def test_passing_confirm_with_fresh_price_recomputes_ppr(self):
        # Fresh live price parsed -> ppr recomputed = price/rounds (consistent),
        # passes, and the refreshed base/ppr land on the listing update.
        outcome, calls = self._run(
            _rec(2, '9mm', 24.0, 50, 0.48, True, price=24.99))
        self.assertEqual(outcome, 'written')
        upd = next(c[2] for c in calls if c[0] == 'listings')
        self.assertEqual(upd['base_price'], 24.99)
        self.assertEqual(upd['price_per_round'], round(24.99 / 50, 4))


if __name__ == '__main__':
    unittest.main(verbosity=2)
