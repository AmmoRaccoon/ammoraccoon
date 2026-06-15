"""Unit tests for the pure 5-gate evaluator (caliber_validate.py, #4 Step-3).

Covers every verdict path (PASS / FAIL / NEEDS_REVIEW) AND every entry_kind
(per_caliber, parent, gritr title_filter), including the two honesty-critical
cases: the empty-healthy -> PARK case (n_products==0, never FAIL) and the
mixed-caliber parent page with a generic title (must PASS, not FAIL).

Card-title fixtures are grounded in real normalize_caliber output (confirmed:
the 9mm/.357/.223/5.56/.308/7.62x39/300blk titles bucket as expected; .243 Win
and magazines bucket to None). Run: py scripts/test_caliber_validate.py
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from caliber_validate import (  # noqa: E402
    Page, evaluate, title_mentions,
    PASS, FAIL, NEEDS_REVIEW, PER_CALIBER, PARENT,
)

# --- Fixtures (all card titles confirmed against live normalize_caliber) ---
NINE = [
    'Federal American Eagle 9mm Luger 115gr FMJ 50rd',
    'Magtech 9mm Luger 124gr FMJ 50rd',
    'Winchester 9mm Luger 115gr FMJ 100rd',
    'Blazer Brass 9mm Luger 115gr FMJ 50rd',
    'Speer Lawman 9mm Luger 147gr TMJ 50rd',
    'CCI Blazer 9mm Luger 115gr 50rd',
    'Sig Sauer 9mm Luger 124gr FMJ 50rd',
    'Remington UMC 9mm Luger 115gr FMJ 50rd',
]
NOISE = [  # all normalize_caliber -> None (untracked)
    'Hornady .243 Winchester 90gr',
    'Magpul PMAG 30rd magazine',
    'Federal .243 Winchester 100gr SP',
    'Magpul PMAG 20rd magazine',
]
THREE57 = [  # all contain '357' AND normalize -> 357mag
    'Winchester .357 Magnum 158gr JSP 50rd',
    'Federal .357 Magnum 158gr JHP 50rd',
    'Magtech .357 Magnum 158gr FMJ 50rd',
    'Hornady .357 Magnum 125gr 25rd',
    'Speer .357 Magnum 158gr 50rd',
    'PMC .357 Magnum 158gr 50rd',
]
PARENT_MIX = [  # 6 tracked (223,556,308,762x39,300blk,9mm) + 2 untracked
    'PMC .223 Remington 55gr FMJ',
    'Federal 5.56 NATO 55gr 20rd',
    'Hornady .308 Winchester 150gr',
    'Wolf 7.62x39 123gr FMJ',
    'Barnes 300 Blackout 110gr',
    'Federal American Eagle 9mm Luger 115gr',
    'Hornady .243 Winchester 90gr',
    'Magpul PMAG 30rd magazine',
]


def page(status=200, req='/9mm-ammo', landed=None, title='9mm Luger Ammo', cards=None):
    return Page(status=status, requested_url=req,
                landed_url=req if landed is None else landed,
                title=title, card_titles=(NINE if cards is None else cards))


class TestPerCaliber(unittest.TestCase):
    def _eval(self, **kw):
        return evaluate(page(**kw), '9mm', PER_CALIBER)

    def test_pass(self):
        r = self._eval(cards=NINE[:7] + NOISE[:1])  # 7/8 = 87.5%
        self.assertEqual(r['verdict'], PASS)
        self.assertTrue(r['title_match'])
        self.assertEqual(r['gate_pass_pct'], 87.5)
        self.assertEqual(r['n_products'], 8)

    def test_fail_non_200(self):
        self.assertEqual(self._eval(status=404)['verdict'], FAIL)

    def test_fail_redirect(self):
        r = self._eval(req='/9mm-ammo', landed='/40-sw-ammo')
        self.assertEqual(r['verdict'], FAIL)
        self.assertTrue(r['redirect'])

    def test_fail_gate4_below_30(self):
        r = self._eval(cards=NINE[:2] + NOISE * 2)  # 2/10 = 20%
        self.assertEqual(r['verdict'], FAIL)
        self.assertEqual(r['gate_pass_pct'], 20.0)

    def test_fail_gate3_names_different_caliber(self):
        # Title names 40 S&W (a different tracked caliber) -> hard FAIL,
        # short-circuits before the (9mm) product gate.
        r = self._eval(title='40 S&W Ammo', cards=NINE)
        self.assertEqual(r['verdict'], FAIL)
        self.assertIn('different tracked caliber', r['note'])

    def test_nr_gate4_gray(self):
        r = self._eval(cards=NINE[:4] + NOISE)  # 4/8 = 50%
        self.assertEqual(r['verdict'], NEEDS_REVIEW)
        self.assertEqual(r['gate_pass_pct'], 50.0)

    def test_nr_gate5_small_count(self):
        # 3 cards, 100% match — small count is NEEDS_REVIEW, never a hard FAIL.
        r = self._eval(cards=NINE[:3])
        self.assertEqual(r['verdict'], NEEDS_REVIEW)
        self.assertEqual(r['n_products'], 3)

    def test_nr_empty_healthy(self):
        r = self._eval(cards=[])
        self.assertEqual(r['verdict'], NEEDS_REVIEW)
        self.assertEqual(r['n_products'], 0)
        self.assertIn('empty-healthy', r['note'])

    def test_nr_unreachable(self):
        r = self._eval(status=None)
        self.assertEqual(r['verdict'], NEEDS_REVIEW)
        self.assertIn('unreachable', r['note'])

    def test_nr_generic_title_products_ok(self):
        # Products strongly match (87.5%) but the title names no caliber:
        # defer to a human, do NOT auto-PASS.
        r = self._eval(title='Ammunition | The Store', cards=NINE[:7] + NOISE[:1])
        self.assertEqual(r['verdict'], NEEDS_REVIEW)
        self.assertFalse(r['title_match'])


class TestParent(unittest.TestCase):
    def _eval(self, cards, title='Rifle Ammo'):
        p = Page(200, '/ammo/rifle-ammo/', '/ammo/rifle-ammo/', title, cards)
        return evaluate(p, None, PARENT)

    def test_parent_pass_mixed_generic_title(self):
        # THE CRITICAL CASE: generic parent title + mixed-caliber cards must
        # PASS (gate3 exempt; gate4 = presence of tracked cards), not FAIL.
        r = self._eval(PARENT_MIX)
        self.assertEqual(r['verdict'], PASS)
        self.assertIsNone(r['title_match'])
        self.assertEqual(r['n_products'], 8)
        self.assertEqual(r['gate_pass_pct'], 75.0)  # 6 tracked / 8

    def test_parent_fail_zero_tracked(self):
        # Cards present but NONE bucket to a tracked caliber = wrong/broken parent.
        r = self._eval(NOISE)
        self.assertEqual(r['verdict'], FAIL)

    def test_parent_nr_few_tracked(self):
        r = self._eval([PARENT_MIX[0], PARENT_MIX[2], PARENT_MIX[3]] + NOISE[:2])  # 3 tracked
        self.assertEqual(r['verdict'], NEEDS_REVIEW)

    def test_parent_empty_healthy(self):
        r = self._eval([])
        self.assertEqual(r['verdict'], NEEDS_REVIEW)
        self.assertIn('empty-healthy', r['note'])


class TestGritrTitleFilter(unittest.TestCase):
    def test_filter_promotes_gray_to_pass(self):
        # A mixed handgun page: 6x .357 + 4x 9mm noise. Whole page = 60% (gray);
        # the title_filter narrows to the 6 .357 cards -> 100% -> PASS.
        cards = THREE57[:6] + NINE[:4]
        p = page(title='357 Magnum Ammo', cards=cards)
        without = evaluate(p, '357mag', PER_CALIBER)
        self.assertEqual(without['verdict'], NEEDS_REVIEW)
        self.assertEqual(without['gate_pass_pct'], 60.0)
        with_filter = evaluate(p, '357mag', PER_CALIBER, title_filter=r'357')
        self.assertEqual(with_filter['verdict'], PASS)
        self.assertEqual(with_filter['gate_pass_pct'], 100.0)
        self.assertEqual(with_filter['n_products'], 6)  # post-filter count

    def test_filter_postfilter_small_count_is_nr(self):
        cards = THREE57[:3] + NINE[:5]
        p = page(title='357 Magnum Ammo', cards=cards)
        r = evaluate(p, '357mag', PER_CALIBER, title_filter=r'357')
        self.assertEqual(r['verdict'], NEEDS_REVIEW)
        self.assertEqual(r['n_products'], 3)


class TestTitleMentions(unittest.TestCase):
    def test_names_the_caliber(self):
        self.assertTrue(title_mentions('9mm Luger Ammo', '9mm'))
        self.assertTrue(title_mentions('5.56 NATO Ammo', '223-556'))
        self.assertTrue(title_mentions('223 Remington Ammo', '223-556'))

    def test_generic_and_wrong(self):
        self.assertFalse(title_mentions('Ammunition | Store', '9mm'))
        self.assertFalse(title_mentions('40 S&W Ammo', '9mm'))
        self.assertTrue(title_mentions('40 S&W Ammo', '40sw'))

    def test_whole_token_no_substring_false_positive(self):
        # '556' must not match inside '5560'.
        self.assertFalse(title_mentions('Brand 5560 Widget', '223-556'))


class TestResultShape(unittest.TestCase):
    def test_keys(self):
        r = evaluate(page(), '9mm', PER_CALIBER)
        self.assertEqual(set(r), {'verdict', 'status', 'redirect', 'title_match',
                                  'gate_pass_pct', 'n_products', 'note'})


if __name__ == '__main__':
    unittest.main(verbosity=2)
