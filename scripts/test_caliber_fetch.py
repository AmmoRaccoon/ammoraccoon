"""Unit tests for caliber_fetch helpers (#4 Step-3).

Locks fix #2: the requests-path title extractor html.unescape()s entities, so
a '&amp;'-encoded title ('.40 S&W') decodes and title-matches instead of having
an "amp" token break the alias (the fenix 40sw wave-1 false title-negative).

Run: py scripts/test_caliber_fetch.py
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from caliber_fetch import _extract_title  # noqa: E402
from caliber_validate import title_mentions  # noqa: E402


class TestExtractTitle(unittest.TestCase):
    def test_unescapes_amp_so_40sw_title_matches(self):
        t = _extract_title('<title>.40 S&amp;W Ammunition for Sale</title>')
        self.assertEqual(t, '.40 S&W Ammunition for Sale')
        self.assertTrue(title_mentions(t, '40sw'))   # was False before the fix

    def test_unescapes_ndash_and_collapses_whitespace(self):
        t = _extract_title('<title>9mm &ndash;   Fenix\n Ammunition</title>')
        self.assertNotIn('ndash', t)
        self.assertNotIn('&', t)
        self.assertTrue(title_mentions(t, '9mm'))

    def test_no_title_tag_returns_empty(self):
        self.assertEqual(_extract_title('<html><body>no title here</body></html>'), '')


if __name__ == '__main__':
    unittest.main(verbosity=2)
