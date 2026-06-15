"""Unit tests for the validation write-back + telemetry (caliber_writeback.py).

Covers the change-of-record gate, byte-level idempotency, the honesty boundary
(NEVER flips status), parent_paths entries, and the JSONL telemetry append.
Run: py scripts/test_caliber_writeback.py
"""
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import caliber_paths_io  # noqa: E402
from caliber_writeback import write_validation, log_telemetry  # noqa: E402

CFG = {
    'retailer': 'tmptest',
    'platform': 'shopify',
    'base': 'https://example.com',
    'calibers': {
        '9mm': [{'path': '/collections/9mm', 'status': 'candidate'}],
    },
    'parent_paths': [{'path': '/ammo/rifle/', 'status': 'active'}],
}
PASS_REC = {'verdict': 'PASS', 'status': 200, 'redirect': False,
            'title_match': True, 'gate_pass_pct': 87.5, 'n_products': 8,
            'note': 'all gates pass'}
FAIL_REC = {'verdict': 'FAIL', 'status': 404, 'redirect': None,
            'title_match': None, 'gate_pass_pct': None, 'n_products': None,
            'note': 'HTTP 404'}


class WritebackBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='cwb_')
        self.cfg_path = os.path.join(self.tmp, 'tmptest.json')
        with open(self.cfg_path, 'w', encoding='utf-8', newline='\n') as f:
            f.write(caliber_paths_io.dump_config(CFG))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _entry(self, caliber='9mm'):
        cfg = caliber_paths_io.load_config(self.cfg_path)
        return (cfg['calibers'][caliber][0] if caliber
                else cfg['parent_paths'][0])


class TestWriteValidation(WritebackBase):
    def test_first_write_records_verdict(self):
        wrote = write_validation(self.cfg_path, '9mm', '/collections/9mm',
                                 PASS_REC, validated_at='2026-06-14')
        self.assertTrue(wrote)
        v = self._entry()['validation']
        self.assertEqual(v['verdict'], 'PASS')
        self.assertEqual(v['method'], 'harness')
        self.assertEqual(v['validated_at'], '2026-06-14')
        self.assertEqual(v['gate_pass_pct'], 87.5)

    def test_idempotent_same_verdict_zero_byte_diff(self):
        self.assertTrue(write_validation(self.cfg_path, '9mm', '/collections/9mm',
                                         PASS_REC, validated_at='2026-06-14'))
        b1 = Path(self.cfg_path).read_bytes()
        wrote2 = write_validation(self.cfg_path, '9mm', '/collections/9mm',
                                  PASS_REC, validated_at='2026-06-14')
        b2 = Path(self.cfg_path).read_bytes()
        self.assertFalse(wrote2)        # no change-of-record -> no write
        self.assertEqual(b1, b2)        # byte-identical file

    def test_verdict_change_rewrites(self):
        write_validation(self.cfg_path, '9mm', '/collections/9mm', PASS_REC,
                         validated_at='2026-06-14')
        wrote = write_validation(self.cfg_path, '9mm', '/collections/9mm',
                                 FAIL_REC, validated_at='2026-06-15')
        self.assertTrue(wrote)          # PASS -> FAIL is a change-of-record
        self.assertEqual(self._entry()['validation']['verdict'], 'FAIL')

    def test_never_touches_status(self):
        # The honesty boundary: writing a verdict must NOT flip status.
        write_validation(self.cfg_path, '9mm', '/collections/9mm', PASS_REC,
                         validated_at='2026-06-14')
        self.assertEqual(self._entry()['status'], 'candidate')   # not -> active
        write_validation(self.cfg_path, '9mm', '/collections/9mm', FAIL_REC,
                         validated_at='2026-06-15')
        self.assertEqual(self._entry()['status'], 'candidate')   # still candidate

    def test_parent_paths_entry(self):
        wrote = write_validation(self.cfg_path, None, '/ammo/rifle/', PASS_REC,
                                 validated_at='2026-06-14')
        self.assertTrue(wrote)
        self.assertEqual(self._entry(None)['validation']['verdict'], 'PASS')
        self.assertEqual(self._entry(None)['status'], 'active')  # untouched

    def test_missing_entry_raises(self):
        with self.assertRaises(KeyError):
            write_validation(self.cfg_path, '9mm', '/collections/nope', PASS_REC,
                             validated_at='2026-06-14')

    def test_clean_diff_only_validation_lines(self):
        # The only lines that change are inside the validation block; status,
        # path, base, etc. survive verbatim.
        before = Path(self.cfg_path).read_text(encoding='utf-8')
        write_validation(self.cfg_path, '9mm', '/collections/9mm', PASS_REC,
                         validated_at='2026-06-14')
        after = Path(self.cfg_path).read_text(encoding='utf-8')
        self.assertIn('"path": "/collections/9mm"', after)
        self.assertIn('"status": "candidate"', after)   # the ENTRY status, intact
        self.assertIn('"verdict": "PASS"', after)
        self.assertNotIn('"verdict"', before)


class TestTelemetry(WritebackBase):
    def test_appends_jsonl_continuous(self):
        tpath = os.path.join(self.tmp, 'tele.jsonl')
        log_telemetry(PASS_REC, retailer='tmptest', caliber='9mm',
                      path_query='/collections/9mm', timestamp='t1',
                      telemetry_path=tpath)
        log_telemetry(FAIL_REC, retailer='tmptest', caliber='9mm',
                      path_query='/collections/9mm', timestamp='t2',
                      telemetry_path=tpath)
        lines = Path(tpath).read_text(encoding='utf-8').strip().split('\n')
        self.assertEqual(len(lines), 2)          # every run logs (continuous)
        rec0 = json.loads(lines[0])
        self.assertEqual(rec0['verdict'], 'PASS')
        self.assertEqual(rec0['timestamp'], 't1')
        self.assertGreaterEqual(
            set(rec0),
            {'timestamp', 'retailer', 'caliber', 'url', 'verdict', 'status',
             'gate_pass_pct', 'n_products', 'note'})


if __name__ == '__main__':
    unittest.main(verbosity=2)
