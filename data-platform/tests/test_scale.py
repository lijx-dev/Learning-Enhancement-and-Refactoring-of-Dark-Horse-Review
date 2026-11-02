"""Protect the scale generator's boundary cases and independent oracle semantics."""
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from scale_fixture import generate
from verify_scale import oracle


class ScaleFixtureTest(unittest.TestCase):
    def test_frozen_sources_repeat_and_first_order_crosses_midnight(self):
        with tempfile.TemporaryDirectory() as root:
            source, batch = generate(root, 10)
            before = {p.name: p.read_bytes() for p in source.iterdir()}
            self.assertEqual(generate(root, 10), (source, batch))
            self.assertEqual(before, {p.name: p.read_bytes() for p in source.iterdir()})
            orders = list(map(json.loads, (source / 'tb_voucher_order.jsonl').read_text(encoding='utf-8').splitlines()))
            first = orders[0]
            self.assertEqual(first['id'], '9007199254741001')
            self.assertIsNone(first['pay_value_snapshot'])
            self.assertEqual(first['create_time'], '2026-10-21T15:50:00Z')
            self.assertEqual(first['pay_time'], '2026-10-21T16:10:00Z')
            self.assertEqual(orders[1]['pay_value_snapshot'], '125')
            self.assertEqual(orders[6]['status'], '4')
            self.assertEqual(orders[8]['status'], '1')

    def test_guard_local_budget_and_detect_source_tampering(self):
        with tempfile.TemporaryDirectory() as root:
            for size in (0, 100001):
                with self.assertRaises(ValueError):
                    generate(root, size)
            source, _ = generate(root, 10)
            with (source / 'tb_voucher_order.jsonl').open('a') as stream:
                stream.write('{}\n')
            with self.assertRaises(ValueError):
                generate(root, 10)

    def test_oracle_distinguishes_all_unknown_from_absent_group(self):
        with tempfile.TemporaryDirectory() as root:
            source, _ = generate(root, 1)
            daily, cohort, status, count, events, cents = oracle(source)
            self.assertEqual((count, events, cents), (1, 2, 0))
            created = next(row for (day, _), row in daily.items() if day == '2026-10-21')
            paid = next(row for (day, _), row in daily.items() if day == '2026-10-22')
            self.assertIsNone(created['created_known_cents'])
            self.assertEqual(created['paid_known_cents'], 0)
            self.assertIsNone(paid['paid_known_cents'])
            self.assertEqual(paid['created_known_cents'], 0)
            self.assertIsNone(next(iter(cohort.values()))['created_known_cents'])
            self.assertEqual(status, {2: 1})


if __name__ == '__main__':
    unittest.main()
