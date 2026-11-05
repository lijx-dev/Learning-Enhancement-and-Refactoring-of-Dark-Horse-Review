"""Unit checks for Hive acceptance report semantics."""
import sys
from copy import deepcopy
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from acceptance_hive import validate_report


class HiveContractTest(unittest.TestCase):
    def sample(self):
        expected = {'counts': {'orders': 1, 'history': 2},
            'created': [{'biz_date': '2026-10-30', 'shop_id': '1', 'orders': 1, 'known_cents': 1000}],
            'paid': [{'biz_date': '2026-10-30', 'shop_id': '1', 'orders': 1, 'known_cents': 1000}],
            'closed': [{'biz_date': '2026-10-30', 'orders': 1}]}
        snapshot = {k: deepcopy(expected[k]) for k in ('created', 'paid', 'closed')}
        snapshot.update(created_orders=1, all_events=2,
            partitions={'orders': ['biz_date=2026-10-30'], 'events': ['event_date=2026-10-30']})
        report = {'passed': True, 'mode': 'reopen', 'catalog_implementation': 'hive',
            'checks': dict.fromkeys(['partition_pruning_observed', 'orders_external_hive_serde', 'events_external_hive_serde'], True),
            'snapshots': {'after': snapshot}}
        return report, {'after': expected}

    def test_counts_alone_cannot_hide_wrong_money(self):
        report, expected = self.sample()
        self.assertTrue(validate_report(report, expected))
        report['snapshots']['after']['paid'][0]['known_cents'] = 999
        with self.assertRaises(ValueError):
            validate_report(report, expected)

    def test_catalog_checks_and_partition_keys_are_required(self):
        for mutation in ('catalog', 'pruning', 'partition', 'external', 'count'):
            report, expected = self.sample()
            if mutation == 'catalog':
                report['catalog_implementation'] = 'in-memory'
            elif mutation == 'pruning':
                del report['checks']['partition_pruning_observed']
            elif mutation == 'external':
                report['checks']['orders_external_hive_serde'] = False
            elif mutation == 'count':
                report['snapshots']['after']['all_events'] = 3
            else:
                report['snapshots']['after']['partitions']['events'] = []
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                validate_report(report, expected)

    def test_build_cannot_skip_partition_repair(self):
        report, expected = self.sample()
        report['mode'] = 'build'
        with self.assertRaises(ValueError):
            validate_report(report, expected)


if __name__ == '__main__':
    unittest.main()
