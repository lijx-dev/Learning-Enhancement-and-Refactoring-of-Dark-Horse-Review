"""Regressions for date literals, timing ratios, pruning evidence and durable retries."""
import importlib.util
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'data-platform/jobs'))
sys.path.insert(0, str(ROOT / 'data-platform/tests'))
from performance_queries import median_ratio, validated_day
from offline_pipeline import parse_models
from acceptance_airflow import validate_retry_evidence

spec = importlib.util.spec_from_file_location('hive_smoke', ROOT / 'dev/local/hive_smoke.py')
hive = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hive)


class ReviewRegressionTest(unittest.TestCase):
    def test_model_boundary_does_not_truncate_comment_or_string_semicolons(self):
        models = parse_models("-- model: first\n-- comment; still a comment\nSELECT ';' AS value;\n"
                              "-- model: second\nSELECT 2;\n")
        self.assertEqual(set(models), {'first', 'second'})
        self.assertIn("SELECT ';' AS value;", models['first'])
        self.assertEqual(models['second'], 'SELECT 2;')

    def test_sql_day_rejects_noncanonical_and_injected_literals(self):
        self.assertEqual(validated_day('2024-02-29'), '2024-02-29')
        for value in ('2026-02-29', '20261030', '2026-10-30\n', "2026-10-30' OR 1=1 --"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validated_day(value)

    def test_unmeasurable_ratio_is_not_reported_as_a_speedup(self):
        self.assertEqual(median_ratio(2, 0.5), 4)
        for baseline, candidate in ((1, 0), (1, -1), (float('inf'), 1), (1, float('nan'))):
            self.assertIsNone(median_ratio(baseline, candidate))

    def test_pruning_requires_target_date_equality_and_only_its_location(self):
        plan = ('Location: InMemoryFileIndex [file:/warehouse/event_date=2026-10-30]\n'
                'PartitionFilters: [isnotnull(event_date#17),\n (event_date#17 = 2026-10-30)]\n'
                'ReadSchema: struct<>\n')
        self.assertTrue(hive.pruning_evidence(plan, '2026-10-30')['passed'])
        self.assertFalse(hive.pruning_evidence(plan, '2026-10-29')['passed'])
        self.assertFalse(hive.pruning_evidence(plan.replace(',\n (event_date#17 = 2026-10-30)', ''),
                                             '2026-10-30')['passed'])
        wrong = plan.replace('/warehouse/event_date=2026-10-30', '/warehouse/event_date=2026-10-29')
        self.assertFalse(hive.pruning_evidence(wrong, '2026-10-30')['passed'])
        many = plan.replace('30]\n', '30,file:/warehouse/event_date=2026-10-29]\n')
        self.assertFalse(hive.pruning_evidence(many, '2026-10-30')['passed'])

    def test_retry_does_not_require_sampling_transient_state(self):
        # Even if every poll missed up_for_retry, a persisted failure and successful
        # second attempt can prove the retry; a second success alone cannot.
        snapshot = {'try_number': 2, 'failed_attempts': 1, 'task_state': 'success'}
        failed = {'passed': False, 'catalog_before_sha256': 'old', 'catalog_after_sha256': 'old'}
        self.assertTrue(validate_retry_evidence(snapshot, failed, 'old'))
        for change in ({'failed_attempts': 0}, {'try_number': 1}):
            with self.assertRaises(ValueError):
                validate_retry_evidence({**snapshot, **change}, failed, 'old')
        for change in ({'passed': True}, {'catalog_after_sha256': 'changed'}):
            with self.assertRaises(ValueError):
                validate_retry_evidence(snapshot, {**failed, **change}, 'old')


if __name__ == '__main__':
    unittest.main()
