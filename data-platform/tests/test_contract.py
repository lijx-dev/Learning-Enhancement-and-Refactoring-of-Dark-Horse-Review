import importlib.util
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

PLATFORM = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLATFORM))
from common import exclusive_lock, publish, safe_name, sha256, validate_snapshot, write_json
spec = importlib.util.spec_from_file_location('fixture', PLATFORM / 'tools/fixture.py')
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


class ContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixture.generate()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = self.root / 's3'
        shutil.copytree(fixture.OUT / 's3', self.source)

    def tearDown(self):
        self.temp.cleanup()

    def mutate(self, table, transform):
        path = self.source / (table + '.jsonl')
        rows = [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines()]
        transform(rows)
        path.write_text(''.join(json.dumps(row) + '\n' for row in rows), encoding='utf-8')
        manifest = json.loads((self.source / 'manifest.json').read_text(encoding='utf-8'))
        manifest['files'][path.name] = {'rows': len(rows), 'sha256': sha256(path)}
        write_json(self.source / 'manifest.json', manifest)

    def test_checksum_blocks_corruption_even_if_json_remains_valid(self):
        path = self.source / 'tb_shop.jsonl'
        path.write_text(path.read_text(encoding='utf-8').replace('1', '3', 1), encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'corrupt'):
            validate_snapshot(self.source, 's3')

    def test_batch_mismatch_is_not_silently_accepted(self):
        with self.assertRaisesRegex(ValueError, 'batch'):
            validate_snapshot(self.source, 's2')

    def test_duplicate_paid_event_cannot_double_amount(self):
        def change(rows):
            duplicate = next(row.copy() for row in rows if row['event_type'] == 'ORDER_PAID')
            duplicate['event_id'] = '99999'
            rows.append(duplicate)
        self.mutate('tb_order_event_history', change)
        with self.assertRaisesRegex(ValueError, 'Multiple paid'):
            validate_snapshot(self.source, 's3')

    def test_unsigned_upper_bound_preserved_and_numeric_id_rejected(self):
        self.mutate('tb_voucher_order', lambda rows: rows[0].update(user_id=str(2**64-1)))
        validate_snapshot(self.source, 's3')
        self.mutate('tb_voucher_order', lambda rows: rows[0].update(user_id=2**64-1))
        with self.assertRaisesRegex(ValueError, 'exact'):
            validate_snapshot(self.source, 's3')

    def test_negative_amount_and_zero_date_fail_contract(self):
        self.mutate('tb_voucher_order', lambda rows: rows[0].update(pay_value_snapshot=-1))
        with self.assertRaisesRegex(ValueError, 'cents'):
            validate_snapshot(self.source, 's3')
        shutil.rmtree(self.source)
        shutil.copytree(fixture.OUT / 's3', self.source)
        self.mutate('tb_voucher_order', lambda rows: rows[0].update(create_time='0000-00-00 00:00:00'))
        with self.assertRaises(ValueError):
            validate_snapshot(self.source, 's3')

    def test_lock_prevents_two_publishers_for_same_date(self):
        with exclusive_lock(self.root, '2026-10-30'):
            with self.assertRaises(FileExistsError):
                with exclusive_lock(self.root, '2026-10-30'):
                    self.fail('second publisher acquired lock')
        with exclusive_lock(self.root, '2026-10-30'):
            pass

    def test_immutable_versions_and_failed_publish_keep_pointer(self):
        old = self.root / 'old'
        old.mkdir()
        (old / 'content').write_text('success')
        catalog = self.root / 'catalog.json'
        publish(old, self.root / 'version1', catalog, {'run_id': 'r1'})
        before = catalog.read_bytes()
        candidate = self.root / 'candidate'
        candidate.mkdir()
        with self.assertRaises(FileExistsError):
            publish(candidate, self.root / 'version1', catalog, {'run_id': 'r2'})
        self.assertEqual(before, catalog.read_bytes())
        self.assertEqual('success', (self.root / 'version1/content').read_text())

    def test_path_traversal_is_rejected(self):
        for value in ('../outside', 'x/y', 'x\\y', '.hidden'):
            with self.assertRaises(ValueError):
                safe_name(value)


if __name__ == '__main__':
    unittest.main()
