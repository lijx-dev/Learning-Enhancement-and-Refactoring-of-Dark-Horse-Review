"""Validate the real filesystem request protocol without Docker/Airflow imports."""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

PLATFORM = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLATFORM / 'orchestration'))
sys.path.insert(0, str(PLATFORM / 'tools'))
import bridge
from common import sha256, write_json
from scale_fixture import generate


class BridgeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.queue = self.root / 'target/queue'
        self.queue.mkdir(parents=True)
        self.root_patch = patch.object(bridge, 'ROOT', self.root)
        self.queue_patch = patch.object(bridge, 'QUEUE', self.queue)
        self.root_patch.start()
        self.queue_patch.start()
        source, batch = generate(self.root / 'target/sources', 10)
        self.config = {'biz_date': '2026-10-21', 'source_batch': batch,
            'input_root': source.relative_to(self.root).as_posix(), 'run_root': 'target/runs', 'demo_fail_first': True}

    def tearDown(self):
        self.queue_patch.stop()
        self.root_patch.stop()
        self.temp.cleanup()

    def test_retry_has_new_identity_and_duplicate_is_not_reexecuted(self):
        one = bridge.submit(self.config, 'manual__demo', 1)
        self.assertEqual(one, bridge.submit(self.config, 'manual__demo', 1))
        calls = []

        def fail(*args):
            calls.append(args)
            return 1

        path = self.queue / (one + '.request.json')
        self.assertTrue(bridge.execute_request(path, fail))
        self.assertTrue(calls[0][-2])
        self.assertFalse(bridge.execute_request(path, fail))
        with self.assertRaises(RuntimeError):
            bridge.wait_result(one, self.config)
        failed = json.loads((self.queue / (one + '.result.json')).read_text())
        self.assertIn('catalog_before_sha256', failed)
        self.assertIn('catalog_after_sha256', failed)
        self.assertEqual(failed['catalog_before_sha256'], failed['catalog_after_sha256'])
        two = bridge.submit(self.config, 'manual__demo', 2)
        self.assertNotEqual(one, two)
        self.assertEqual(len(calls), 1)

    def test_expired_request_cannot_start_spark(self):
        key = bridge.submit(self.config, 'expired', 1)
        path = self.queue / (key + '.request.json')
        request = json.loads(path.read_text(encoding='utf-8'))
        request['expires_at_utc'] = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
        write_json(path, request)
        bridge.execute_request(path, lambda *args: self.fail('Expired request executed'))
        result = json.loads((self.queue / (key + '.result.json')).read_text())
        self.assertFalse(result['passed'])
        self.assertIn('expired', result['error'])

    def test_paths_and_free_form_commands_are_rejected(self):
        for value in ('../other', 'README.md', '/workspace/target', 'target/../../outside', 'target\\x'):
            with self.assertRaises(ValueError):
                bridge.project_path(value)
        with self.assertRaises(ValueError):
            bridge.validate_config({**self.config, 'command': 'anything'})
        with self.assertRaises(ValueError):
            bridge.validate_config({**self.config, 'biz_date': '2026-02-30'})
        with self.assertRaises(ValueError):
            bridge.validate_config({**self.config, 'run_root': 'target/sources'})

    def test_success_is_verified_and_artifact_tampering_rejected(self):
        key = bridge.submit(self.config, 'success', 2)

        def publish_fake(source, dest, batch, day, run_id, fail, cache):
            self.assertFalse(fail)
            version = dest / 'versions' / day / run_id
            version.mkdir(parents=True)
            (version / 'model.txt').write_text('known result', encoding='utf-8')
            write_json(version / 'run_manifest.json', {'run_id': run_id, 'source_batch': batch,
                'biz_date': day, 'quality': {'passed': True}, 'artifacts': {'model.txt': sha256(version / 'model.txt')}})
            (dest / 'catalog').mkdir()
            write_json(dest / 'catalog' / (day + '.json'), {'version': version.relative_to(dest).as_posix(),
                'manifest_sha256': sha256(version / 'run_manifest.json')})
            return 0

        bridge.execute_request(self.queue / (key + '.request.json'), publish_fake)
        result = bridge.wait_result(key, self.config)
        self.assertTrue(result['passed'])
        # A later legitimate backfill changes latest; this attempt's immutable result stays valid.
        write_json(self.root / self.config['run_root'] / 'catalog' / (self.config['biz_date'] + '.json'),
                   {'version': 'versions/later-backfill'})
        self.assertTrue(bridge.wait_result(key, self.config)['passed'])
        artifact = self.root / self.config['run_root'] / result['version'] / 'model.txt'
        artifact.write_text('corrupted', encoding='utf-8')
        with self.assertRaises(ValueError):
            bridge.wait_result(key, self.config)

    def test_unfinished_claim_is_preserved(self):
        key = bridge.submit(self.config, 'interrupted', 1)
        claim = self.queue / (key + '.claim.json')
        write_json(claim, {'pid': -1})
        self.assertFalse(bridge.execute_request(self.queue / (key + '.request.json'), lambda *a: self.fail('Reclaimed')))
        self.assertTrue(claim.exists())


if __name__ == '__main__':
    unittest.main()
