"""HDFS acceptance must reject corrupt evidence, local fallback and reformatted state."""
from copy import deepcopy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from acceptance_hdfs import validate_report, validate_restart
import fixture
import hdfs_control as hdfs


class HdfsContractTest(unittest.TestCase):
    def status(self):
        return {'report': 'Live datanodes (1):', 'safe_mode': 'Safe mode is OFF',
            'identity': {'namenode': {'clusterID': hdfs.CLUSTER, 'namespaceID': '123', 'cTime': '0'},
                         'datanode': {'clusterID': hdfs.CLUSTER, 'datanodeUuid': 'persistent-test-uuid'}}}

    def sample(self):
        base = 'hdfs://namenode:8020/hmdp-lab/test-1'
        answers = {batch: fixture.expected(batch) for batch in ('s2', 's3')}
        files = {batch: {'dwd/dwd_order_created/part.parquet': {'sha256': 'a' * 64, 'bytes': 128}}
                 for batch in answers}
        report = {'mode': 'build', 'completed': True, 'hdfs_root': base,
            'spark_version': '3.5.7', 'hadoop_client_version': '3.3.4', 'catalog_implementation': 'hive',
            'files': deepcopy(files), 'snapshots': {}, 'tables': {}, 'writeback': {}, 'pruning': {}}
        for batch, answer in answers.items():
            snapshot = {key: deepcopy(answer[key]) for key in ('created', 'paid', 'closed')}
            snapshot.update(created_orders=answer['counts']['orders'], all_events=answer['counts']['history'],
                partitions={'orders': sorted({'biz_date=' + row['biz_date'] for row in answer['created']}),
                            'events': ['event_date=' + day for day in answer['event_dates']]})
            report['snapshots'][batch] = snapshot
            report['tables'][batch] = {}
            for table, model in [('orders', 'dwd_order_created'), ('events', 'dwd_order_event')]:
                location = base + '/' + batch + '/dwd/' + model
                report['tables'][batch][table] = {'location': location, 'type': 'EXTERNAL', 'provider': 'hive',
                    'input_files': [location + '/part.parquet'], 'schema': 'struct<_snapshot_as_of:timestamp>'}
            totals = {}
            for row in answer['paid']:
                totals[row['biz_date']] = totals.get(row['biz_date'], 0) + row['orders']
            report['writeback'][batch] = {'location': base + '/derived/' + batch,
                'rows': [{'biz_date': day, 'paid_orders': total} for day, total in sorted(totals.items())]}
            report['pruning'][batch] = {'date_equality': True, 'selected_partition_dates': ['2026-10-30']}
        return report, files, answers

    def test_identifiers_cannot_escape_hdfs_or_inject_sql(self):
        self.assertEqual(hdfs.lab_path('test-1'), '/hmdp-lab/test-1')
        for tag in ('', '..', '../elsewhere', '/tmp', "x';DROP TABLE orders", 'a/b', 'a\\b', 'a' * 81):
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                hdfs.lab_path(tag)

    def test_fsck_requires_nonempty_healthy_blocks(self):
        healthy = 'Status: HEALTHY\nTotal files: 3\nTotal blocks (validated): 3\nMissing blocks: 0\nCorrupt blocks: 0'
        self.assertEqual(hdfs.validate_fsck(healthy), {'files': 3, 'blocks': 3})
        for text in (healthy.replace('HEALTHY', 'UNHEALTHY'), healthy.replace('files: 3', 'files: 0'),
                     healthy.replace('(validated): 3', '(validated): 0'),
                     healthy.replace('Missing blocks: 0', 'Missing blocks: 1'),
                     healthy.replace('Corrupt blocks: 0', 'Corrupt blocks: 2'), 'Status: HEALTHY'):
            with self.subTest(text=text), self.assertRaises(ValueError):
                hdfs.validate_fsck(text)

    def test_cluster_health_and_persistent_identity_are_required(self):
        before = self.status()
        self.assertTrue(validate_restart(before, deepcopy(before)))
        for mutation in ('live', 'safe', 'cluster', 'uuid', 'namespace', 'reformat'):
            after = deepcopy(before)
            if mutation == 'live':
                after['report'] = 'Live datanodes (0):'
            elif mutation == 'safe':
                after['safe_mode'] = 'Safe mode is ON'
            elif mutation == 'cluster':
                after['identity']['datanode']['clusterID'] = 'foreign'
            elif mutation == 'uuid':
                del after['identity']['datanode']['datanodeUuid']
            elif mutation == 'namespace':
                del after['identity']['namenode']['namespaceID']
            else:
                after['identity']['namenode']['namespaceID'] = 'new-namespace'
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                validate_restart(before, after)

    def test_counts_cannot_hide_wrong_amounts_or_event_partitions(self):
        report, files, answers = self.sample()
        self.assertTrue(validate_report(report, 'test-1', files, answers))
        for metric in ('created', 'paid'):
            changed = deepcopy(report)
            changed['snapshots']['s3'][metric][0]['known_cents'] += 1
            with self.subTest(metric=metric), self.assertRaises(ValueError):
                validate_report(changed, 'test-1', files, answers)
        report['snapshots']['s3']['partitions']['events'] = ['event_date=2026-10-30']
        with self.assertRaises(ValueError):
            validate_report(report, 'test-1', files, answers)

    def test_passed_flag_cannot_replace_hdfs_runtime_evidence(self):
        for mutation in ('hash', 'local', 'input', 'writeback', 'schema', 'timestamp_ntz', 'pruning', 'catalog', 'version', 'completed'):
            report, files, answers = self.sample()
            report['passed'] = True
            if mutation == 'hash':
                report['files']['s2']['dwd/dwd_order_created/part.parquet']['sha256'] = '0' * 64
            elif mutation == 'local':
                report['tables']['s2']['orders']['location'] = 'file:///workspace/target/fake'
            elif mutation == 'input':
                report['tables']['s2']['orders']['input_files'] = ['file:///workspace/target/fake.parquet']
            elif mutation == 'writeback':
                report['writeback']['s2']['rows'][0]['paid_orders'] += 1
            elif mutation == 'schema':
                report['tables']['s2']['orders']['schema'] = 'struct<_snapshot_as_of:string>'
            elif mutation == 'timestamp_ntz':
                report['tables']['s2']['orders']['schema'] = 'struct<_snapshot_as_of:timestamp_ntz>'
            elif mutation == 'pruning':
                report['pruning']['s2']['selected_partition_dates'].append('2026-10-29')
            elif mutation == 'catalog':
                report['catalog_implementation'] = 'in-memory'
            elif mutation == 'version':
                report['hadoop_client_version'] = 'unknown'
            else:
                del report['completed']
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                validate_report(report, 'test-1', files, answers)


if __name__ == '__main__':
    unittest.main()
