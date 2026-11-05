#!/usr/bin/env python3
"""Real HDFS read/write, Hive and restart acceptance, isolated from the production catalog."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'data-platform'))
sys.path.insert(0, str(ROOT / 'data-platform/tools'))
from common import sha256, utc_now, write_json
from warehouse import container_path, inspect
import fixture
import hdfs_control as hdfs
from acceptance_hive import normalized, verified_version


def expected_files(path, manifest):
    result = {name: {'sha256': checksum, 'bytes': (path / name).stat().st_size}
        for name, checksum in manifest['artifacts'].items()
        if any(name.startswith('dwd/' + model + '/') for model in ('dwd_order_created', 'dwd_order_event'))}
    if not result or sum(item['bytes'] for item in result.values()) > 64 * 1024 * 1024:
        raise ValueError('Only small nonempty hand fixtures are supported')
    return result


def validate_report(report, tag, files, answers):
    base = 'hdfs://namenode:8020' + hdfs.lab_path(tag)
    if (report.get('completed') is not True or report.get('mode') not in ('build', 'reopen')
            or report.get('hdfs_root') != base or report.get('catalog_implementation') != 'hive'
            or report.get('spark_version') != '3.5.7' or report.get('hadoop_client_version') != '3.3.4'):
        raise ValueError('Missing real HDFS/Hive runtime evidence or unexpected client version')
    if report.get('files') != files:
        raise ValueError('Remote file hashes/sizes differ from independently verified sources')
    for batch, answer in answers.items():
        actual = report['snapshots'][batch]
        if actual['created_orders'] != answer['counts']['orders'] or actual['all_events'] != answer['counts']['history']:
            raise ValueError('DWD counts mismatch')
        for metric in ('created', 'paid', 'closed'):
            if normalized(actual[metric]) != normalized(answer[metric]):
                raise ValueError('Hand answer mismatch: ' + batch + '/' + metric)
        for table, model, key, dates in (
                ('orders', 'dwd_order_created', 'biz_date', {r['biz_date'] for r in answer['created']}),
                ('events', 'dwd_order_event', 'event_date', set(answer['event_dates']))):
            if actual['partitions'][table] != sorted(key + '=' + day for day in dates):
                raise ValueError('Partition dates differ from independent answer')
            detail = report['tables'][batch][table]
            location = base + '/' + batch + '/dwd/' + model
            if detail['location'].rstrip('/') != location or detail['type'] != 'EXTERNAL' or detail['provider'] != 'hive':
                raise ValueError('Expected Hive SerDe external table on HDFS')
            if not re.search(r'\b_snapshot_as_of:timestamp(?=[,>])', detail['schema']):
                raise ValueError('Snapshot must retain timestamp type')
            names = detail['input_files']
            if not names or not all(name.startswith(location + '/') for name in names):
                raise ValueError('Read must use HDFS files, never local fallback')
        evidence = report['pruning'][batch]
        if evidence.get('date_equality') is not True or evidence.get('selected_partition_dates') != ['2026-10-30']:
            raise ValueError('Partition pruning evidence missing')
        paid = {}
        for row in answer['paid']:
            paid[row['biz_date']] = paid.get(row['biz_date'], 0) + row['orders']
        wanted = [{'biz_date': day, 'paid_orders': count} for day, count in sorted(paid.items())]
        writeback = report['writeback'][batch]
        if writeback['location'] != base + '/derived/' + batch or normalized(writeback['rows']) != normalized(wanted):
            raise ValueError('HDFS Parquet write/read differs from hand answer')
    return True


def validate_restart(before, after):
    hdfs.validate_status(before)
    hdfs.validate_status(after)
    if before['identity'] != after['identity']:
        raise ValueError('Persistent HDFS identities changed after restart')
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-root', type=Path, default=ROOT / 'target/warehouse-hdfs-sources')
    parser.add_argument('--previous-version', type=Path, required=True)
    parser.add_argument('--tag', required=True)
    args = parser.parse_args()
    hdfs.lab_path(args.tag)
    current, _ = inspect(args.run_root, '2026-10-30')
    paths = {'s2': args.previous_version.resolve(), 's3': current}
    manifests = {batch: verified_version(path) for batch, path in paths.items()}
    for batch, manifest in manifests.items():
        if (manifest['source_batch'] != batch or manifest.get('synthetic') is not True
                or manifest['source_env'] != 'synthetic_local_demo'
                or manifest.get('model_contract_version') != 'hmdp-model-v2'):
            raise ValueError('Requires synthetic v2 s2/s3 published sources')
    output = ROOT / 'target/warehouse-hdfs-acceptance' / args.tag
    output.mkdir(parents=True, exist_ok=False)
    hashes = {batch: sha256(path / 'run_manifest.json') for batch, path in paths.items()}
    files = {batch: expected_files(path, manifests[batch]) for batch, path in paths.items()}
    write_json(output / 'inputs.json', {'tag': args.tag, 'sources': {batch: {
        'path': container_path(path), 'manifest_sha256': hashes[batch]} for batch, path in paths.items()}})
    answers = {batch: fixture.expected(batch) for batch in paths}
    report = {'passed': False, 'source_manifest_sha256': hashes,
        'source_run_ids': {batch: manifest['run_id'] for batch, manifest in manifests.items()},
        'code_sha256': {name: sha256(ROOT / name) for name in (
            'dev/local/hdfs_smoke.py', 'dev/local/hdfs.compose.yml', 'dev/local/hdfs/entrypoint.sh',
            'dev/local/hdfs/core-site.xml', 'dev/local/hdfs/hdfs-site.xml',
            'data-platform/tools/hdfs_control.py', 'data-platform/tests/acceptance_hdfs.py')}}
    try:
        report['image'] = hdfs.require_image()
        report['server_version'] = hdfs.dfs('version')
        if not report['server_version'].startswith('Hadoop 3.4.2'):
            raise ValueError('Unexpected real HDFS server version')
        report['before'] = hdfs.status()
        report['volumes'] = json.loads(hdfs.capture(['docker', '--context', 'desktop-linux', 'volume',
            'inspect', 'hmdp-hdfs_namenode-data', 'hmdp-hdfs_datanode-data']))
        for mode in ('build', 'reopen'):
            container = 'hmdp-hdfs-' + args.tag + '-' + mode
            command = hdfs.COMPOSE + ['run', '--name', container, '--rm', '--no-deps', '--pull', 'never', 'spark',
                '--master', 'local[2]', '--driver-memory', '768m', '/workspace/dev/local/hdfs_smoke.py',
                '--inputs', container_path(output / 'inputs.json'), '--output', container_path(output / 'catalog'), '--mode', mode]
            with (output / (mode + '-spark.log')).open('w', encoding='utf-8') as stream:
                try:
                    process = subprocess.run(command, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT, timeout=900)
                except (subprocess.TimeoutExpired, KeyboardInterrupt):
                    # Terminating the compose client alone can leave its container running.
                    hdfs.capture(['docker', '--context', 'desktop-linux', 'stop', '--time', '10', container])
                    raise
            if process.returncode:
                raise RuntimeError('Spark failed; inspect ' + str(output / (mode + '-spark.log')))
            result = json.loads((output / 'catalog' / (mode + '-report.json')).read_text(encoding='utf-8'))
            validate_report(result, args.tag, files, answers)
            report[mode] = result
            fsck = hdfs.dfs('fsck', hdfs.lab_path(args.tag), '-files', '-blocks', '-locations')
            (output / (mode + '-fsck.txt')).write_text(fsck + '\n', encoding='utf-8')
            report[mode + '_fsck'] = hdfs.validate_fsck(fsck)
            if mode == 'build':
                # Explicit init on an existing volume must preserve its metadata.
                report['repeat_init'] = hdfs.capture(hdfs.COMPOSE + ['run', '--rm', '--no-deps',
                    '--pull', 'never', 'namenode', 'init'])
                if 'no format performed' not in report['repeat_init']:
                    raise ValueError('Repeated init did not prove metadata preservation')
                hdfs.capture([sys.executable, 'data-platform/tools/hdfs_control.py', 'restart'], timeout=360)
                report['after_restart'] = hdfs.status()
                validate_restart(report['before'], report['after_restart'])
        for key in ('snapshots', 'files', 'tables', 'writeback', 'pruning'):
            if report['build'][key] != report['reopen'][key]:
                raise ValueError('HDFS/Hive state changed after restart: ' + key)
        for batch, path in paths.items():
            verified_version(path)
            if sha256(path / 'run_manifest.json') != hashes[batch]:
                raise ValueError('Local published manifest changed')
        report.update(passed=True, persisted_across_restart=True, source_artifacts_unchanged=True, verified_at_utc=utc_now())
    except BaseException as failure:
        report['error'] = str(failure)
        raise
    finally:
        try:
            (output / 'hdfs.log').write_text(hdfs.capture(hdfs.COMPOSE + ['logs', '--tail', '200', 'namenode', 'datanode']), encoding='utf-8')
        except Exception as failure:
            report['log_capture_error'] = str(failure)
        report['evidence_sha256'] = {p.relative_to(output).as_posix(): sha256(p)
            for p in output.rglob('*') if p.is_file() and p.suffix in ('.log', '.txt', '.json')
            and p.parent in (output, output / 'catalog') and p.name not in ('acceptance.json', 'failure.json')}
        write_json(output / ('acceptance.json' if report['passed'] else 'failure.json'), report)
    print(json.dumps({'passed': True, 'report': str(output / 'acceptance.json')}, ensure_ascii=False))


if __name__ == '__main__':
    main()
