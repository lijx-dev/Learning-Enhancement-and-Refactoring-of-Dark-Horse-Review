#!/usr/bin/env python3
"""Verify isolated Hive external tables against hand answers and PyArrow source reads."""
import argparse
import json
from pathlib import Path
import subprocess
import sys

PLATFORM = Path(__file__).resolve().parents[1]
ROOT = PLATFORM.parent
sys.path.insert(0, str(PLATFORM))
sys.path.insert(0, str(PLATFORM / 'tools'))
from common import safe_name, sha256, utc_now, write_json
from warehouse import container_path, inspect

COMPOSE = ['docker', '--context', 'desktop-linux', 'compose', '-f', 'dev/local/compose.yml']


def normalized(rows):
    return sorted(json.dumps({k: str(v) if v is not None else None for k, v in row.items()}, sort_keys=True) for row in rows)


def validate_report(report, expected):
    if report.get('passed') is not True or report.get('catalog_implementation') != 'hive':
        raise ValueError('Real Hive catalog did not pass')
    if report.get('mode') not in ('build', 'reopen'):
        raise ValueError('Unknown catalog test mode')
    checks = report.get('checks', {})
    required = {'partition_pruning_observed', 'orders_external_hive_serde', 'events_external_hive_serde'}
    if report.get('mode') == 'build':
        required |= {'external_partition_files_preserved', 'partition_removed_from_catalog', 'repair_restored_results'}
    if not required.issubset(checks) or not all(checks.values()):
        raise ValueError('Missing or failed catalog checks')
    for phase, answer in expected.items():
        actual = report['snapshots'][phase]
        if actual['created_orders'] != answer['counts']['orders'] or actual['all_events'] != answer['counts']['history']:
            raise ValueError('Table counts mismatch: ' + phase)
        for metric in ('created', 'paid', 'closed'):
            if normalized(actual[metric]) != normalized(answer[metric]):
                raise ValueError('Independent hand answer mismatch: ' + phase + '/' + metric)
        for table, field in [('orders', 'biz_date'), ('events', 'event_date')]:
            wanted = sorted({field + '=' + row['biz_date'] for row in answer['created' if table == 'orders' else 'closed']})
            if actual['partitions'][table] != wanted:
                raise ValueError('Partition keys mismatch')
    return True


def verified_version(path):
    path = Path(path).resolve()
    if not path.is_relative_to(ROOT / 'target') or path.parent.parent.name != 'versions':
        raise ValueError('Only published versions under project target are permitted')
    record = json.loads((path / 'run_manifest.json').read_text(encoding='utf-8'))
    if record['run_id'] != path.name or record['biz_date'] != path.parent.name:
        raise ValueError('Published version path and lineage differ')
    if record['quality']['passed'] is not True:
        raise ValueError('Source quality gate failed')
    for name, checksum in record['artifacts'].items():
        file = (path / name).resolve()
        if not file.is_relative_to(path) or sha256(file) != checksum:
            raise ValueError('Source artifact corrupt: ' + name)
    return record


def main():
    import fixture
    from verify_models import records
    from decimal import Decimal
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-root', type=Path, default=ROOT / 'target/warehouse-acceptance')
    parser.add_argument('--previous-version', type=Path, required=True)
    parser.add_argument('--biz-date', default='2026-10-30')
    parser.add_argument('--tag', required=True)
    args = parser.parse_args()
    safe_name(args.tag)
    if args.biz_date != '2026-10-30':
        raise ValueError('This hand-calculable suite verifies 2026-10-30')
    current, _ = inspect(args.run_root, args.biz_date)
    previous = args.previous_version.resolve()
    manifests = [verified_version(p) for p in (previous, current)]
    if [m['source_batch'] for m in manifests] != ['s2', 's3']:
        raise ValueError('This suite requires published fixture s2 then s3')
    for record in manifests:
        if not record.get('synthetic') or record['source_env'] != 'synthetic_local_demo':
            raise ValueError('Only synthetic hand fixtures are supported')
    output = ROOT / 'target/warehouse-hive-acceptance' / args.tag
    output.mkdir(parents=True, exist_ok=False)
    before_hash = [sha256(p / 'run_manifest.json') for p in (previous, current)]
    arrow = {}
    for version, batch in [(previous, 's2'), (current, 's3')]:
        orders = records(version, 'dwd_order_created', 'dwd')
        events = records(version, 'dwd_order_event', 'dwd')
        amounts = {row['order_id']: row['pay_cents'] for row in orders}
        selected = [e for e in events if str(e['event_date']) == args.biz_date and e['event_type'] == 'ORDER_PAID']
        arrow[batch] = {'paid_orders': len(selected), 'paid_cents': str(sum((amounts[e['order_id']] or Decimal(0) for e in selected), Decimal(0)))}
    report = {'passed': False, 'pyarrow': arrow, 'source_manifest_sha256': before_hash,
        'source_run_ids': [m['run_id'] for m in manifests],
        'code_sha256': {name: sha256(ROOT / name) for name in
            ('dev/local/hive_smoke.py', 'data-platform/tests/acceptance_hive.py')}}
    try:
        for mode in ('build', 'reopen'):
            command = COMPOSE + ['run', '--rm', '--no-deps', 'spark', '--master', 'local[2]', '--driver-memory', '768m',
                '/workspace/dev/local/hive_smoke.py', '--version', container_path(current),
                '--previous-version', container_path(previous), '--output', container_path(output / 'catalog'),
                '--biz-date', args.biz_date, '--mode', mode]
            log = output / (mode + '-spark.log')
            with log.open('w', encoding='utf-8') as stream:
                code = subprocess.run(command, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT).returncode
            if code:
                raise RuntimeError('Hive process failed: ' + str(log))
            result = json.loads((output / 'catalog' / (mode + '-report.json')).read_text(encoding='utf-8'))
            expected = {'after': fixture.expected('s3')}
            if mode == 'build':
                expected['before'] = fixture.expected('s2')
            validate_report(result, expected)
            report[mode] = result
        if report['build']['snapshots']['after'] != report['reopen']['snapshots']['after']:
            raise ValueError('Catalog did not persist across Spark processes')
        for path, original in zip((previous, current), before_hash):
            verified_version(path)
            if sha256(path / 'run_manifest.json') != original:
                raise ValueError('Published source manifest changed')
        if arrow != {'s2': {'paid_orders': 5, 'paid_cents': '15000'}, 's3': {'paid_orders': 6, 'paid_cents': '16000'}}:
            raise ValueError('Independent PyArrow result differs from hand answer')
        report['evidence_sha256'] = {p.relative_to(output).as_posix(): sha256(p)
            for p in output.rglob('*') if p.is_file() and p.suffix in ('.log', '.txt', '.json')
            and p.parent in (output, output / 'catalog')}
        report.update(passed=True, persisted_across_processes=True, source_artifacts_unchanged=True, verified_at_utc=utc_now())
        write_json(output / 'acceptance.json', report)
        print(json.dumps({'passed': True, 'pyarrow': arrow, 'persisted_across_processes': True, 'report': str(output / 'acceptance.json')}, ensure_ascii=False), flush=True)
    except BaseException as failure:
        report['error'] = str(failure)
        write_json(output / 'failure.json', report)
        raise


if __name__ == '__main__':
    main()
