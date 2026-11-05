#!/usr/bin/env python3
"""Three manually defined orders: unknown paid amount, closed-only day, unprojected creation."""
import argparse
from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys

PLATFORM = Path(__file__).resolve().parents[1]
ROOT = PLATFORM.parent
sys.path.insert(0, str(PLATFORM))
sys.path.insert(0, str(PLATFORM / 'tools'))
from common import safe_name, sha256, write_json
import fixture
from verify_models import verify
from acceptance_hive import COMPOSE, validate_report
from warehouse import container_path, inspect, spark_run


def source(output):
    fixture.generate()
    tables = deepcopy(fixture.tables_for('s3'))
    tables = {table: tables[table] for table in fixture.read_json(PLATFORM / 'contracts/source-contract.json')['source_tables']}
    orders = tables['tb_voucher_order'][:3]
    # A: Oct 29 creation, Oct 30 payment with missing historical amount.
    # B: Oct 29 creation, Nov 1 closure; no creation or payment on closure day.
    # C: Oct 31 creation, still pending projection; Oct 31 is not an event partition.
    for row, shop, created, status, amount in zip(orders, ('1', '2', '1'),
            ('2026-10-29T01:00:00Z', '2026-10-29T02:00:00Z', '2026-10-31T01:00:00Z'),
            (2, 4, 1), (None, 100, 100)):
        row.update(shop_id_snapshot=shop, create_time=created, update_time=created,
                   status=status, pay_value_snapshot=amount, pay_time=None, expire_at=None)
    orders[0]['pay_time'] = '2026-10-30T01:00:00Z'
    events = []
    for eid, order, kind, day in [('81001', orders[0], 'ORDER_CREATED', '2026-10-29T01:00:00Z'),
            ('81002', orders[1], 'ORDER_CREATED', '2026-10-29T02:00:00Z'),
            ('81003', orders[0], 'ORDER_PAID', '2026-10-30T01:00:00Z'),
            ('81004', orders[1], 'ORDER_CLOSED', '2026-11-01T01:00:00Z')]:
        events.append(dict(event_id=eid, order_id=order['id'], event_type=kind,
                           occurred_at=day, processed_at=day))
    outbox = [dict(id=e['event_id'], kind=e['event_type'], aggregate_id=e['order_id'],
        user_id='101', state='DONE', created_at=e['occurred_at'], consumed_at=e['processed_at']) for e in events]
    outbox.append(dict(id='81005', kind='ORDER_CREATED', aggregate_id=orders[2]['id'], user_id='101',
        state='PENDING', created_at=orders[2]['create_time'], consumed_at=None))
    tables.update(tb_voucher_order=orders, tb_order_event_history=events, tb_business_outbox=outbox)
    output.mkdir()
    files = {}
    for table, rows in tables.items():
        path = output / (table + '.jsonl')
        path.write_text(''.join(json.dumps(row, ensure_ascii=False) + '\n' for row in rows), encoding='utf-8')
        files[path.name] = {'rows': len(rows), 'sha256': sha256(path)}
    write_json(output / 'manifest.json', {'fixture_id': 'hmdp_hand_v1', 'batch_id': 'edges',
        'synthetic': True, 'source_env': 'synthetic_local_demo', 'payment': 'mock_only',
        'scenario_as_of': '2026-11-02T00:00:00Z', 'timestamp_timezone': 'UTC',
        'business_timezone': 'Asia/Shanghai', 'files': files})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tag', required=True)
    args = parser.parse_args()
    safe_name(args.tag)
    output = ROOT / 'target/warehouse-model-edges' / args.tag
    output.mkdir(parents=True, exist_ok=False)
    source(output / 'source')
    report = {'passed': False, 'runs': [], 'hive': {}}
    try:
        for day in ('2026-10-30', '2026-10-31', '2026-11-01'):
            run_id = args.tag + '-' + day
            if spark_run(output / 'source', output / 'runs', 'edges', day, run_id, cache_policy='memory-and-disk'):
                raise RuntimeError('Edge fixture Spark run failed')
            report['runs'].append(verify(output / 'runs', day, 'edges', 'edges'))
        version, _ = inspect(output / 'runs', '2026-10-30')
        before = {p.relative_to(version).as_posix(): sha256(p) for p in version.rglob('*') if p.is_file()}
        for mode in ('build', 'reopen'):
            command = COMPOSE + ['run', '--rm', '--no-deps', 'spark', '--master', 'local[2]', '--driver-memory', '768m',
                '/workspace/dev/local/hive_smoke.py', '--version', container_path(version),
                '--output', container_path(output / 'catalog'), '--biz-date', '2026-10-30', '--mode', mode]
            if mode == 'build':
                command += ['--previous-version', container_path(version)]
            with (output / (mode + '.log')).open('w', encoding='utf-8') as stream:
                if subprocess.run(command, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT).returncode:
                    raise RuntimeError('Edge fixture Hive ' + mode + ' failed')
            result = fixture.read_json(output / 'catalog' / (mode + '-report.json'))
            expected = {'after': fixture.expected('edges')}
            if mode == 'build':
                expected['before'] = fixture.expected('edges')
            validate_report(result, expected)
            report['hive'][mode] = result
        if report['hive']['build']['snapshots']['after'] != report['hive']['reopen']['snapshots']['after']:
            raise ValueError('Reopened edge catalog differs')
        if before != {p.relative_to(version).as_posix(): sha256(p) for p in version.rglob('*') if p.is_file()}:
            raise ValueError('Hive changed published source files')
        report['passed'] = True
    finally:
        write_json(output / 'report.json', report)
    print(json.dumps({'passed': True, 'report': str(output / 'report.json')}, ensure_ascii=False))


if __name__ == '__main__':
    main()
