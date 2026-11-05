#!/usr/bin/env python3
"""Spark regressions using a real transactional Java payment export and controlled time faults."""
import argparse
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys

PLATFORM = Path(__file__).resolve().parents[1]
ROOT = PLATFORM.parent
sys.path.insert(0, str(PLATFORM))
sys.path.insert(0, str(PLATFORM / 'tools'))
from common import safe_name, sha256, write_json
from warehouse import inspect, spark_run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=ROOT / 'target/warehouse-payment-clock/source.json')
    parser.add_argument('--tag', required=True)
    args = parser.parse_args()
    safe_name(args.tag)
    original = json.loads(args.source.read_text(encoding='utf-8'))
    output = ROOT / 'target/warehouse-payment-clock' / args.tag
    output.mkdir(parents=True, exist_ok=False)
    report = {'passed': False, 'java_evidence_sha256': sha256(args.source),
              'jvm_timezone': original['jvm_timezone'], 'cases': {}}
    run_root = output / 'runs'
    try:
        cases = [('real', True, False), ('offset', True, False), ('warning', True, True),
                 ('minute', False, False), ('eight-hours', False, False),
                 ('midnight', False, False), ('missing', False, False)]
        for name, passed, warning in cases:
            tables = deepcopy(original['source_tables'])
            order = tables['tb_voucher_order'][0]
            event = tables['tb_order_event_history'][0]
            paid = datetime.fromisoformat(order['pay_time'].replace('Z', '+00:00'))
            if name == 'offset':
                event['occurred_at'] = paid.astimezone(timezone(timedelta(hours=8))).isoformat()
            elif name in ('warning', 'minute', 'eight-hours'):
                seconds = {'warning': 2, 'minute': 60, 'eight-hours': 28800}[name]
                # Use noon, keeping these threshold cases separate from day-boundary behavior.
                paid = paid.replace(hour=4, minute=0, second=0, microsecond=0)
                order['pay_time'] = paid.isoformat()
                event['occurred_at'] = (paid + timedelta(seconds=seconds)).isoformat()
            elif name == 'midnight':
                paid = paid.replace(hour=15, minute=59, second=59, microsecond=0)
                order['pay_time'] = paid.isoformat()
                event['occurred_at'] = (paid + timedelta(seconds=2)).isoformat()
            elif name == 'missing':
                order['pay_time'] = None
            source = output / name
            source.mkdir()
            files = {}
            for table, rows in tables.items():
                path = source / (table + '.jsonl')
                path.write_text(''.join(json.dumps(row, ensure_ascii=False) + '\n' for row in rows), encoding='utf-8')
                files[path.name] = {'rows': len(rows), 'sha256': sha256(path)}
            write_json(source / 'manifest.json', {'source_batch': name, 'snapshot_version': 'hmdp-offline-v1',
                'synthetic': True, 'source_env': 'synthetic_local_demo', 'payment_provenance': 'mock_only',
                'snapshot_semantics': 'transactional Java integration-test export, not a committed historical snapshot',
                'timestamp_storage': 'UTC_ISO8601', 'business_timezone': 'Asia/Shanghai',
                'snapshot_as_of': original['scenario_as_of'], 'source_tables': files})
            day = paid.astimezone(timezone(timedelta(hours=8))).date().isoformat()
            pointer = run_root / 'catalog' / (day + '.json')
            before = pointer.read_bytes() if pointer.exists() else None
            run_id = args.tag + '-' + name
            code = spark_run(source, run_root, name, day, run_id, cache_policy='memory-and-disk')
            if (code == 0) != passed:
                raise AssertionError('Unexpected publication outcome: ' + name)
            if passed:
                _, manifest = inspect(run_root, day)
                quality = manifest['quality']
                if bool(quality['payment_time']['warning']) != warning:
                    raise AssertionError('Unexpected warning: ' + name)
            else:
                if (pointer.read_bytes() if pointer.exists() else None) != before:
                    raise AssertionError('Failed time gate changed catalog: ' + name)
                quality = json.loads((run_root / '.staging' / day / run_id / 'quality.json').read_text(encoding='utf-8'))
                failed = {key for key, ok in quality['checks'].items() if not ok}
                expected = 'paid_time_present' if name == 'missing' else 'paid_time_consistent'
                if failed != {expected}:
                    raise AssertionError('Unexpected failed checks: ' + str(failed))
            report['cases'][name] = {'expected_publication': passed, 'quality': quality['payment_time']}
        report['passed'] = True
    finally:
        write_json(output / 'report.json', report)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
