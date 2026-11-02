#!/usr/bin/env python3
"""Run the ordered fixture, late-event, replay and failed-publication acceptance suite."""
import argparse
from pathlib import Path
import sys

PLATFORM = Path(__file__).resolve().parents[1]
ROOT = PLATFORM.parent
sys.path.insert(0, str(PLATFORM))
sys.path.insert(0, str(PLATFORM / 'tools'))
from common import utc_now, write_json
from warehouse import inspect, spark_run
from verify_models import logical_signature, verify
import fixture


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tag', required=True, help='Unique suite tag for immutable runs')
    parser.add_argument('--run-root', type=Path, default=ROOT / 'target/warehouse-acceptance')
    args = parser.parse_args()
    fixture.generate()
    results = []
    for batch, day in [('s1', '2026-10-29'), ('s2', '2026-10-30'), ('s3', '2026-10-30')]:
        run_id = args.tag + '-' + batch
        code = spark_run(fixture.OUT / batch, args.run_root, batch, day, run_id)
        if code:
            raise RuntimeError('Warehouse acceptance run failed: ' + run_id)
        results.append(verify(args.run_root, day, batch, batch))
    first, _ = inspect(args.run_root, '2026-10-30')
    signature = logical_signature(first)
    if spark_run(fixture.OUT / 's3', args.run_root, 's3', '2026-10-30', args.tag + '-replay'):
        raise RuntimeError('Replay failed')
    replay, _ = inspect(args.run_root, '2026-10-30')
    if signature != logical_signature(replay):
        raise ValueError('Replay changed logical business results')
    before = (args.run_root / 'catalog/2026-10-30.json').read_bytes()
    if spark_run(fixture.OUT / 's2', args.run_root, 's2', '2026-10-30', args.tag + '-fault', True) == 0:
        raise ValueError('Expected injected failure did not fail')
    if before != (args.run_root / 'catalog/2026-10-30.json').read_bytes():
        raise ValueError('Failed run changed published catalog')
    version, _ = inspect(args.run_root, '2026-10-30')
    if signature != logical_signature(version):
        raise ValueError('Failed run damaged previous published result')
    report = {'passed': True, 'verified_at_utc': utc_now(), 'fixture_results': results,
        'replay_logical_results_equal': True, 'late_payment_old_date_corrected': results[1]['paid_orders'] == 5 and results[2]['paid_orders'] == 6,
        'failure_preserves_catalog_and_old_artifacts': True, 'failed_run_retained': args.tag + '-fault'}
    write_json(args.run_root / ('acceptance-' + args.tag + '.json'), report)
    print(report)


if __name__ == '__main__':
    main()
