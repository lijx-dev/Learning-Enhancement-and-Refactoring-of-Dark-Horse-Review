#!/usr/bin/env python3
"""Real Spark bridge acceptance; explicitly does not claim Airflow scheduler coverage."""
import argparse
import json
from pathlib import Path
import sys

PLATFORM = Path(__file__).resolve().parents[1]
ROOT = PLATFORM.parent
sys.path.insert(0, str(PLATFORM / 'orchestration'))
sys.path.insert(0, str(PLATFORM / 'tools'))
sys.path.insert(0, str(PLATFORM / 'tests'))
import bridge
import fixture
from common import safe_name, write_json, utc_now
from warehouse import inspect, spark_run
from verify_models import verify, logical_signature


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tag', required=True)
    args = parser.parse_args()
    safe_name(args.tag)
    root = ROOT / 'target/warehouse-bridge-acceptance' / args.tag
    root.mkdir(parents=True, exist_ok=False)
    bridge.QUEUE = root / 'queue'
    fixture.generate()
    run_root = root / 'runs'
    results = []

    def attempt(batch, day, run, number, fail=False):
        config = {'biz_date': day, 'source_batch': batch,
            'input_root': (fixture.OUT / batch).relative_to(ROOT).as_posix(),
            'run_root': run_root.relative_to(ROOT).as_posix(), 'demo_fail_first': fail}
        key = bridge.submit(config, args.tag + '-' + run, number)
        if not bridge.execute_request(bridge.QUEUE / (key + '.request.json'), spark_run):
            raise ValueError('Expected a new attempt')
        if bridge.execute_request(bridge.QUEUE / (key + '.request.json'), spark_run):
            raise ValueError('Duplicate was executed')
        if fail and number == 1:
            try:
                bridge.wait_result(key, config)
            except RuntimeError:
                return key
            raise ValueError('Injected failure was accepted')
        bridge.wait_result(key, config)
        results.append(verify(run_root, day, batch))
        return key

    attempt('s1', '2026-10-29', 'previous-day', 1)
    attempt('s2', '2026-10-30', 'initial', 1)
    pointer = run_root / 'catalog/2026-10-30.json'
    before = pointer.read_bytes()
    old, _ = inspect(run_root, '2026-10-30')
    signature = logical_signature(old)
    failed = attempt('s3', '2026-10-30', 'late-retry', 1, True)
    if pointer.read_bytes() != before or logical_signature(old) != signature:
        raise ValueError('Failed bridge attempt damaged previous publication')
    retried = attempt('s3', '2026-10-30', 'late-retry', 2, True)
    if failed == retried:
        raise ValueError('Retry reused failed version id')
    report = {'bridge_passed': True, 'airflow_scheduler_verified': False, 'verified_at_utc': utc_now(),
        'results': results, 'failed_attempt': failed, 'retry_attempt': retried,
        'failed_attempt_preserves_previous': True, 'duplicate_not_reexecuted': True,
        'late_payment_old_date_corrected': results[-2]['paid_orders'] == 5 and results[-1]['paid_orders'] == 6}
    write_json(root / 'report.json', report)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
