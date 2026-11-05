#!/usr/bin/env python3
"""End-to-end acceptance against a running local Airflow 2.11 scheduler and Spark."""
import argparse
import json
from pathlib import Path
import hashlib
import subprocess
import sys
import time

PLATFORM = Path(__file__).resolve().parents[1]
ROOT = PLATFORM.parent
sys.path.insert(0, str(PLATFORM / 'orchestration'))
sys.path.insert(0, str(PLATFORM / 'tools'))
sys.path.insert(0, str(PLATFORM / 'tests'))
import bridge
import fixture
from common import safe_name, sha256, utc_now, write_json
from verify_models import logical_signature, verify
from warehouse import inspect

COMPOSE = ['docker', '--context', 'desktop-linux', 'compose', '-f', 'dev/local/airflow.compose.yml']
DAG = 'hmdp_offline_daily'


def capture(command):
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True,
        encoding='utf-8', errors='replace', check=True)
    return result.stdout.strip()


def metadata(run_id):
    """Read-only local acceptance observation, never update Airflow metadata directly."""
    # Inspect SQLite inside the Linux container. Windows host readers can interfere
    # with hot-journal recovery/locking on the bind-mounted database.
    code = ("import sqlite3,json,sys; "
        "c=sqlite3.connect('file:/opt/airflow/state/airflow.db?mode=ro',uri=True,timeout=10); "
        "r=c.execute('SELECT state FROM dag_run WHERE dag_id=? AND run_id=?',(sys.argv[1],sys.argv[2])).fetchone(); "
        "t=c.execute('SELECT state,try_number,max_tries FROM task_instance WHERE dag_id=? AND run_id=? AND task_id=?',"
        "(sys.argv[1],sys.argv[2],'build_verify_publish')).fetchone(); "
        "f=c.execute('SELECT COUNT(*) FROM task_fail WHERE dag_id=? AND run_id=? AND task_id=?',"
        "(sys.argv[1],sys.argv[2],'build_verify_publish')).fetchone()[0]; "
        "print(json.dumps({'dag_state':r[0] if r else None,'task_state':t[0] if t else None,"
        "'try_number':t[1] if t else None,'max_tries':t[2] if t else None,'failed_attempts':f})); c.close()")
    return json.loads(capture(COMPOSE + ['exec', '-T', 'scheduler', 'python', '-c', code, DAG, run_id]))


def validate_retry_evidence(snapshot, failed, previous_pointer_hash):
    """Use persisted failures/attempt results; observing transient up_for_retry is optional."""
    if snapshot['try_number'] != 2 or snapshot['failed_attempts'] != 1 or failed.get('passed') is not False:
        raise ValueError('Automatic retry lacks a durable first failure and second attempt')
    if (failed.get('catalog_before_sha256') != previous_pointer_hash
            or failed.get('catalog_after_sha256') != previous_pointer_hash):
        raise ValueError('Failed attempt did not preserve previous catalog')
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tag', required=True, help='New tag; never reuse an existing acceptance run')
    args = parser.parse_args()
    safe_name(args.tag)
    if len(args.tag) > 60:
        raise ValueError('Use a tag no longer than 60 characters')
    output = ROOT / 'target/warehouse-airflow-acceptance' / args.tag
    output.mkdir(parents=True, exist_ok=False)
    report = {'passed': False, 'airflow_scheduler_verified': False, 'started_at_utc': utc_now(),
        'dag_id': DAG, 'runs': [], 'observations': [], 'evidence': {}}
    worker = None
    worker_log = None
    try:
        # Do not pick up someone else's pending work during a bounded test.
        if (bridge.QUEUE / '.locks/host-worker.json').exists():
            raise RuntimeError('A host worker/retained lock already exists; inspect before testing')
        for request in bridge.QUEUE.glob('*.request.json'):
            key = request.name.removesuffix('.request.json')
            if not (bridge.QUEUE / (key + '.result.json')).exists():
                raise RuntimeError('An unfinished request already exists: ' + key)
        report['image'] = json.loads(capture(['docker', '--context', 'desktop-linux', 'image', 'inspect',
            'apache/airflow:2.11.0-python3.11', '--format', '{{json .}}']))
        report['image'] = {key: report['image'][key] for key in ('Id', 'RepoDigests', 'Os', 'Architecture')}
        if (report['image']['Os'], report['image']['Architecture']) != ('linux', 'amd64'):
            raise ValueError('Expected a linux/amd64 image')
        version = capture(COMPOSE + ['exec', '-T', 'scheduler', 'airflow', 'version'])
        report['airflow_version'] = version
        if version.splitlines()[-1] != '2.11.0':
            raise ValueError('Expected Airflow 2.11.0')
        imports = capture(COMPOSE + ['exec', '-T', 'scheduler', 'airflow', 'dags', 'list-import-errors', '-o', 'json'])
        report['import_errors'] = json.loads(imports)
        if report['import_errors']:
            raise ValueError('DAG import failed')
        dags = json.loads(capture(COMPOSE + ['exec', '-T', 'scheduler', 'airflow', 'dags', 'list', '-o', 'json']))
        report['dag_loaded'] = any(row['dag_id'] == DAG and str(row['is_paused']).lower() == 'false' for row in dags)
        if not report['dag_loaded']:
            raise ValueError('DAG missing or paused')
        report['code_sha256'] = {name: sha256(ROOT / name) for name in [
            'data-platform/orchestration/bridge.py', 'data-platform/orchestration/dags/hmdp_offline.py',
            'data-platform/tools/airflow_control.py', 'dev/local/airflow.compose.yml']}
        fixture.generate()
        run_root = output / 'runs'
        worker_log = (output / 'host-worker.log').open('w', encoding='utf-8')
        worker = subprocess.Popen([sys.executable, '-X', 'utf8', 'data-platform/tools/orchestrate.py',
            '--max-jobs', '4', '--idle-timeout', '180'], cwd=ROOT, stdout=worker_log, stderr=subprocess.STDOUT)
        report['worker_pid'] = worker.pid

        def trigger(batch, day, suffix, retry=False, prior=None):
            run_id = args.tag + '-' + suffix
            config = {'biz_date': day, 'source_batch': batch,
                'input_root': (fixture.OUT / batch).relative_to(ROOT).as_posix(),
                'run_root': run_root.relative_to(ROOT).as_posix(),
                'cache_policy': 'memory-and-disk', 'demo_fail_first': retry}
            config_path = output / (suffix + '-conf.json')
            write_json(config_path, config)
            trigger_output = capture([sys.executable, '-X', 'utf8', 'data-platform/tools/airflow_control.py',
                'trigger', '--config', str(config_path), '--run-id', run_id])
            (output / (suffix + '-trigger.log')).write_text(trigger_output, encoding='utf-8')
            started, last = time.monotonic(), None
            observed_retry, preserved = False, False
            failed_key = bridge.attempt_id(run_id, 1)
            while time.monotonic() - started < 1100:
                snapshot = metadata(run_id)
                if snapshot != last:
                    report['observations'].append({'run_id': run_id, 'at_utc': utc_now(), **snapshot})
                    print(json.dumps({'run_id': run_id, **snapshot}), flush=True)
                    last = snapshot
                    write_json(output / 'progress.json', report)
                observed_retry |= snapshot['task_state'] == 'up_for_retry'
                failure_result = bridge.QUEUE / (failed_key + '.result.json')
                if snapshot['dag_state'] == 'failed':
                    raise RuntimeError('Airflow marked DAG run failed: ' + run_id)
                if snapshot['dag_state'] == 'success':
                    if snapshot['task_state'] != 'success' or snapshot['try_number'] != (2 if retry else 1):
                        raise ValueError('Unexpected Airflow task attempt count/state')
                    if retry:
                        failed = json.loads(failure_result.read_text(encoding='utf-8'))
                        pointer, old_version, signature = prior
                        preserved = validate_retry_evidence(snapshot, failed, hashlib.sha256(pointer).hexdigest())
                        if logical_signature(old_version) != signature:
                            raise ValueError('Failed/retried attempt damaged previous immutable artifacts')
                    key = bridge.attempt_id(run_id, snapshot['try_number'])
                    result = bridge.wait_result(key, config, timeout_seconds=1)
                    verified = verify(run_root, day, batch, batch)
                    if verified['run_id'] != key:
                        raise ValueError('Airflow run and published version identity differ')
                    record = {'airflow_run_id': run_id, 'task': snapshot, 'worker_attempt': key,
                        'verification': verified, 'up_for_retry_observed': observed_retry,
                        'retry_verified_from_persisted_evidence': preserved if retry else None,
                        'failed_attempt_preserves_previous': preserved if retry else None,
                        'published_manifest_sha256': result['manifest_sha256']}
                    report['runs'].append(record)
                    return record
                if worker.poll() not in (None, 0):
                    raise RuntimeError('Host worker exited unexpectedly; inspect host-worker.log')
                time.sleep(1)
            raise TimeoutError('Airflow run did not complete; inspect Airflow and host-worker logs')

        trigger('s1', '2026-10-29', 'previous-day')
        trigger('s2', '2026-10-30', 'initial')
        pointer = (run_root / 'catalog/2026-10-30.json').read_bytes()
        old, _ = inspect(run_root, '2026-10-30')
        trigger('s3', '2026-10-30', 'late-retry', True, (pointer, old, logical_signature(old)))
        if worker.wait(timeout=30) != 0:
            raise RuntimeError('Host worker did not finish successfully')
        report['late_payment_old_date_corrected'] = (report['runs'][1]['verification']['paid_orders'],
            report['runs'][2]['verification']['paid_orders']) == (5, 6)
        if not report['late_payment_old_date_corrected']:
            raise ValueError('Late correction missing')
        report['scheduler_logs'] = capture(COMPOSE + ['logs', '--no-color', 'scheduler'])
        (output / 'scheduler.log').write_text(report.pop('scheduler_logs'), encoding='utf-8')
        for name in ('host-worker.log', 'scheduler.log'):
            if name == 'host-worker.log':
                worker_log.flush()
            report['evidence'][name] = sha256(output / name)
        report.update(passed=True, airflow_scheduler_verified=True, completed_at_utc=utc_now())
        write_json(output / 'report.json', report)
        print('PASSED ' + str(output / 'report.json'), flush=True)
    except BaseException as failure:
        report.update(failure=str(failure), failed_at_utc=utc_now())
        write_json(output / 'failure.json', report)
        # Do not kill a worker that may still be publishing a Spark version.
        if worker and worker.poll() is None:
            print(f'Worker PID {worker.pid} may still be running; inspect it before removing any lock.', flush=True)
        raise
    finally:
        if worker_log:
            worker_log.close()


if __name__ == '__main__':
    main()
