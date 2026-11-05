"""Local filesystem bridge: validated requests, immutable attempts, no shell commands."""
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import time

PLATFORM = Path(__file__).resolve().parents[1]
ROOT = PLATFORM.parent
sys.path.insert(0, str(PLATFORM))
from common import exclusive_lock, safe_name, sha256, utc_now, validate_snapshot, write_json

QUEUE = ROOT / 'target/warehouse-orchestration/queue'


def project_path(value):
    path = Path(value)
    if path.is_absolute() or '..' in path.parts or '\\' in value:
        raise ValueError('Use project-relative forward-slash paths')
    resolved = (ROOT / path).resolve()
    if not resolved.is_relative_to((ROOT / 'target').resolve()):
        raise ValueError('Only project target paths are permitted')
    return resolved


def validate_config(config):
    allowed = {'biz_date', 'source_batch', 'input_root', 'run_root', 'cache_policy', 'demo_fail_first'}
    if not isinstance(config, dict) or set(config) - allowed:
        raise ValueError('Unsupported DAG configuration keys')
    for key in ('biz_date', 'source_batch', 'input_root', 'run_root'):
        if not isinstance(config.get(key), str):
            raise ValueError('Missing string configuration: ' + key)
    if date.fromisoformat(config['biz_date']).isoformat() != config['biz_date']:
        raise ValueError('biz_date must be YYYY-MM-DD')
    safe_name(config['source_batch'])
    source, dest = project_path(config['input_root']), project_path(config['run_root'])
    if source == dest or source in dest.parents or dest in source.parents:
        raise ValueError('Source and output roots must be separate')
    if config.get('cache_policy', 'memory-and-disk') not in ('none', 'memory-and-disk'):
        raise ValueError('Invalid cache policy')
    if not isinstance(config.get('demo_fail_first', False), bool):
        raise ValueError('demo_fail_first must be boolean')
    validate_snapshot(source, config['source_batch'])
    return source, dest


def attempt_id(dag_run_id, try_number):
    if not isinstance(try_number, int) or try_number < 1:
        raise ValueError('Invalid try number')
    digest = hashlib.sha256(dag_run_id.encode('utf-8')).hexdigest()[:24]
    return safe_name(f'af-{digest}-t{try_number}')


def atomic_json(path, value):
    path = Path(path)
    temp = path.with_suffix('.tmp-' + str(os.getpid()))
    write_json(temp, value)
    os.replace(temp, path)


def submit(config, dag_run_id, try_number):
    validate_config(config)
    key = attempt_id(dag_run_id, try_number)
    QUEUE.mkdir(parents=True, exist_ok=True)
    path = QUEUE / (key + '.request.json')
    if not path.exists():
        request = {'request_id': key, 'config': config, 'try_number': try_number,
            'created_at_utc': utc_now(), 'expires_at_utc': (datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat()}
        atomic_json(path, request)
    else:
        existing = json.loads(path.read_text(encoding='utf-8'))
        if existing['config'] != config or existing['try_number'] != try_number:
            raise ValueError('Attempt id reused with different configuration')
    return key


def read_success(result, config):
    if not result.get('passed'):
        raise RuntimeError('Worker failed: ' + result.get('error', 'unknown'))
    root = project_path(config['run_root'])
    version = (root / result['version']).resolve()
    if not version.is_relative_to(root / 'versions'):
        raise ValueError('Invalid published version')
    manifest_path = version / 'run_manifest.json'
    if sha256(manifest_path) != result['manifest_sha256']:
        raise ValueError('Result manifest hash mismatch')
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    if manifest['run_id'] != result['request_id'] or manifest['biz_date'] != config['biz_date'] or manifest['source_batch'] != config['source_batch']:
        raise ValueError('Result lineage mismatch')
    if not manifest['quality']['passed']:
        raise ValueError('Unsuccessful quality gate')
    for name, checksum in manifest['artifacts'].items():
        artifact = (version / name).resolve()
        if not artifact.is_relative_to(version) or sha256(artifact) != checksum:
            raise ValueError('Result artifact mismatch: ' + name)
    return result


def wait_result(key, config, timeout_seconds=960, poll_seconds=2):
    started = time.monotonic()
    path = QUEUE / (safe_name(key) + '.result.json')
    while time.monotonic() - started < timeout_seconds:
        if path.exists():
            result = json.loads(path.read_text(encoding='utf-8'))
            if result.get('request_id') != key:
                raise ValueError('Unexpected worker response')
            return read_success(result, config)
        time.sleep(poll_seconds)
    raise TimeoutError('Worker result timeout; inspect queue/claim and warehouse logs before retry')


def execute_request(path, runner):
    request = json.loads(Path(path).read_text(encoding='utf-8'))
    key = safe_name(request['request_id'])
    if Path(path).name != key + '.request.json':
        raise ValueError('Request filename mismatch')
    result_path = QUEUE / (key + '.result.json')
    if result_path.exists():
        return False
    # Claim is retained on interruption; never silently execute an uncertain attempt twice.
    claim = QUEUE / (key + '.claim.json')
    if claim.exists():
        return False
    with claim.open('x', encoding='utf-8') as stream:
        json.dump({'pid': os.getpid(), 'claimed_at_utc': utc_now()}, stream)
    result = {'request_id': key, 'passed': False, 'started_at_utc': utc_now()}
    catalog = None
    try:
        config = request['config']
        source, dest = validate_config(config)
        catalog = dest / 'catalog' / (config['biz_date'] + '.json')
        result['catalog_before_sha256'] = sha256(catalog) if catalog.exists() else None
        if datetime.fromisoformat(request['expires_at_utc']) < datetime.now(timezone.utc):
            raise TimeoutError('Queued request expired; no Spark job started')
        fail = config.get('demo_fail_first', False) and request['try_number'] == 1
        if runner(source, dest, config['source_batch'], config['biz_date'], key, fail,
                  config.get('cache_policy', 'memory-and-disk')):
            raise RuntimeError('Spark job failed; see target/warehouse-logs/' + key + '.log')
        sys.path.insert(0, str(PLATFORM / 'tools'))
        from warehouse import inspect
        version, manifest = inspect(dest, config['biz_date'])
        if manifest['run_id'] != key:
            raise ValueError('Catalog points to another run')
        result.update(passed=True, version=version.relative_to(dest).as_posix(),
            manifest_sha256=sha256(version / 'run_manifest.json'), quality=manifest['quality'])
    except Exception as failure:
        result['error'] = str(failure)
    if catalog is not None:
        result['catalog_after_sha256'] = sha256(catalog) if catalog.exists() else None
    result['completed_at_utc'] = utc_now()
    atomic_json(result_path, result)
    return True


def worker(max_jobs=0, idle_timeout=900):
    sys.path.insert(0, str(PLATFORM / 'tools'))
    from warehouse import spark_run
    QUEUE.mkdir(parents=True, exist_ok=True)
    completed, last_work = 0, time.monotonic()
    with exclusive_lock(QUEUE, 'host-worker'):
        while not max_jobs or completed < max_jobs:
            for path in sorted(QUEUE.glob('*.request.json')):
                if execute_request(path, spark_run):
                    completed += 1
                    last_work = time.monotonic()
                    print('COMPLETED ' + path.stem, flush=True)
                    if max_jobs and completed >= max_jobs:
                        break
            if time.monotonic() - last_work > idle_timeout:
                break
            time.sleep(1)
    return completed
