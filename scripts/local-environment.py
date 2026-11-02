#!/usr/bin/env python3
"""Operate the isolated local application and offline Spark validation environment."""
import argparse
from datetime import datetime, timezone
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'target/local-environment'
DOCKER = ['docker', '--context', 'desktop-linux']
COMPOSE = DOCKER + ['compose', '-f', 'dev/local/compose.yml']


def capture(command, timeout=30):
    try:
        proc = subprocess.run(command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
        raw = proc.stdout
        encoding = 'utf-16le' if b'\x00' in raw[:500] else 'utf-8'
        return {'exit_code': proc.returncode, 'output': raw.decode(encoding, errors='replace').strip()}
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {'exit_code': -1, 'output': str(exc)}


def run(command, label):
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / (label + '.log')
    print('Running:', ' '.join(command), flush=True)
    print('Full log:', path, flush=True)
    with path.open('w', encoding='utf-8') as log:
        proc = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        code = proc.wait()
    lines = path.read_text(encoding='utf-8', errors='replace').splitlines()
    print('\n'.join(lines[-35:]), flush=True)
    if code:
        raise RuntimeError(f'{label} failed ({code}); inspect {path}')


def write_report(name, report):
    OUT.mkdir(parents=True, exist_ok=True)
    report['checked_at_utc'] = datetime.now(timezone.utc).isoformat()
    (OUT / name).write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)


def check():
    commands = {tool: shutil.which(tool) for tool in ['git', 'java', 'javac', 'mvn', 'python', 'node', 'docker', 'wsl', 'code']}
    versions = {'python': platform.python_version()}
    for tool, args in [('node', ['--version']), ('docker', ['compose', 'version']), ('java', ['-version']), ('mvn', ['-version'])]:
        if commands[tool]:
            versions[tool] = capture([commands[tool], *args])
    packages = {}
    for name in ['pyspark', 'pyarrow', 'pandas', 'PyMySQL', 'pytest', 'apache-airflow', 'duckdb']:
        try:
            packages[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            packages[name] = None
    ports = {}
    for port in [3306, 6379, 8080, 8081, 13306, 18080, 18081, 4040]:
        with socket.socket() as client:
            client.settimeout(0.2)
            ports[str(port)] = client.connect_ex(('127.0.0.1', port)) == 0
    report = {
        'os': platform.platform(), 'commands': commands, 'versions': versions,
        'python_packages': packages, 'ports_in_use': ports,
        'free_disk_gib': {drive: round(shutil.disk_usage(drive).free / 2**30, 2) for drive in (['C:/', 'D:/'] if os.name == 'nt' else ['/'])},
        'docker_engine': capture(DOCKER + ['info', '--format', '{{.ServerVersion}} {{.OSType}} CPUs={{.NCPU}} Memory={{.MemTotal}}']),
        'docker_images': capture(DOCKER + ['image', 'ls', '--format', '{{.Repository}}:{{.Tag}} {{.Size}}']),
        'project_services': capture(COMPOSE + ['ps', '--all', '--format', 'json']),
        'extensions': capture([commands['code'], '--list-extensions', '--show-versions']) if commands['code'] else None,
        'wsl': capture(['wsl', '--list', '--verbose']) if commands['wsl'] else None,
    }
    try:
        import psutil
        memory = psutil.virtual_memory()
        report['memory_gib'] = {'total': round(memory.total / 2**30, 2), 'available': round(memory.available / 2**30, 2)}
    except ImportError:
        pass
    write_report('inventory.json', report)


def require_engine():
    status = capture(DOCKER + ['info', '--format', '{{.ServerVersion}}'])
    if status['exit_code']:
        raise RuntimeError('Docker is unavailable. Start Docker Desktop and rerun. ' + status['output'])


def smoke():
    (OUT / 'application-smoke.json').unlink(missing_ok=True)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    base = 'http://127.0.0.1:18080'

    def request(path, payload=None, token=None):
        headers = {'Content-Type': 'application/json'}
        if token:
            headers['authorization'] = token
        data = json.dumps(payload).encode() if payload is not None else None
        with opener.open(urllib.request.Request(base + path, data=data, headers=headers), timeout=10) as response:
            raw = response.read()
            return response.status, json.loads(raw) if 'json' in response.headers.get('Content-Type', '') else raw

    deadline = time.monotonic() + 240
    while True:
        try:
            status, health = request('/api/actuator/health')
            if status == 200 and health.get('status') == 'UP':
                break
        except (OSError, ValueError, urllib.error.HTTPError):
            pass
        if time.monotonic() > deadline:
            raise RuntimeError('Application did not become healthy; inspect compose logs app')
        time.sleep(3)
    checks = {}
    for path in ['/', '/api/shop-type/list', '/api/shop/1', '/api/experience/cache?shopId=1', '/imgs/missing-local-demo.png']:
        status, body = request(path)
        assert status == 200, path
        if isinstance(body, dict):
            assert body.get('success') is True, (path, body)
        checks[path] = 'passed'
    # Use the normal code endpoint. Read only that fixture's code from this stack's Redis.
    phone = '18800009991'
    code_response = request('/api/user/code?phone=' + phone, {})[1]
    assert code_response['success'], 'Fixture code request rejected; repeated smoke runs must respect the 60-second code rate limit.'
    result = capture(COMPOSE + ['exec', '-T', 'redis', 'redis-cli', '--raw', 'GET', 'login:code:' + phone])
    code = result['output'].strip()
    assert result['exit_code'] == 0 and len(code) == 6 and code.isdigit(), 'fixture code unavailable'
    login = request('/api/user/login', {'phone': phone, 'code': code})[1]
    assert login['success'], 'normal login rejected'
    token = login['data']
    me = request('/api/user/me', token=token)[1]
    assert me['success'] and me['data'].get('id'), 'authenticated identity unavailable'
    checks['normal login and /user/me'] = 'passed'
    write_report('application-smoke.json', {
        'passed': True, 'base_url': base, 'checks': checks,
        'fixture_phone': phone, 'limits': 'Local demo fixture only; no live SMS, payment or broker/CDC validation.',
    })


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['check', 'build', 'up', 'smoke', 'test', 'spark', 'down', 'status'])
    args = parser.parse_args()
    if args.action == 'check':
        check()
        return
    require_engine()
    if args.action == 'build':
        run(COMPOSE + ['run', '--rm', 'build'], 'java-build')
    elif args.action == 'up':
        jar = ROOT / 'target/hm-dianping-0.0.1-SNAPSHOT.jar'
        if not jar.exists() or any(path.stat().st_mtime > jar.stat().st_mtime for path in [ROOT / 'pom.xml', *ROOT.glob('src/main/**/*')] if path.is_file()):
            raise RuntimeError('Build the current source first: python scripts/local-environment.py build')
        run(COMPOSE + ['up', '-d', 'mysql', 'redis', 'app', 'web'], 'application-up')
        smoke()
    elif args.action == 'smoke':
        smoke()
    elif args.action == 'test':
        run(COMPOSE + ['stop', 'app', 'web'], 'stop-app-for-test')
        try:
            run(COMPOSE + ['run', '--rm', 'test'], 'java-integration-tests')
        finally:
            run(COMPOSE + ['stop', 'mysql-test', 'redis-test'], 'stop-test-services')
    elif args.action == 'spark':
        run(COMPOSE + ['stop', 'app', 'web'], 'stop-app-for-spark')
        run(COMPOSE + ['up', '-d', '--wait', 'mysql'], 'spark-source-up')
        run(COMPOSE + ['run', '--rm', 'spark'], 'spark-smoke')
    elif args.action == 'down':
        run(COMPOSE + ['--profile', 'test', 'down'], 'environment-down')
    elif args.action == 'status':
        print(capture(COMPOSE + ['ps', '--all'])['output'])


if __name__ == '__main__':
    try:
        main()
    except Exception as failure:
        print('FAILED:', failure, file=sys.stderr)
        sys.exit(1)
