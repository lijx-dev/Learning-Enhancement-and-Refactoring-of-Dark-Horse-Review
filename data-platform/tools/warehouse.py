#!/usr/bin/env python3
"""Host entry point for local Spark daily runs, extraction and catalog inspection."""
import argparse
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'data-platform'))
from common import sha256, safe_name


def container_path(path):
    path = Path(path).resolve()
    if not path.is_relative_to(ROOT):
        raise ValueError('Container paths must stay within the mounted project root')
    return '/workspace/' + path.relative_to(ROOT).as_posix()


def spark_run(input_root, run_root, source_batch, biz_date, run_id, fail=False, cache_policy='none'):
    safe_name(source_batch)
    safe_name(run_id)
    command = ['docker', '--context', 'desktop-linux', 'compose', '-f', 'dev/local/compose.yml',
        'run', '--rm', '--no-deps', 'spark', '--master', 'local[2]', '--driver-memory', '768m',
        '/workspace/data-platform/jobs/offline_pipeline.py', '--input-root', container_path(input_root),
        '--run-root', container_path(run_root), '--source-batch', source_batch, '--biz-date', biz_date, '--run-id', run_id,
        '--cache-policy', cache_policy]
    if fail:
        command.append('--fail-before-publish')
    log = ROOT / 'target/warehouse-logs' / (run_id + '.log')
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open('w', encoding='utf-8') as stream:
        proc = subprocess.run(command, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT)
    print(log.read_text(encoding='utf-8', errors='replace')[-4000:])
    return proc.returncode


def inspect(run_root, biz_date):
    root = Path(run_root).resolve()
    pointer = json.loads((root / 'catalog' / (biz_date + '.json')).read_text(encoding='utf-8'))
    version = (root / pointer['version']).resolve()
    if not version.is_relative_to(root / 'versions'):
        raise ValueError('Invalid published version path')
    path = version / 'run_manifest.json'
    if sha256(path) != pointer['manifest_sha256']:
        raise ValueError('Published run manifest checksum mismatch')
    manifest = json.loads(path.read_text(encoding='utf-8'))
    for name, checksum in manifest['artifacts'].items():
        artifact = (version / name).resolve()
        if not artifact.is_relative_to(version) or sha256(artifact) != checksum:
            raise ValueError('Published artifact checksum mismatch: ' + name)
    return version, manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    run = sub.add_parser('run')
    run.add_argument('--input-root', type=Path, required=True)
    run.add_argument('--run-root', type=Path, default=ROOT / 'target/warehouse-v2')
    run.add_argument('--source-batch', required=True)
    run.add_argument('--biz-date', required=True)
    run.add_argument('--run-id', required=True)
    run.add_argument('--fail-before-publish', action='store_true')
    run.add_argument('--cache-policy', choices=['none', 'memory-and-disk'], default='none')
    status = sub.add_parser('status')
    status.add_argument('--run-root', type=Path, default=ROOT / 'target/warehouse-v2')
    status.add_argument('--biz-date', required=True)
    args = parser.parse_args()
    if args.action == 'run':
        raise SystemExit(spark_run(args.input_root, args.run_root, args.source_batch, args.biz_date, args.run_id, args.fail_before_publish, args.cache_policy))
    version, manifest = inspect(args.run_root, args.biz_date)
    print(json.dumps({'version': str(version), 'manifest': manifest}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
