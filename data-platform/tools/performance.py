#!/usr/bin/env python3
"""Sequential bounded performance acceptance; all data/logs stay under target."""
import argparse
import json
from pathlib import Path
import platform
import subprocess
import sys
from time import perf_counter

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'data-platform'))
sys.path.insert(0, str(ROOT / 'data-platform/tests'))
from common import safe_name, sha256, utc_now, write_json
from scale_fixture import generate
from verify_scale import verify
from verify_models import logical_signature
from warehouse import container_path, inspect, spark_run


def capture(command):
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, encoding='utf-8', errors='replace', check=True)
    return result.stdout.strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tag', required=True)
    parser.add_argument('--sizes', type=int, nargs='+', default=[10000, 100000])
    parser.add_argument('--root', type=Path, default=ROOT / 'target/warehouse-scale')
    args = parser.parse_args()
    safe_name(args.tag)
    if any(n not in (10000, 100000) for n in args.sizes) or len(set(args.sizes)) != len(args.sizes):
        raise ValueError('Choose distinct supported sizes: 10000 and/or 100000')
    args.root = args.root.resolve()
    if not args.root.is_relative_to(ROOT / 'target'):
        raise ValueError('Performance artifacts must stay under the project target directory')
    report_dir = args.root / 'reports' / args.tag
    report_dir.mkdir(parents=True, exist_ok=False)
    report = {'tag': args.tag, 'started_at_utc': utc_now(), 'python': platform.python_version(),
        'host': platform.platform(), 'seed': 42, 'biz_date': '2026-10-30', 'runs': [],
        'code_sha256': {name: sha256(ROOT / name) for name in [
            'data-platform/jobs/offline_pipeline.py', 'data-platform/jobs/performance_queries.py',
            'data-platform/sql/models.sql', 'data-platform/tools/scale_fixture.py',
            'data-platform/tests/verify_scale.py', 'dev/local/compose.yml']},
        'source_hashes': {}, 'generation_seconds': {},
        'docker': capture(['docker', '--context', 'desktop-linux', 'info', '--format', '{{.ServerVersion}} CPUs={{.NCPU}} Memory={{.MemTotal}}']),
        'image_id': capture(['docker', '--context', 'desktop-linux', 'image', 'inspect',
            'apache/spark:3.5.7-scala2.12-java17-python3-ubuntu', '--format', '{{.Id}}']),
        'limits': {'container_memory_bytes': 2147483648, 'container_cpus': 2, 'driver_memory': '768m',
            'master': 'local[2]', 'main_runs_per_policy_per_size': 1,
            'cold_cache': 'OS/Docker caches not dropped; main timings are observations, not statistical speedup estimates'}}
    try:
        for size in args.sizes:
            started = perf_counter()
            source, batch = generate(args.root / 'sources', size)
            report['generation_seconds'][str(size)] = perf_counter() - started
            report['source_hashes'][str(size)] = sha256(source / 'manifest.json')
            signatures = []
            # Alternate main-run order by size to reduce a fixed first-policy bias.
            policies = ['none', 'memory-and-disk'] if size == 10000 else ['memory-and-disk', 'none']
            for policy in policies:
                run_id = f'{args.tag}-n{size}-{policy}'
                run_root = args.root / 'runs' / f'n{size}-{policy}'
                print(f'START main size={size} policy={policy}', flush=True)
                started = perf_counter()
                if spark_run(source, run_root, batch, report['biz_date'], run_id, cache_policy=policy):
                    raise RuntimeError('Main pipeline failed: ' + run_id)
                wall = perf_counter() - started
                version, _ = inspect(run_root, report['biz_date'])
                result = verify(source, run_root, report['biz_date'])
                signature = logical_signature(version)
                signatures.append(signature)
                record = {'size': size, 'policy': policy, 'process_wall_seconds': wall,
                    'execution': json.loads((version / 'execution.json').read_text(encoding='utf-8')),
                    'independent_verification': result, 'version': version.relative_to(ROOT).as_posix()}
                report['runs'].append(record)
                write_json(report_dir / 'progress.json', report)
                print(json.dumps({**result, 'policy': policy, 'wall_seconds': wall}), flush=True)
            if signatures[0] != signatures[1]:
                raise ValueError('Caching changed logical contents of a model')
            report.setdefault('all_model_cache_equivalence', {})[str(size)] = True
            cached_root = args.root / 'runs' / f'n{size}-memory-and-disk'
            version, _ = inspect(cached_root, report['biz_date'])
            query_out = report_dir / f'queries-n{size}'
            command = ['docker', '--context', 'desktop-linux', 'compose', '-f', 'dev/local/compose.yml',
                'run', '--rm', '--no-deps', 'spark', '--master', 'local[2]', '--driver-memory', '768m',
                '/workspace/data-platform/jobs/performance_queries.py', '--version', container_path(version),
                '--output', container_path(query_out), '--biz-date', report['biz_date']]
            log = report_dir / f'queries-n{size}.log'
            with log.open('w', encoding='utf-8') as stream:
                subprocess.run(command, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT, check=True)
            report.setdefault('queries', {})[str(size)] = json.loads((query_out / 'report.json').read_text(encoding='utf-8'))
            write_json(report_dir / 'progress.json', report)
        report.update(passed=True, completed_at_utc=utc_now())
        write_json(report_dir / 'report.json', report)
        print('PASSED ' + str(report_dir / 'report.json'), flush=True)
    except BaseException as failure:
        report.update(passed=False, failure=str(failure), failed_at_utc=utc_now())
        write_json(report_dir / 'failure.json', report)
        raise


if __name__ == '__main__':
    main()
