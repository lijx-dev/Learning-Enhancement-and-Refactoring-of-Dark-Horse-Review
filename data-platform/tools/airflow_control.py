#!/usr/bin/env python3
"""Small local Airflow CLI wrapper; trigger config is JSON, never interpolated shell."""
import argparse
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'data-platform/orchestration'))
from bridge import validate_config
from common import safe_name

COMPOSE = ['docker', '--context', 'desktop-linux', 'compose', '-f', 'dev/local/airflow.compose.yml']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    sub.add_parser('up')
    sub.add_parser('down')
    sub.add_parser('status')
    sub.add_parser('logs')
    trigger = sub.add_parser('trigger')
    trigger.add_argument('--config', type=Path, required=True)
    trigger.add_argument('--run-id', required=True)
    args = parser.parse_args()
    if args.action == 'up':
        (ROOT / 'target/airflow-state').mkdir(parents=True, exist_ok=True)
        command = COMPOSE + ['up', '-d', '--wait', '--pull', 'never', 'scheduler']
    elif args.action == 'down':
        command = COMPOSE + ['down']
    elif args.action == 'logs':
        command = COMPOSE + ['logs', '--tail', '100', 'scheduler']
    elif args.action == 'status':
        command = COMPOSE + ['exec', '-T', 'scheduler', 'airflow', 'dags', 'list-runs', '-d', 'hmdp_offline_daily', '-o', 'json']
    else:
        safe_name(args.run_id)
        config = json.loads(args.config.read_text(encoding='utf-8-sig'))
        validate_config(config)
        command = COMPOSE + ['exec', '-T', 'scheduler', 'airflow', 'dags', 'trigger', 'hmdp_offline_daily',
            '--run-id', args.run_id, '--conf', json.dumps(config, ensure_ascii=False)]
    subprocess.run(command, cwd=ROOT, check=True)


if __name__ == '__main__':
    main()
