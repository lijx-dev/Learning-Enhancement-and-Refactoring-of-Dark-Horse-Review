#!/usr/bin/env python3
"""Host worker for Airflow's fixed warehouse request protocol."""
import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'orchestration'))
from bridge import worker

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--max-jobs', type=int, default=0, help='0 means until idle timeout/interruption')
    parser.add_argument('--idle-timeout', type=int, default=900)
    args = parser.parse_args()
    if args.max_jobs < 0 or args.idle_timeout < 1:
        parser.error('Invalid worker limits')
    print('Processed requests:', worker(args.max_jobs, args.idle_timeout))
