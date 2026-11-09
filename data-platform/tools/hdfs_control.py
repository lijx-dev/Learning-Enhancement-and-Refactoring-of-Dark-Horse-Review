#!/usr/bin/env python3
"""Bounded single-node HDFS controls; initialization is explicit, data volumes are retained."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'data-platform'))
from common import safe_name

IMAGE = 'apache/hadoop:3.4.2'
REPOSITORY = 'apache/hadoop'
# Containerd image store reports the manifest digest as Id; the classic store
# reports the config digest. Both are pinned, so either can identify this image.
IMAGE_MANIFEST = 'sha256:0f79c18d27a2b15984868a41e245b09c98daa5638b573b4dbd848be650f05635'
IMAGE_ID = 'sha256:3a8752e6b7603e5da5967212315849a26c85bd416f056abe1702b12919cced60'
CLUSTER = 'CID-hmdp-lab-v1'
COMPOSE = ['docker', '--context', 'desktop-linux', 'compose', '-f', 'dev/local/hdfs.compose.yml']


def capture(arguments, timeout=120):
    process = subprocess.run(arguments, cwd=ROOT, capture_output=True, text=True,
        encoding='utf-8', errors='replace', timeout=timeout)
    if process.returncode:
        raise RuntimeError('Command failed: ' + ' '.join(arguments) + '\n' + process.stdout[-2000:] + process.stderr[-2000:])
    return process.stdout.strip()


def lab_path(tag):
    return '/hmdp-lab/' + safe_name(tag)


def require_image():
    raw = capture(['docker', '--context', 'desktop-linux', 'image', 'inspect', IMAGE, '--format', '{{json .}}'])
    image = json.loads(raw)
    if (image['Os'], image['Architecture']) != ('linux', 'amd64'):
        raise ValueError('HDFS requires the verified linux/amd64 image')
    descriptor = (image.get('Descriptor') or {}).get('digest')
    pulled_by_digest = REPOSITORY + '@' + IMAGE_MANIFEST in image.get('RepoDigests', [])
    if image['Id'] == IMAGE_ID:
        pass
    elif image['Id'] == IMAGE_MANIFEST and descriptor == IMAGE_MANIFEST and pulled_by_digest:
        pass
    else:
        raise ValueError('Unexpected Hadoop image digest; do not reuse a mutable tag')
    return {key: image[key] for key in ('Id', 'RepoDigests', 'Os', 'Architecture')}


def dfs(*arguments):
    return capture(COMPOSE + ['exec', '-T', 'namenode', 'env', 'HADOOP_HEAPSIZE_MAX=96', 'hdfs', *arguments])


def properties(text):
    return dict(line.split('=', 1) for line in text.splitlines() if line and not line.startswith('#') and '=' in line)


def validate_status(status):
    if not re.search(r'Live datanodes\s*\(1\)', status['report']):
        raise ValueError('Expected exactly one live DataNode')
    if not re.search(r'Safe mode is OFF\b', status['safe_mode']):
        raise ValueError('NameNode is still in safe mode')
    for node in ('namenode', 'datanode'):
        if status['identity'][node].get('clusterID') != CLUSTER:
            raise ValueError('Unexpected cluster identity: ' + node)
    if not status['identity']['namenode'].get('namespaceID') or not status['identity']['datanode'].get('datanodeUuid'):
        raise ValueError('Missing persistent namespace/DataNode identity')
    return True


def status():
    identity = {}
    for node in ('namenode', 'datanode'):
        identity[node] = properties(capture(COMPOSE + ['exec', '-T', node, 'cat', '/storage/' + node + '/current/VERSION']))
    result = {'identity': identity, 'report': dfs('dfsadmin', '-report', '-live'),
              'safe_mode': dfs('dfsadmin', '-safemode', 'get')}
    validate_status(result)
    return result


def validate_fsck(text):
    files = re.search(r'Total files:\s*(\d+)', text)
    blocks = re.search(r'Total blocks \(validated\):\s*(\d+)', text)
    if (not re.search(r'Status:\s*HEALTHY\b', text) or not files or int(files[1]) < 1
            or not blocks or int(blocks[1]) < 1
            or re.search(r'(?:Missing|Corrupt) blocks:\s*[1-9]\d*', text, re.I)):
        raise ValueError('HDFS fsck did not prove healthy nonempty files and blocks')
    return {'files': int(files[1]), 'blocks': int(blocks[1])}


def wait_ready(timeout=90):
    deadline = time.monotonic() + timeout
    while True:
        try:
            return status()
        except (ValueError, RuntimeError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['image', 'init', 'up', 'restart', 'status', 'down', 'logs'])
    args = parser.parse_args()
    if args.action == 'down':
        print(capture(COMPOSE + ['down']))  # Deliberately no -v.
    elif args.action == 'logs':
        print(capture(COMPOSE + ['logs', '--tail', '100', 'namenode', 'datanode']))
    elif args.action == 'status':
        print(json.dumps(status(), indent=2))
    else:
        image = require_image()
        if args.action == 'image':
            print(json.dumps(image, indent=2))
        elif args.action == 'init':
            print(capture(COMPOSE + ['run', '--rm', '--no-deps', '--pull', 'never', 'namenode', 'init']))
        else:
            if args.action == 'restart':
                print(capture(COMPOSE + ['stop', 'datanode', 'namenode']))
            print(capture(COMPOSE + ['up', '-d', '--wait', '--wait-timeout', '180', '--pull', 'never', 'namenode', 'datanode'], timeout=210))
            print(json.dumps(wait_ready(), indent=2))


if __name__ == '__main__':
    main()
