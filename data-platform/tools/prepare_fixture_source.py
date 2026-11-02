#!/usr/bin/env python3
"""Prepare only the owned synthetic MySQL source and grant its existing reader SELECT."""
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]


def main():
    subprocess.run([sys.executable, '-X', 'utf8', str(ROOT / 'data-platform/tools/fixture.py'), 'mysql'], cwd=ROOT, check=True)
    sql = (ROOT / 'dev/local/03-fixture-reader.sql').read_text(encoding='utf-8')
    cmd = ['docker', '--context', 'desktop-linux', 'compose', '-f', 'dev/local/compose.yml', 'exec', '-T', 'mysql', 'sh', '-c',
           'export MYSQL_PWD="$MYSQL_ROOT_PASSWORD"; exec mysql -uroot --batch']
    subprocess.run(cmd, input=sql, text=True, cwd=ROOT, check=True)


if __name__ == '__main__':
    main()
