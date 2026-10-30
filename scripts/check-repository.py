#!/usr/bin/env python3
"""Check publishable files, common credential signatures and local Markdown links."""
import ast
import json
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import unquote
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]


def main():
    result = subprocess.run(
        ['git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z'],
        cwd=ROOT, check=True, capture_output=True,
    )
    names = sorted(set(result.stdout.decode('utf-8').strip('\0').split('\0')))
    published_files = set(names)
    errors = []
    links = 0
    for name in names:
        path = ROOT / name
        if not path.is_file():
            continue
        parts = Path(name).parts
        if any(p in {'.idea', 'target', 'node_modules', '__pycache__', 'uploads', 'data'} for p in parts):
            errors.append(f'{name}: generated/runtime file is included')
        if path.name == '.env' or (path.name.startswith('.env.') and path.name != '.env.example'):
            errors.append(f'{name}: private environment file is included')
        if path.suffix.lower() in {'.pem', '.key', '.p12', '.pfx', '.keystore', '.log'}:
            errors.append(f'{name}: credentials or raw logs are included')
        try:
            content = path.read_text(encoding='utf-8')
        except UnicodeDecodeError:
            continue
        # Only high-confidence signatures; do not print matched secrets to output.
        for signature in (
            r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----',
            r'\bgh[pousr]_[A-Za-z0-9_]{30,}',
            r'\bgithub_pat_[A-Za-z0-9_]{30,}',
            r'\bAKIA[0-9A-Z]{16}\b',
        ):
            if re.search(signature, content):
                errors.append(f'{name}: possible credential signature')
        try:
            if path.suffix == '.json':
                json.loads(content)
            elif path.suffix == '.xml':
                ET.fromstring(content)
            elif path.suffix == '.py':
                ast.parse(content, filename=name)
        except (ValueError, SyntaxError, ET.ParseError) as exc:
            errors.append(f'{name}: invalid syntax: {exc}')
        if path.suffix != '.md':
            continue
        # Ignore teaching code fences; validate files, not renderer-specific heading anchors.
        content = re.sub(r'(?ms)^```.*?^```[^\n]*', '', content)
        for target in re.findall(r'!?\[[^\]\n]*\]\(([^)\n]+)\)', content):
            target = target.strip()
            if target.startswith('<') and '>' in target:
                target = target[1:target.index('>')]
            else:
                target = target.split(' ', 1)[0]
            if target.startswith(('#', '/', '//')) or re.match(r'^[a-zA-Z][\w+.-]*:', target):
                continue
            target = unquote(target.split('#', 1)[0].split('?', 1)[0])
            if target:
                links += 1
                destination = (path.parent / target).resolve()
                if not destination.is_relative_to(ROOT) or not destination.exists():
                    errors.append(f'{name}: missing local link: {target}')
                elif destination.is_file() and destination.relative_to(ROOT).as_posix() not in published_files:
                    errors.append(f'{name}: local link points to an excluded file: {target}')
    for error in errors:
        print(error)
    print(f'Checked {len(names)} candidate files and {links} local links; {len(errors)} issues.')
    return bool(errors)


if __name__ == '__main__':
    sys.exit(main())
