#!/usr/bin/env python3
"""Package the current runnable JAR and web assets; never include local credentials."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
CONFIG_FILES = (
    'Dockerfile', '.dockerignore', '.env.example', 'compose.yml',
    'nginx.conf', 'init-env.sh', 'init-images.sh', 'README.md',
)
IMAGE_SUFFIXES = {'.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg', '.ico', '.avif'}


def copy_tree(source, destination, images_only=False):
    for file in sorted(source.rglob('*')):
        if not file.is_file() or file.is_symlink():
            continue
        relative = file.relative_to(source)
        if any(part.startswith('.') or part in {'node_modules', 'tests'} for part in relative.parts):
            continue
        if images_only and file.suffix.lower() not in IMAGE_SUFFIXES:
            continue
        if not images_only and file.suffix.lower() not in {
            '.html', '.js', '.css', '.json', '.woff', '.woff2', '.ttf', *IMAGE_SUFFIXES
        }:
            continue
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(file, target)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT / 'target/hmdp-cloud-release.tar.gz')
    parser.add_argument('--images-dir', type=Path, default=ROOT / 'dev/images',
                        help='Optional original demo images; defaults to repository placeholders.')
    args = parser.parse_args()
    jar = ROOT / 'target/hm-dianping-0.0.1-SNAPSHOT.jar'
    if not jar.is_file():
        parser.error('Build the current Java project with Maven package first.')
    inputs = [ROOT / 'pom.xml', *ROOT.glob('src/main/**/*')]
    if any(p.is_file() and p.stat().st_mtime > jar.stat().st_mtime for p in inputs):
        parser.error('JAR predates current source/configuration. Rebuild before packaging.')
    images = args.images_dir.resolve()
    if not images.is_dir():
        parser.error(f'Initial image directory is missing: {images}')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='hmdp-cloud-package-') as temporary:
        bundle = Path(temporary) / 'hmdp-cloud'
        bundle.mkdir()
        for name in CONFIG_FILES:
            shutil.copy2(ROOT / 'deploy' / name, bundle / name)
        shutil.copy2(jar, bundle / 'app.jar')
        shutil.copy2(ROOT / 'src/main/resources/db/hmdp.sql', bundle / 'hmdp.sql')
        (bundle / 'frontend').mkdir()
        (bundle / 'images').mkdir()
        copy_tree(ROOT / 'frontend', bundle / 'frontend')
        copy_tree(images, bundle / 'images', images_only=True)
        manifest = {
            str(p.relative_to(bundle)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(bundle.rglob('*')) if p.is_file()
        }
        (bundle / 'manifest.json').write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + '\n', encoding='utf-8'
        )
        # Explicit config whitelist and tree filter exclude .env, private keys and dev logs.
        with tarfile.open(args.output, 'w:gz') as archive:
            archive.add(bundle, arcname='hmdp-cloud', recursive=True)
    print(args.output.resolve())
    print('SHA256:', hashlib.sha256(args.output.read_bytes()).hexdigest())
    print('Bytes:', args.output.stat().st_size)


if __name__ == '__main__':
    main()
