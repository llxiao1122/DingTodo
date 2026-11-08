"""Build a private, reproducible DingTodo candidate from one Git commit."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile


def build(repo: Path, revision: str, destination: Path) -> dict:
    repo = repo.resolve(strict=True)
    if destination.exists() or destination.is_symlink():
        raise ValueError('destination_already_exists')
    commit = subprocess.check_output(
        ['git', '-C', str(repo), 'rev-parse', '--verify', revision + '^{commit}'], text=True
    ).strip()
    destination.mkdir(mode=0o700, parents=False)
    try:
        with tempfile.TemporaryFile() as archive:
            subprocess.run(['git', '-C', str(repo), 'archive', '--format=tar', commit],
                           stdout=archive, check=True)
            archive.seek(0)
            with tarfile.open(fileobj=archive, mode='r:') as source:
                members = source.getmembers()
                for member in members:
                    path = Path(member.name)
                    if path.is_absolute() or '..' in path.parts or not (member.isfile() or member.isdir()):
                        raise ValueError('unsafe_archive_member')
                source.extractall(destination, members=members, filter='data')
        files = {}
        for path in sorted(destination.rglob('*')):
            if path.is_file():
                files[str(path.relative_to(destination))] = hashlib.sha256(path.read_bytes()).hexdigest()
        manifest = {'commit': commit, 'files': files}
        (destination / 'release-manifest.json').write_text(json.dumps(manifest, sort_keys=True, indent=2))
        os.chmod(destination / 'release-manifest.json', 0o600)
        return {'commit': commit, 'file_count': len(files), 'destination': str(destination),
                'manifest_sha256': hashlib.sha256((destination / 'release-manifest.json').read_bytes()).hexdigest()}
    except BaseException:
        import shutil
        shutil.rmtree(destination)
        raise


def verify(destination: Path, expected_manifest_sha256: str | None = None) -> str:
    """Verify the complete file set and content before a release is used."""
    if destination.is_symlink() or not destination.is_dir():
        raise ValueError('invalid_release_directory')
    manifest_path = destination / 'release-manifest.json'
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ValueError('release_manifest_missing')
    manifest_bytes = manifest_path.read_bytes()
    if expected_manifest_sha256 is not None and hashlib.sha256(manifest_bytes).hexdigest() != expected_manifest_sha256:
        raise ValueError('release_manifest_changed')
    manifest = json.loads(manifest_bytes)
    expected = manifest.get('files')
    if not isinstance(expected, dict) or not isinstance(manifest.get('commit'), str):
        raise ValueError('invalid_release_manifest')
    actual = {}
    for path in destination.rglob('*'):
        if path.is_symlink():
            raise ValueError('release_symlink_forbidden')
        if path.is_file() and path != manifest_path:
            actual[str(path.relative_to(destination))] = hashlib.sha256(path.read_bytes()).hexdigest()
    if actual != expected:
        raise ValueError('release_files_changed')
    return manifest['commit']


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo', type=Path)
    parser.add_argument('--revision')
    parser.add_argument('--destination', type=Path, required=True)
    parser.add_argument('--verify', action='store_true')
    parser.add_argument('--manifest-sha256')
    args = parser.parse_args()
    if args.verify:
        if args.repo or args.revision:
            parser.error('--verify cannot be combined with --repo or --revision')
        print(json.dumps({'commit': verify(args.destination, args.manifest_sha256), 'verified': True}))
        return
    if args.manifest_sha256:
        parser.error('--manifest-sha256 requires --verify')
    if not args.repo or not args.revision:
        parser.error('--repo and --revision are required for build')
    print(json.dumps(build(args.repo, args.revision, args.destination), ensure_ascii=False))


if __name__ == '__main__':
    main()
