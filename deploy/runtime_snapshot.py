"""Create an isolated, hashed Python runtime snapshot without configuration files."""
from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

from deploy.runtime_preflight import compare


def snapshot(candidate: Path, runtime: Path, destination: Path) -> dict:
    if destination.exists() or destination.is_symlink():
        raise ValueError('runtime_destination_exists')
    preflight = compare(candidate, runtime)
    if not preflight['compatible']:
        raise ValueError('runtime_dependency_mismatch')
    source = runtime / 'pymysql'
    if source.is_symlink() or not source.is_dir():
        raise ValueError('pymysql_source_invalid')
    destination.mkdir(mode=0o700)
    try:
        packages = {
            destination / 'workflows' / name: candidate / 'writer_candidate' / name
            for name in ('polardb_work_adapter', 'polardb_work_writer')
        }
        packages[destination / 'pymysql'] = source
        for target, origin in packages.items():
            target.mkdir(parents=True)
            if any(path.is_symlink() for path in origin.rglob('*')):
                raise ValueError('runtime_source_symlink')
            files = list(origin.rglob('*.py'))
            if not files:
                raise ValueError('runtime_package_empty')
            for path in files:
                if path.is_symlink() or not path.is_file():
                    raise ValueError('runtime_source_symlink')
                relative = path.relative_to(origin)
                target_file = target / relative
                target_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(path, target_file)
                target_file.chmod(0o600)
        files = {str(p.relative_to(destination)): hashlib.sha256(p.read_bytes()).hexdigest()
                 for p in sorted(destination.rglob('*.py'))}
        manifest = json.dumps(files, sort_keys=True, separators=(',', ':')).encode()
        (destination / 'runtime-manifest.json').write_bytes(manifest)
        (destination / 'runtime-manifest.json').chmod(0o600)
        return {'files': len(files), 'manifest_sha256': hashlib.sha256(manifest).hexdigest()}
    except BaseException:
        shutil.rmtree(destination)
        raise


def verify_snapshot(destination: Path, expected_manifest_sha256: str) -> None:
    if destination.is_symlink() or not destination.is_dir():
        raise ValueError('runtime_snapshot_invalid')
    manifest_path = destination / 'runtime-manifest.json'
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ValueError('runtime_manifest_missing')
    manifest = manifest_path.read_bytes()
    if hashlib.sha256(manifest).hexdigest() != expected_manifest_sha256:
        raise ValueError('runtime_manifest_changed')
    expected = json.loads(manifest)
    if not isinstance(expected, dict) or not expected:
        raise ValueError('runtime_manifest_invalid')
    actual = {}
    for path in destination.rglob('*'):
        if path.is_symlink():
            raise ValueError('runtime_snapshot_symlink')
        if path.is_file() and path != manifest_path:
            actual[str(path.relative_to(destination))] = hashlib.sha256(path.read_bytes()).hexdigest()
    if actual != expected:
        raise ValueError('runtime_snapshot_changed')
