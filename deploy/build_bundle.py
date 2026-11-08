"""Build and verify a code release and its separate runtime snapshot."""
from __future__ import annotations

import json
import shutil
from pathlib import Path

from deploy.build_release import build, verify
from deploy.runtime_snapshot import snapshot, verify_snapshot


def build_bundle(repo: Path, revision: str, runtime: Path, destination: Path) -> dict:
    if destination.exists() or destination.is_symlink():
        raise ValueError('bundle_destination_exists')
    destination.mkdir(mode=0o700)
    try:
        code = build(repo, revision, destination / 'code')
        dependencies = snapshot(destination / 'code', runtime, destination / 'runtime')
        metadata = {'commit': code['commit'], 'code_manifest_sha256': code['manifest_sha256'],
                    'runtime_manifest_sha256': dependencies['manifest_sha256']}
        (destination / 'bundle.json').write_text(json.dumps(metadata, sort_keys=True))
        (destination / 'bundle.json').chmod(0o600)
        return metadata
    except BaseException:
        shutil.rmtree(destination)
        raise


def verify_bundle(destination: Path, trusted_metadata: dict) -> None:
    metadata_path = destination / 'bundle.json'
    if metadata_path.is_symlink() or not metadata_path.is_file():
        raise ValueError('bundle_metadata_missing')
    if json.loads(metadata_path.read_text()) != trusted_metadata:
        raise ValueError('bundle_metadata_changed')
    commit = verify(destination / 'code', trusted_metadata['code_manifest_sha256'])
    if commit != trusted_metadata['commit']:
        raise ValueError('bundle_commit_mismatch')
    verify_snapshot(destination / 'runtime', trusted_metadata['runtime_manifest_sha256'])
