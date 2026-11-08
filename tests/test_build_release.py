import subprocess
from pathlib import Path

import pytest

from deploy.build_release import build, verify


def test_build_uses_committed_files_and_never_overwrites(tmp_path):
    repo = tmp_path / 'repo'
    repo.mkdir()
    subprocess.run(['git', 'init', '-q', str(repo)], check=True)
    (repo / 'app.py').write_text('committed')
    subprocess.run(['git', '-C', str(repo), 'add', 'app.py'], check=True)
    subprocess.run(['git', '-C', str(repo), '-c', 'user.name=test', '-c',
                    'user.email=test@localhost', 'commit', '-qm', 'test'], check=True)
    (repo / 'app.py').write_text('uncommitted')
    destination = tmp_path / 'release'
    result = build(repo, 'HEAD', destination)
    assert (destination / 'app.py').read_text() == 'committed'
    assert len(result['commit']) == 40
    assert verify(destination, result['manifest_sha256']) == result['commit']
    with pytest.raises(ValueError, match='release_manifest_changed'):
        verify(destination, '0' * 64)
    with pytest.raises(ValueError, match='destination_already_exists'):
        build(repo, 'HEAD', destination)
    assert (destination / 'app.py').read_text() == 'committed'
    (destination / 'app.py').write_text('changed')
    with pytest.raises(ValueError, match='release_files_changed'):
        verify(destination)
