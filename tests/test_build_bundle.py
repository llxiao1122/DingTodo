import subprocess

import pytest

from deploy.build_bundle import build_bundle, verify_bundle


def test_bundle_binds_code_and_runtime(tmp_path):
    repo, runtime = tmp_path / 'repo', tmp_path / 'runtime'
    repo.mkdir()
    for name in ('polardb_work_adapter', 'polardb_work_writer'):
        candidate = repo / 'writer_candidate' / name
        live = runtime / 'workflows' / name
        candidate.mkdir(parents=True)
        live.mkdir(parents=True)
        (candidate / '__init__.py').write_text('')
        (live / '__init__.py').write_text('')
    (runtime / 'pymysql').mkdir()
    (runtime / 'pymysql' / '__init__.py').write_text('')
    subprocess.run(['git', 'init', '-q', str(repo)], check=True)
    subprocess.run(['git', '-C', str(repo), 'add', '.'], check=True)
    subprocess.run(['git', '-C', str(repo), '-c', 'user.name=test', '-c',
                    'user.email=test@localhost', 'commit', '-qm', 'test'], check=True)
    dest = tmp_path / 'bundle'
    metadata = build_bundle(repo, 'HEAD', runtime, dest)
    verify_bundle(dest, metadata)
    (dest / 'runtime' / 'pymysql' / '__init__.py').write_text('changed')
    with pytest.raises(ValueError, match='runtime_snapshot_changed'):
        verify_bundle(dest, metadata)


def test_release_plan_preserves_existing_service_arguments(tmp_path):
    from deploy.release_command import UNITS, plan
    from unittest.mock import patch
    bundle = tmp_path / 'bundle'
    root = tmp_path / 'systemd'
    for unit in UNITS:
        directory = root / (unit + '.service.d')
        directory.mkdir(parents=True)
        (directory / '50-self-assignment.conf').write_text(
            '[Service]\nWorkingDirectory=/old/release\nExecStart=\n'
            'ExecStart=/usr/bin/python3 /old/release/worker.py --existing-flag\n')
    with patch('deploy.release_command.verify_bundle') as verify:
        changes = plan(bundle, {'commit': 'synthetic'},
                       cifra_app_id='12345678-1234-1234-1234-123456789abc', systemd_root=root)
    verify.assert_called_once()
    assert len(changes) == 3
    assert all('--existing-flag' in value and str(bundle / 'runtime') in value
               for value in changes.values())
    assert 'DINGTODO_REVIEW_APP_ID=12345678-1234-1234-1234-123456789abc' in next(
        value for path, value in changes.items() if 'cifra-review-inbox' in path)


def test_release_plan_can_target_resident_and_recovery_without_touching_cifra(tmp_path):
    from deploy.release_command import plan
    from unittest.mock import patch

    bundle = tmp_path / 'bundle'
    root = tmp_path / 'systemd'
    units = ('hermes-dingtalk-history-resident', 'hermes-dingtalk-daily-recovery')
    for unit in units:
        directory = root / (unit + '.service.d')
        directory.mkdir(parents=True)
        (directory / '50-self-assignment.conf').write_text(
            '[Service]\nWorkingDirectory=/old/release\nExecStart=\n'
            'ExecStart=/usr/bin/python3 /old/release/worker.py --existing-flag\n')

    with patch('deploy.release_command.verify_bundle') as verify:
        changes = plan(
            bundle,
            {'commit': 'synthetic'},
            units=units,
            systemd_root=root,
        )

    verify.assert_called_once()
    assert len(changes) == 2
    assert all('--existing-flag' in value for value in changes.values())
    assert all('cifra-review-inbox' not in path for path in changes)


def test_partial_rollback_record_is_accepted_and_restores_only_selected_units(tmp_path, monkeypatch):
    import json
    from deploy import release_command
    from unittest.mock import patch

    path = str(tmp_path / 'hermes-dingtalk-history-resident.service.d' / '90-dingtodo-release.conf')
    allowed = {path, str(tmp_path / 'cifra-review-inbox.service.d' / '90-dingtodo-release.conf')}
    monkeypatch.setattr(release_command, 'allowed_override_paths', lambda: allowed)
    record = {
        'previous': {path: None},
        'installed': {path: '[Service]\nWorkingDirectory=/candidate/code\n'},
    }
    rollback_path = tmp_path / 'rollback.json'
    rollback_path.write_text(json.dumps(record))
    rollback_path.chmod(0o600)

    loaded = release_command.load_rollback_record(rollback_path)

    class Platform:
        def read_override(self, override_path):
            assert override_path == path
            return record['installed'][path]

    with patch('deploy.release_command.switch') as switch_transaction:
        release_command.rollback(loaded, platform=Platform(), lock=object())

    assert loaded == record
    assert switch_transaction.call_args.kwargs['changes'] == record['previous']
