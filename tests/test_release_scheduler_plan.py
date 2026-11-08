from unittest.mock import patch

import pytest

from deploy.release_command import DEFAULT_UNITS, STREAM_RELEASE_UNITS, UNITS, _normalize_units, plan


def test_scheduler_option_only_changes_review_service(tmp_path):
    for unit in UNITS:
        directory = tmp_path / (unit + '.service.d')
        directory.mkdir()
        (directory / '50-self-assignment.conf').write_text(
            '[Service]\nWorkingDirectory=/old\nExecStart=\nExecStart=/usr/bin/python3 /old/worker.py --profile p\n')
    with patch('deploy.release_command.verify_bundle'):
        changes = plan(tmp_path / 'bundle', {}, cifra_app_id='12345678-1234-1234-1234-123456789abc',
                       scheduler_owner_open_id='owner_123', systemd_root=tmp_path)
    for path, content in changes.items():
        if 'cifra-review-inbox' in path:
            assert 'worker.py --profile p --scheduler-owner-open-id owner_123' in content
        else:
            assert '--scheduler-owner-open-id' not in content


def test_scheduler_option_rejects_service_argument_injection(tmp_path):
    with pytest.raises(ValueError, match='scheduler_owner_open_id_invalid'):
        plan(tmp_path, {}, cifra_app_id='12345678-1234-1234-1234-123456789abc',
             scheduler_owner_open_id='owner --other', systemd_root=tmp_path)


def test_stream_is_explicit_only_release_target_and_keeps_service_arguments(tmp_path):
    stream_unit = STREAM_RELEASE_UNITS[0]
    assert stream_unit in UNITS
    assert stream_unit not in DEFAULT_UNITS
    assert _normalize_units(None) == DEFAULT_UNITS

    root = tmp_path / 'systemd'
    directory = root / (stream_unit + '.service.d')
    directory.mkdir(parents=True)
    (directory / '50-self-assignment.conf').write_text(
        '[Service]\nWorkingDirectory=/old/release\nExecStart=\n'
        'ExecStart=/usr/bin/python3 /old/release/review_stream_worker.py --profile cifra\n')

    with patch('deploy.release_command.verify_bundle'):
        changes = plan(
            tmp_path / 'bundle',
            {'commit': 'synthetic'},
            units=(stream_unit,),
            systemd_root=root,
        )

    assert len(changes) == 1
    rendered = next(iter(changes.values()))
    assert 'review_stream_worker.py --profile cifra' in rendered
    assert '--card-callbacks' not in rendered


def test_stream_service_example_does_not_enable_card_callbacks_or_card_sender():
    from pathlib import Path

    example = Path(__file__).parents[1] / 'deploy' / 'cifra-review-stream.service.example'
    content = example.read_text()

    assert '--card-callbacks' not in content
    assert 'DWS_CARD_REVIEW_ENABLED=true' not in content
    assert '--card-template-id' not in content


@pytest.mark.parametrize('was_active', (False, True))
def test_stream_rollback_record_keeps_version_paths_and_original_active_state(
    tmp_path, monkeypatch, was_active
):
    import json
    from deploy import release_command
    from unittest.mock import patch

    stream_unit = STREAM_RELEASE_UNITS[0]
    override = str(tmp_path / (stream_unit + '.service.d') / '90-dingtodo-release.conf')
    monkeypatch.setattr(release_command, 'allowed_override_paths', lambda: {override})

    class Platform:
        def read_override(self, path):
            assert path == override
            return ('[Service]\nWorkingDirectory=/root/.hermes/releases/previous/code\n'
                    'ExecStart=/usr/bin/python3 /root/.hermes/releases/previous/code/review_stream_worker.py\n')

        def is_active(self, unit):
            assert unit == stream_unit + '.service'
            return was_active

    record_path = tmp_path / 'rollback.json'
    installed = ('[Service]\nWorkingDirectory=/root/.hermes/releases/candidate/code\n'
                 'ExecStart=/usr/bin/python3 /root/.hermes/releases/candidate/code/review_stream_worker.py\n')
    record = release_command.save_rollback_record(
        record_path, Platform(), {override: installed}, units=(stream_unit,))

    assert record['service_states'] == {stream_unit: was_active}
    assert '/previous/code' in record['previous'][override]
    assert '/candidate/code' in record['installed'][override]
    loaded = release_command.load_rollback_record(record_path)

    class RollbackPlatform:
        def read_override(self, path):
            return installed

    with patch('deploy.release_command.switch') as switch_transaction:
        release_command.rollback(loaded, platform=RollbackPlatform(), lock=object())

    assert switch_transaction.call_args.kwargs['services'] == (stream_unit + '.service',)
    assert switch_transaction.call_args.kwargs['restore_service_states'] == {
        stream_unit + '.service': was_active}


def test_stream_rollback_record_without_saved_state_is_rejected(tmp_path, monkeypatch):
    import json
    from deploy import release_command

    stream_unit = STREAM_RELEASE_UNITS[0]
    override = str(tmp_path / (stream_unit + '.service.d') / '90-dingtodo-release.conf')
    monkeypatch.setattr(release_command, 'allowed_override_paths', lambda: {override})
    record_path = tmp_path / 'rollback.json'
    record_path.write_text(json.dumps({
        'previous': {override: None},
        'installed': {override: '[Service]\nWorkingDirectory=/candidate/code\n'},
    }))
    record_path.chmod(0o600)

    with pytest.raises(ValueError, match='rollback_record_invalid'):
        release_command.load_rollback_record(record_path)


def _plan_for_cifra_app_id(tmp_path, app_id):
    systemd_root = tmp_path / 'systemd'
    directory = systemd_root / 'cifra-review-inbox.service.d'
    directory.mkdir(parents=True)
    (directory / '50-self-assignment.conf').write_text(
        '[Service]\nWorkingDirectory=/old/release\nExecStart=\n'
        'ExecStart=/usr/bin/python3 /old/release/review_inbox_worker.py --profile cifra\n')
    with patch('deploy.release_command.verify_bundle'):
        return plan(
            tmp_path / 'bundle',
            {},
            cifra_app_id=app_id,
            units=('cifra-review-inbox',),
            systemd_root=systemd_root,
        )


def test_cifra_app_id_accepts_opaque_37_character_identifier(tmp_path):
    app_id = 'id:/' + 'Az9_+' * 6 + 'abc'
    assert len(app_id) == 37

    changes = _plan_for_cifra_app_id(tmp_path, app_id)

    assert 'DINGTODO_REVIEW_APP_ID=' + app_id in next(iter(changes.values()))


def test_cifra_app_id_escapes_systemd_quoted_value_without_restricting_characters(tmp_path):
    app_id = 'review%id"with\\punctuation'

    changes = _plan_for_cifra_app_id(tmp_path, app_id)

    assert ('Environment="DINGTODO_REVIEW_APP_ID=review%%id\\"with\\\\punctuation"\n'
            in next(iter(changes.values())))


def test_cifra_app_id_accepts_256_character_limit(tmp_path):
    app_id = 'x' * 256

    changes = _plan_for_cifra_app_id(tmp_path, app_id)

    assert 'DINGTODO_REVIEW_APP_ID=' + app_id in next(iter(changes.values()))


@pytest.mark.parametrize('app_id', [
    None,
    '',
    '   ',
    ' leading',
    'trailing ',
    'interior\x1fcontrol',
    'x' * 257,
])
def test_cifra_app_id_rejects_empty_whitespace_control_and_overlong_values(tmp_path, app_id):
    with pytest.raises(ValueError, match='cifra_app_id_invalid'):
        plan(tmp_path / 'bundle', {}, cifra_app_id=app_id,
             units=('cifra-review-inbox',), systemd_root=tmp_path)
