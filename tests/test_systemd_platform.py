import subprocess
from unittest.mock import patch

import pytest

from deploy.switch_transaction import STREAM_SERVICES
from deploy.systemd_platform import SystemdPlatform, _systemd_timeout_seconds, work_lock


def test_atomic_override_restore_and_unit_allowlist(tmp_path):
    path = tmp_path / 'override.conf'
    platform = SystemdPlatform({str(path)})
    assert platform.read_override(str(path)) is None
    platform.write_override(str(path), '[Service]\n')
    assert platform.read_override(str(path)) == '[Service]\n'
    platform.restore_override(str(path), None)
    assert not path.exists()
    with pytest.raises(ValueError):
        platform.stop('unrelated.service')
    with patch('deploy.systemd_platform.subprocess.run', return_value=subprocess.CompletedProcess([], 0, 'failed\n', '')):
        with pytest.raises(RuntimeError, match='unit_state_uncertain'):
            platform.is_active('cifra-review-inbox.service')


def test_work_lock_is_exclusive(tmp_path):
    path = tmp_path / 'work.lock'
    path.touch()
    with work_lock(path):
        with pytest.raises(BlockingIOError):
            with work_lock(path):
                pass


def test_resident_health_rejects_restart_during_stability_window():
    outputs = [
        'ActiveState=active\nSubState=running\nResult=success\nMainPID=101\n'
        'InvocationID=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n',
        'ActiveState=active\nSubState=running\nResult=success\nMainPID=101\n'
        'InvocationID=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n',
        'ActiveState=active\nSubState=running\nResult=success\nMainPID=102\n'
        'InvocationID=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n',
    ]
    platform = SystemdPlatform(set())
    platform._command = lambda *args: subprocess.CompletedProcess(args, 0, outputs.pop(0), '')

    class Clock:
        now = 0.0

    clock = Clock()
    platform._monotonic = lambda: clock.now
    platform._sleep = lambda seconds: setattr(clock, 'now', clock.now + seconds)

    with pytest.raises(RuntimeError, match='resident_health_unstable'):
        platform.wait_for_resident_health(stability_seconds=2, poll_interval_seconds=1)
    assert clock.now == 2
    assert outputs == []


def test_stream_unit_is_allowlisted_and_health_requires_stable_running_process():
    unit = STREAM_SERVICES[0]
    platform = SystemdPlatform(set())
    commands = []

    def command(*args, **kwargs):
        commands.append((args, kwargs))
        if args[0] == 'show' and '--property=ActiveState' in args:
            return subprocess.CompletedProcess(args, 0, 'active\n', '')
        if args[0] == 'show' and '--property=TimeoutStopUSec' in args:
            return subprocess.CompletedProcess(args, 0, '3min\n', '')
        return subprocess.CompletedProcess(args, 0, '', '')

    platform._command = command
    assert platform.is_active(unit)
    platform.stop(unit)
    platform.start(unit)
    assert commands == [
        (('show', unit, '--property=ActiveState', '--value'), {}),
        (('show', unit, '--property=TimeoutStopUSec', '--value'), {}),
        (('stop', unit), {'timeout': 240.0}),
        (('start', unit), {}),
    ]

    health = {
        'ActiveState': 'active',
        'SubState': 'running',
        'Result': 'success',
        'MainPID': '2024',
        'InvocationID': 'c' * 32,
    }
    snapshots = []
    platform._service_health_snapshot = lambda selected: snapshots.append(selected) or health

    class Clock:
        now = 0.0

    clock = Clock()
    platform._monotonic = lambda: clock.now
    platform._sleep = lambda seconds: setattr(clock, 'now', clock.now + seconds)
    platform.wait_for_stream_health(unit, stability_seconds=1, poll_interval_seconds=1)
    assert snapshots == [unit, unit]
    assert clock.now == 1

    with pytest.raises(ValueError, match='unit_not_allowed'):
        platform.start('unrelated.service')


def test_systemd_timeout_parser_accepts_normalized_compound_values_and_rejects_unbounded():
    assert _systemd_timeout_seconds('1min 30s') == 90
    assert _systemd_timeout_seconds('180s') == 180
    for invalid in ('', 'infinity', '0', 'n/a', '3601s'):
        with pytest.raises(ValueError, match='unit_stop_timeout_invalid'):
            _systemd_timeout_seconds(invalid)
