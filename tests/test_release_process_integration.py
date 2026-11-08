import json
import subprocess

import pytest

from deploy.switch_transaction import ONESHOTS, RESIDENT, TIMERS, switch
from deploy.systemd_platform import SystemdPlatform, work_lock


_HEALTHY_RESIDENT = {
    'ActiveState': 'active',
    'SubState': 'running',
    'Result': 'success',
    'MainPID': '4242',
    'InvocationID': 'a' * 32,
}


def _fake_systemd(tmp_path, monkeypatch, **state_overrides):
    state_path = tmp_path / 'state.json'
    state = {
        'states': {
            RESIDENT: 'active',
            **{timer: 'active' for timer in TIMERS},
            **{unit: 'inactive' for unit in ONESHOTS},
        },
        'resident_properties': dict(_HEALTHY_RESIDENT),
        'resident_starts': 0,
        'health_checks': 0,
        'commands': [],
        **state_overrides,
    }
    state_path.write_text(json.dumps(state))

    binary = tmp_path / 'systemctl'
    binary.write_text('''#!/usr/bin/python3
import fcntl
import json
import os
import sys

state_path = os.environ['DINGTODO_TEST_STATE']
with open(state_path) as file:
    state = json.load(file)
args = sys.argv[1:]
state['commands'].append(args)

def save_state():
    with open(state_path, 'w') as file:
        json.dump(state, file)

if args[0] == 'show':
    unit = args[1]
    if unit == 'hermes-dingtalk-history-resident.service' and '--value' not in args:
        state['health_checks'] += 1
        lock_fd = os.open(os.environ['DINGTODO_TEST_LOCK'], os.O_RDWR | os.O_NOFOLLOW)
        try:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                state['lock_contended_during_health'] = False
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            except BlockingIOError:
                state['lock_contended_during_health'] = True

            if state.get('uncertain_candidate_health') and state['resident_starts'] == 1:
                state['states'][unit] = 'activating'
                state['resident_properties']['ActiveState'] = 'activating'
                state['resident_properties']['SubState'] = 'start'
                state['resident_properties']['MainPID'] = '0'
            if state.get('exit_after_health_check') == state['health_checks']:
                state['states'][unit] = 'inactive'
                state['resident_properties'].update({
                    'ActiveState': 'inactive',
                    'SubState': 'dead',
                    'Result': 'exit-code',
                    'MainPID': '0',
                })
            for argument in args[2:]:
                if argument.startswith('--property='):
                    name = argument.split('=', 1)[1]
                    print(name + '=' + state['resident_properties'][name])
        finally:
            os.close(lock_fd)
    else:
        print(state['states'][unit])
elif args[0] in ('stop', 'start'):
    action, unit = args
    if action == 'stop':
        state['states'][unit] = 'inactive'
        if unit == 'hermes-dingtalk-history-resident.service':
            state['resident_properties'].update({
                'ActiveState': 'inactive',
                'SubState': 'dead',
                'Result': 'success',
                'MainPID': '0',
            })
    else:
        state['states'][unit] = 'active'
        if unit == 'hermes-dingtalk-history-resident.service':
            state['resident_starts'] += 1
            if state.get('uncertain_rollback_start') and state['resident_starts'] >= 2:
                state['states'][unit] = 'activating'
                state['resident_properties'].update({
                    'ActiveState': 'activating',
                    'SubState': 'start',
                    'MainPID': '0',
                })
            else:
                state['resident_properties'] = {
                    'ActiveState': 'active',
                    'SubState': 'running',
                    'Result': 'success',
                    'MainPID': str(4242 + state['resident_starts']),
                    'InvocationID': chr(96 + state['resident_starts']) * 32,
                }
elif args[0] == 'daemon-reload' and state.get('fail_reload'):
    state['fail_reload'] = False
    save_state()
    sys.exit(1)

save_state()
''')
    binary.chmod(0o700)
    monkeypatch.setenv('PATH', str(tmp_path))
    monkeypatch.setenv('DINGTODO_TEST_STATE', str(state_path))

    lock_path = tmp_path / 'work.lock'
    lock_path.touch()
    monkeypatch.setenv('DINGTODO_TEST_LOCK', str(lock_path))
    config = tmp_path / 'override.conf'
    config.write_text('old')
    platform = SystemdPlatform({str(config)})
    return state_path, config, lock_path, platform


class _SimulatedClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def _use_simulated_clock(platform):
    clock = _SimulatedClock()
    platform._monotonic = clock.monotonic
    platform._sleep = clock.sleep
    return clock


def _run_switch(platform, config, lock_path):
    switch(
        platform=platform,
        changes={str(config): 'new'},
        verify_bundle=lambda: None,
        lock=work_lock(lock_path),
    )


def _read_state(state_path):
    return json.loads(state_path.read_text())


def test_successful_switch_keeps_resident_healthy_for_full_window(tmp_path, monkeypatch):
    state_path, config, lock_path, platform = _fake_systemd(tmp_path, monkeypatch)
    clock = _use_simulated_clock(platform)

    _run_switch(platform, config, lock_path)

    state = _read_state(state_path)
    assert config.read_text() == 'new'
    assert state['states'][RESIDENT] == 'active'
    assert all(state['states'][timer] == 'active' for timer in TIMERS)
    assert clock.now == 35.0
    assert sum(clock.sleeps) == 35.0
    assert state['health_checks'] == 36
    assert state['lock_contended_during_health'] is True


def test_candidate_exit_after_start_restores_files_and_restarts_previous_resident(
    tmp_path, monkeypatch
):
    state_path, config, lock_path, platform = _fake_systemd(
        tmp_path, monkeypatch, exit_after_health_check=2
    )
    clock = _use_simulated_clock(platform)

    with pytest.raises(RuntimeError, match='resident_health_check_failed'):
        _run_switch(platform, config, lock_path)

    state = _read_state(state_path)
    assert config.read_text() == 'old'
    assert state['resident_starts'] == 2
    assert state['health_checks'] == 2
    assert state['states'][RESIDENT] == 'active'
    assert all(state['states'][timer] == 'active' for timer in TIMERS)
    assert clock.now == 1.0
    resident_starts = [
        command for command in state['commands']
        if command == ['start', RESIDENT]
    ]
    assert len(resident_starts) == 2


def test_uncertain_rollback_state_keeps_timers_stopped(tmp_path, monkeypatch):
    state_path, config, lock_path, platform = _fake_systemd(
        tmp_path,
        monkeypatch,
        uncertain_candidate_health=True,
        uncertain_rollback_start=True,
    )

    with pytest.raises(RuntimeError, match='release_rollback_failed'):
        _run_switch(platform, config, lock_path)

    state = _read_state(state_path)
    assert config.read_text() == 'old'
    assert state['states'][RESIDENT] == 'activating'
    assert all(state['states'][timer] == 'inactive' for timer in TIMERS)
    assert not any(
        command[0] == 'start' and command[1] in TIMERS
        for command in state['commands']
    )


def test_reload_failure_restores_previous_files_and_unit_states(tmp_path, monkeypatch):
    state_path, config, lock_path, platform = _fake_systemd(
        tmp_path, monkeypatch, fail_reload=True
    )

    with pytest.raises(subprocess.CalledProcessError):
        _run_switch(platform, config, lock_path)

    state = _read_state(state_path)
    assert config.read_text() == 'old'
    assert state['states'][RESIDENT] == 'active'
    assert all(state['states'][timer] == 'active' for timer in TIMERS)
