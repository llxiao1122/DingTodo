from contextlib import contextmanager

import pytest

from deploy.switch_transaction import ONESHOTS, RESIDENT, STREAM_SERVICES, TIMERS, switch


class Platform:
    def __init__(self, fail_write=False, running_oneshot=False, fail_health=False,
                 fail_stream_health=False):
        self.files = {'override': 'old'}
        self.states = {RESIDENT: True, **{timer: True for timer in TIMERS},
                       **{unit: False for unit in ONESHOTS},
                       **{unit: False for unit in STREAM_SERVICES}}
        if running_oneshot:
            self.states[ONESHOTS[0]] = True
        self.fail_write = fail_write
        self.fail_health = fail_health
        self.fail_stream_health = fail_stream_health
        self.stream_health_checks = 0
        self.locked = False
        self.health_checked_while_locked = False
        self.operations = []

    def is_active(self, unit): return self.states[unit]
    def stop(self, unit):
        self.operations.append(('stop', unit))
        self.states[unit] = False
    def start(self, unit):
        self.operations.append(('start', unit))
        self.states[unit] = True
    def read_override(self, path): return self.files.get(path)
    def write_override(self, path, value):
        assert self.locked
        self.files[path] = value
        if self.fail_write:
            raise RuntimeError('injected failure')
    def restore_override(self, path, value):
        assert self.locked
        self.files[path] = value
    def daemon_reload(self): pass
    def wait_for_resident_health(self):
        self.health_checked_while_locked = self.locked
        if self.fail_health:
            self.states[RESIDENT] = False
            raise RuntimeError('resident_health_unstable')
    def wait_for_stream_health(self, unit):
        assert unit in STREAM_SERVICES
        assert self.locked
        self.stream_health_checks += 1
        if self.fail_stream_health and self.stream_health_checks == 1:
            self.states[unit] = False
            raise RuntimeError('stream_health_unstable')

    @contextmanager
    def lock(self):
        self.locked = True
        try: yield
        finally: self.locked = False


def test_failure_restores_previous_configuration_and_states():
    platform = Platform(fail_write=True)
    with pytest.raises(RuntimeError, match='injected failure'):
        switch(platform=platform, changes={'override': 'new'}, verify_bundle=lambda: None,
               lock=platform.lock())
    assert platform.files['override'] == 'old'
    assert platform.states[RESIDENT]
    assert all(platform.states[timer] for timer in TIMERS)


def test_running_oneshot_refuses_switch_and_restores_timers():
    platform = Platform(running_oneshot=True)
    with pytest.raises(RuntimeError, match='oneshot_running'):
        switch(platform=platform, changes={'override': 'new'}, verify_bundle=lambda: None,
               lock=platform.lock())
    assert platform.files['override'] == 'old'
    assert all(platform.states[timer] for timer in TIMERS)


def test_switch_can_leave_unrelated_timer_running():
    platform = Platform()
    switch(platform=platform, changes={'override': 'new'}, verify_bundle=lambda: None,
           lock=platform.lock(), timers=(TIMERS[0],))
    assert platform.states[TIMERS[0]]
    assert platform.states[TIMERS[1]]
    assert ('stop', TIMERS[0]) in platform.operations
    assert ('start', TIMERS[0]) in platform.operations
    assert ('stop', TIMERS[1]) not in platform.operations
    assert ('start', TIMERS[1]) not in platform.operations


def test_candidate_that_exits_during_stability_window_is_rolled_back():
    platform = Platform(fail_health=True)
    with pytest.raises(RuntimeError, match='resident_health_unstable'):
        switch(platform=platform, changes={'override': 'new'}, verify_bundle=lambda: None,
               lock=platform.lock())
    assert platform.health_checked_while_locked
    assert platform.files['override'] == 'old'
    assert platform.states[RESIDENT]
    assert all(platform.states[timer] for timer in TIMERS)


def test_failed_rollback_keeps_timers_stopped():
    platform = Platform(fail_write=True)
    def fail_restore(path, value):
        raise RuntimeError('restore failed')
    platform.restore_override = fail_restore
    with pytest.raises(RuntimeError, match='release_rollback_failed'):
        switch(platform=platform, changes={'override': 'new'}, verify_bundle=lambda: None,
               lock=platform.lock())
    assert all(not platform.states[timer] for timer in TIMERS)


def test_failure_to_restore_one_timer_does_not_skip_the_other():
    platform = Platform()
    start = platform.start
    attempted = []
    def fail_first_timer(unit):
        attempted.append(unit)
        if unit == TIMERS[0]:
            raise RuntimeError('timer failure')
        start(unit)
    platform.start = fail_first_timer
    with pytest.raises(RuntimeError, match='timer_restore_failed'):
        switch(platform=platform, changes={'override': 'new'}, verify_bundle=lambda: None,
               lock=platform.lock())
    assert TIMERS[1] in attempted
    assert platform.states[TIMERS[1]]


def test_uncertain_timer_state_is_not_blindly_restarted():
    platform = Platform()
    original_is_active = platform.is_active
    checks = {TIMERS[0]: 0}
    starts = []

    def uncertain_is_active(unit):
        if unit == TIMERS[0]:
            checks[unit] += 1
            if checks[unit] > 1:
                raise RuntimeError('unit_state_uncertain')
        return original_is_active(unit)

    def uncertain_stop(unit):
        if unit == TIMERS[0]:
            raise TimeoutError('stop outcome uncertain')
        platform.states[unit] = False

    original_start = platform.start

    def record_start(unit):
        starts.append(unit)
        original_start(unit)

    platform.is_active = uncertain_is_active
    platform.stop = uncertain_stop
    platform.start = record_start
    with pytest.raises(RuntimeError, match='timer_restore_failed'):
        switch(platform=platform, changes={'override': 'new'}, verify_bundle=lambda: None,
               lock=platform.lock())
    assert TIMERS[0] not in starts
    assert TIMERS[1] not in starts
    assert platform.states[TIMERS[1]]


def test_selected_stream_is_restarted_and_health_checked_under_lock():
    platform = Platform()
    stream = STREAM_SERVICES[0]
    platform.states[stream] = True

    switch(platform=platform, changes={'override': 'new'}, verify_bundle=lambda: None,
           lock=platform.lock(), services=(stream,), expected_service_states={stream: True})

    assert platform.states[stream]
    assert platform.stream_health_checks == 1
    assert ('stop', stream) in platform.operations
    assert ('start', stream) in platform.operations


def test_inactive_stream_stays_inactive_when_selected():
    platform = Platform()
    stream = STREAM_SERVICES[0]

    switch(platform=platform, changes={'override': 'new'}, verify_bundle=lambda: None,
           lock=platform.lock(), services=(stream,), expected_service_states={stream: False})

    assert not platform.states[stream]
    assert ('stop', stream) not in platform.operations
    assert ('start', stream) not in platform.operations
    assert platform.stream_health_checks == 0


def test_stream_health_failure_restores_configuration_and_original_active_state():
    platform = Platform(fail_stream_health=True)
    stream = STREAM_SERVICES[0]
    platform.states[stream] = True

    with pytest.raises(RuntimeError, match='stream_health_unstable'):
        switch(platform=platform, changes={'override': 'new'}, verify_bundle=lambda: None,
               lock=platform.lock(), services=(stream,))

    assert platform.files['override'] == 'old'
    assert platform.states[stream]
    assert platform.stream_health_checks == 2
    assert all(platform.states[timer] for timer in TIMERS)


def test_third_stop_timeout_restores_previous_active_state_without_stopping_all_again():
    platform = Platform()
    stream = STREAM_SERVICES[0]
    platform.states[stream] = True
    original_stop = platform.stop
    attempted_stops = []

    def timeout_after_third_stop(unit):
        attempted_stops.append(unit)
        if len(attempted_stops) == 3:
            platform.operations.append(('stop', unit))
            platform.states[unit] = False
            raise TimeoutError('synthetic systemd stop timeout')
        original_stop(unit)

    platform.stop = timeout_after_third_stop

    with pytest.raises(TimeoutError, match='synthetic systemd stop timeout'):
        switch(
            platform=platform,
            changes={'override': 'new'},
            verify_bundle=lambda: None,
            lock=platform.lock(),
            timers=(TIMERS[0],),
            services=(stream,),
        )

    assert platform.files['override'] == 'old'
    assert attempted_stops == [TIMERS[0], stream, RESIDENT]
    assert [unit for operation, unit in platform.operations if operation == 'stop'] == attempted_stops
    assert platform.states[RESIDENT]
    assert platform.states[stream]
    assert platform.states[TIMERS[0]]
    assert platform.states[TIMERS[1]]


def test_stream_state_change_after_preflight_fails_before_override_change():
    platform = Platform()
    stream = STREAM_SERVICES[0]
    original_is_active = platform.is_active
    checks = 0

    def change_stream_state_during_preflight(unit):
        nonlocal checks
        if unit == stream:
            checks += 1
            if checks > 1:
                platform.states[stream] = True
        return original_is_active(unit)

    platform.is_active = change_stream_state_during_preflight

    with pytest.raises(RuntimeError, match='stream_state_changed_during_preflight'):
        switch(platform=platform, changes={'override': 'new'}, verify_bundle=lambda: None,
               lock=platform.lock(), services=(stream,))

    assert platform.files['override'] == 'old'


def test_successful_switch_can_restore_saved_configuration(tmp_path, monkeypatch):
    import json
    from deploy import release_command
    platform = Platform()
    unit = 'hermes-dingtalk-history-resident'
    override = str(tmp_path / (unit + '.service.d') / '90-dingtodo-release.conf')
    monkeypatch.setattr(release_command, 'allowed_override_paths', lambda: {override})
    platform.files[override] = 'old'
    record = tmp_path / 'rollback.json'
    release_command.save_rollback_record(record, platform, {override: 'new'})
    switch(platform=platform, changes={override: 'new'}, verify_bundle=lambda: None,
           lock=platform.lock())
    assert platform.files[override] == 'new'
    release_command.rollback(json.loads(record.read_text()), platform=platform, lock=platform.lock())
    assert platform.files[override] == 'old'
    assert platform.states[RESIDENT]


def test_rollback_record_rejects_unrelated_override(tmp_path):
    import json
    from deploy.release_command import load_rollback_record
    record = tmp_path / 'record.json'
    record.write_text(json.dumps({'previous': {'/tmp/unrelated': None},
                                  'installed': {'/tmp/unrelated': 'modified'}}))
    record.chmod(0o600)
    with pytest.raises(ValueError, match='rollback_record_invalid'):
        load_rollback_record(record)
