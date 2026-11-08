"""Transactional deployment orchestration; platform operations are injected."""
from __future__ import annotations

from typing import Any


TIMERS = ('hermes-dingtalk-daily-recovery.timer', 'cifra-review-inbox.timer')
ONESHOTS = ('hermes-dingtalk-daily-recovery.service', 'cifra-review-inbox.service')
RESIDENT = 'hermes-dingtalk-history-resident.service'
STREAM_SERVICES = ('cifra-review-stream.service',)


def switch(
    *,
    platform: Any,
    changes: dict[str, str | None],
    verify_bundle,
    lock,
    timers: tuple[str, ...] = TIMERS,
    services: tuple[str, ...] = (),
    restore_service_states: dict[str, bool] | None = None,
    expected_service_states: dict[str, bool] | None = None,
) -> None:
    """Switch prepared overrides, restoring prior state if any step fails.

    `lock` must use the same work lock as production. It remains held through
    resident stability observation so an unverified candidate cannot enter a
    production work pass. This checks process stability, not business health.
    `platform` must raise on uncertain or failed systemd/file operations.
    """
    selected_timers = tuple(timers)
    if (len(selected_timers) != len(set(selected_timers))
            or any(timer not in TIMERS for timer in selected_timers)):
        raise ValueError('switch_timers_invalid')
    selected_services = tuple(services)
    if (len(selected_services) != len(set(selected_services))
            or any(service not in STREAM_SERVICES for service in selected_services)):
        raise ValueError('switch_services_invalid')

    for service_states in (restore_service_states, expected_service_states):
        if (service_states is not None
                and (not isinstance(service_states, dict)
                     or not set(service_states) <= set(selected_services)
                     or any(type(active) is not bool for active in service_states.values()))):
            raise ValueError('switch_service_states_invalid')

    originals = {path: platform.read_override(path) for path in changes}
    timer_states = {timer: platform.is_active(timer) for timer in selected_timers}
    service_states = {service: platform.is_active(service) for service in selected_services}
    target_service_states = dict(service_states)
    if restore_service_states is not None:
        target_service_states.update(restore_service_states)
    resident_was_active = platform.is_active(RESIDENT)
    candidate_services_started: set[str] = set()
    candidate_resident_started = False
    override_mutation_attempted = False
    safe_to_restore_timers = True

    def restore_previous_configuration() -> None:
        failures: list[Exception] = []
        if override_mutation_attempted:
            for service in selected_services:
                if service not in candidate_services_started:
                    continue
                try:
                    if platform.is_active(service):
                        platform.stop(service)
                except Exception as exc:
                    failures.append(exc)
            if candidate_resident_started:
                try:
                    if platform.is_active(RESIDENT):
                        platform.stop(RESIDENT)
                except Exception as exc:
                    failures.append(exc)
            if failures:
                raise RuntimeError('rollback_candidate_stop_failed') from failures[0]

            for path, content in originals.items():
                try:
                    platform.restore_override(path, content)
                except Exception as exc:
                    failures.append(exc)
            if failures:
                raise RuntimeError('rollback_override_restore_failed') from failures[0]
            platform.daemon_reload()

        def restore_active_state(unit: str, was_active: bool, *, stream: bool = False) -> None:
            try:
                is_active = platform.is_active(unit)
                if was_active and not is_active:
                    platform.start(unit)
                    is_active = platform.is_active(unit)
                    if not is_active:
                        raise RuntimeError('rollback_unit_not_active')
                    if stream:
                        platform.wait_for_stream_health(unit)
                elif not was_active and is_active:
                    raise RuntimeError('rollback_unit_state_changed')
            except Exception as exc:
                failures.append(exc)

        restore_active_state(RESIDENT, resident_was_active)
        for service in selected_services:
            restore_active_state(service, service_states[service], stream=True)
        if failures:
            raise RuntimeError('rollback_previous_state_restore_failed') from failures[0]

    try:
        for timer, active in timer_states.items():
            if active:
                platform.stop(timer)
        with lock:
            verify_bundle()
            if any(platform.read_override(path) != content for path, content in originals.items()):
                raise RuntimeError('override_changed_during_preflight')
            if any(platform.is_active(service) != active
                   for service, active in service_states.items()):
                raise RuntimeError('stream_state_changed_during_preflight')
            if (expected_service_states is not None
                    and any(service_states[service] != active
                            for service, active in expected_service_states.items())):
                raise RuntimeError('stream_state_changed_during_preflight')
            if any(platform.is_active(unit) for unit in ONESHOTS):
                raise RuntimeError('oneshot_running')
            try:
                for service in selected_services:
                    if service_states[service]:
                        platform.stop(service)
                if resident_was_active:
                    platform.stop(RESIDENT)
                override_mutation_attempted = True
                for path, content in changes.items():
                    if content is None:
                        platform.restore_override(path, None)
                    else:
                        platform.write_override(path, content)
                platform.daemon_reload()
                if resident_was_active:
                    candidate_resident_started = True
                    platform.start(RESIDENT)
                    platform.wait_for_resident_health()
                for service in selected_services:
                    if target_service_states[service]:
                        candidate_services_started.add(service)
                        platform.start(service)
                        platform.wait_for_stream_health(service)
            except BaseException:
                try:
                    restore_previous_configuration()
                except BaseException as exc:
                    safe_to_restore_timers = False
                    raise RuntimeError('release_rollback_failed') from exc
                raise
    finally:
        failed_timers = []
        for timer, active in timer_states.items():
            if active and safe_to_restore_timers:
                try:
                    if platform.is_active(timer):
                        continue
                    platform.start(timer)
                    if not platform.is_active(timer):
                        raise RuntimeError('timer_not_active')
                except Exception:
                    failed_timers.append(timer)
        if failed_timers:
            raise RuntimeError('timer_restore_failed:' + ','.join(failed_timers))
