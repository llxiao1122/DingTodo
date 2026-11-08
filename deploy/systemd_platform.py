"""Strict systemd and atomic-file adapter for release transactions."""
from __future__ import annotations

import fcntl
import os
import re
import subprocess
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

from deploy.switch_transaction import ONESHOTS, RESIDENT, STREAM_SERVICES, TIMERS

RESIDENT_HEALTH_STABILITY_SECONDS = 35.0
RESIDENT_HEALTH_POLL_INTERVAL_SECONDS = 1.0
SYSTEMD_COMMAND_TIMEOUT_SECONDS = 60.0
SYSTEMD_STOP_TIMEOUT_MARGIN_SECONDS = 60.0
SYSTEMD_STOP_TIMEOUT_MAX_SECONDS = 3600.0


def _systemd_timeout_seconds(value: str) -> float:
    """Parse the normalized duration emitted for systemd *USec properties."""
    units = {
        "us": 0.000001,
        "usec": 0.000001,
        "µs": 0.000001,
        "ms": 0.001,
        "msec": 0.001,
        "s": 1.0,
        "sec": 1.0,
        "min": 60.0,
        "m": 60.0,
        "h": 3600.0,
        "d": 86400.0,
        "w": 604800.0,
    }
    if not isinstance(value, str) or not value.strip() or value.strip().lower() == "infinity":
        raise ValueError("unit_stop_timeout_invalid")
    text = value.strip()
    token_pattern = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*(usec|us|µs|msec|ms|sec|min|s|m|h|d|w)")
    position = 0
    total = 0.0
    matches = 0
    for match in token_pattern.finditer(text):
        if text[position:match.start()].strip():
            raise ValueError("unit_stop_timeout_invalid")
        total += float(match.group(1)) * units[match.group(2)]
        position = match.end()
        matches += 1
    if text[position:].strip() or matches == 0 or total <= 0 or total > SYSTEMD_STOP_TIMEOUT_MAX_SECONDS:
        raise ValueError("unit_stop_timeout_invalid")
    return total


class SystemdPlatform:
    def __init__(self, allowed_overrides: set[str]):
        self.allowed_overrides = frozenset(allowed_overrides)
        self._monotonic = time.monotonic
        self._sleep = time.sleep

    def _command(self, *args: str, timeout: float = SYSTEMD_COMMAND_TIMEOUT_SECONDS) -> subprocess.CompletedProcess:
        return subprocess.run(['systemctl', *args], capture_output=True, text=True,
                              timeout=timeout, check=True)

    def is_active(self, unit: str) -> bool:
        if unit not in (*ONESHOTS, *TIMERS, RESIDENT, *STREAM_SERVICES):
            raise ValueError('unit_not_allowed')
        state = self._command('show', unit, '--property=ActiveState', '--value').stdout.strip()
        if state not in ('active', 'inactive'):
            raise RuntimeError('unit_state_uncertain:' + unit)
        return state == 'active'

    def wait_for_resident_health(
        self,
        *,
        stability_seconds: float = RESIDENT_HEALTH_STABILITY_SECONDS,
        poll_interval_seconds: float = RESIDENT_HEALTH_POLL_INTERVAL_SECONDS,
    ) -> None:
        if stability_seconds <= 0 or poll_interval_seconds <= 0:
            raise ValueError('resident_health_window_invalid')

        deadline = self._monotonic() + stability_seconds
        baseline_identity: tuple[int, str] | None = None
        while True:
            health = self._resident_health_snapshot()
            if (health['ActiveState'] != 'active' or health['SubState'] != 'running'
                    or health['Result'] != 'success'):
                raise RuntimeError('resident_health_check_failed')

            try:
                main_pid = int(health['MainPID'])
            except ValueError as exc:
                raise RuntimeError('resident_health_state_uncertain') from exc
            invocation_id = health['InvocationID']
            if main_pid <= 0 or not invocation_id or invocation_id == '0' * len(invocation_id):
                raise RuntimeError('resident_health_state_uncertain')

            identity = (main_pid, invocation_id)
            if baseline_identity is None:
                baseline_identity = identity
            elif identity != baseline_identity:
                raise RuntimeError('resident_health_unstable')

            remaining = deadline - self._monotonic()
            if remaining <= 0:
                return
            self._sleep(min(poll_interval_seconds, remaining))

    def wait_for_stream_health(
        self,
        unit: str,
        *,
        stability_seconds: float = RESIDENT_HEALTH_STABILITY_SECONDS,
        poll_interval_seconds: float = RESIDENT_HEALTH_POLL_INTERVAL_SECONDS,
    ) -> None:
        if unit not in STREAM_SERVICES:
            raise ValueError('unit_not_allowed')
        if stability_seconds <= 0 or poll_interval_seconds <= 0:
            raise ValueError('stream_health_window_invalid')

        deadline = self._monotonic() + stability_seconds
        baseline_identity: tuple[int, str] | None = None
        while True:
            health = self._service_health_snapshot(unit)
            if (health['ActiveState'] != 'active' or health['SubState'] != 'running'
                    or health['Result'] != 'success'):
                raise RuntimeError('stream_health_check_failed')

            try:
                main_pid = int(health['MainPID'])
            except ValueError as exc:
                raise RuntimeError('stream_health_state_uncertain') from exc
            invocation_id = health['InvocationID']
            if main_pid <= 0 or not invocation_id or invocation_id == '0' * len(invocation_id):
                raise RuntimeError('stream_health_state_uncertain')

            identity = (main_pid, invocation_id)
            if baseline_identity is None:
                baseline_identity = identity
            elif identity != baseline_identity:
                raise RuntimeError('stream_health_unstable')

            remaining = deadline - self._monotonic()
            if remaining <= 0:
                return
            self._sleep(min(poll_interval_seconds, remaining))

    def _resident_health_snapshot(self) -> dict[str, str]:
        return self._service_health_snapshot(RESIDENT)

    def _service_health_snapshot(self, unit: str) -> dict[str, str]:
        if unit not in (RESIDENT, *STREAM_SERVICES):
            raise ValueError('unit_not_allowed')
        properties = ('ActiveState', 'SubState', 'Result', 'MainPID', 'InvocationID')
        output = self._command(
            'show', unit, *(f'--property={property_name}' for property_name in properties)
        ).stdout
        values = {}
        for line in output.splitlines():
            name, separator, value = line.partition('=')
            if separator:
                values[name] = value
        if any(property_name not in values for property_name in properties):
            raise RuntimeError('resident_health_state_uncertain')
        return values

    def stop(self, unit: str) -> None:
        if unit not in (*TIMERS, RESIDENT, *STREAM_SERVICES):
            raise ValueError('unit_not_allowed')
        configured_timeout = self._command(
            'show', unit, '--property=TimeoutStopUSec', '--value',
        ).stdout.strip()
        timeout_seconds = _systemd_timeout_seconds(configured_timeout)
        self._command(
            'stop', unit,
            timeout=timeout_seconds + SYSTEMD_STOP_TIMEOUT_MARGIN_SECONDS,
        )

    def start(self, unit: str) -> None:
        if unit not in (*TIMERS, RESIDENT, *STREAM_SERVICES): raise ValueError('unit_not_allowed')
        self._command('start', unit)

    def daemon_reload(self) -> None:
        self._command('daemon-reload')

    def _path(self, value: str) -> Path:
        if value not in self.allowed_overrides: raise ValueError('override_not_allowed')
        path = Path(value)
        if path.is_symlink() or path.parent.is_symlink(): raise ValueError('override_symlink_forbidden')
        return path

    def read_override(self, value: str) -> str | None:
        path = self._path(value)
        return path.read_text() if path.exists() else None

    def write_override(self, value: str, content: str) -> None:
        path = self._path(value)
        fd, temporary = tempfile.mkstemp(prefix='.dingtodo-', dir=path.parent)
        try:
            with os.fdopen(fd, 'w') as file:
                file.write(content)
                file.flush()
                os.fsync(file.fileno())
            os.chmod(temporary, 0o600)
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary): os.unlink(temporary)

    def restore_override(self, value: str, content: str | None) -> None:
        if content is None:
            self._path(value).unlink(missing_ok=True)
        else:
            self.write_override(value, content)


@contextmanager
def work_lock(path: Path):
    fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
