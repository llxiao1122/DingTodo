"""Build a release switch plan; apply uses the transactional switch adapter."""
from __future__ import annotations

import argparse
import json
import os
import re
import stat
import unicodedata
from pathlib import Path

from deploy.build_bundle import verify_bundle
from deploy.render_switch import render
from deploy.switch_transaction import STREAM_SERVICES, switch
from deploy.systemd_platform import SystemdPlatform, work_lock


DEFAULT_UNITS = ('hermes-dingtalk-history-resident', 'hermes-dingtalk-daily-recovery', 'cifra-review-inbox')
STREAM_RELEASE_UNITS = tuple(service.removesuffix('.service') for service in STREAM_SERVICES)
UNITS = (*DEFAULT_UNITS, *STREAM_RELEASE_UNITS)
SYSTEMD_ROOT = Path('/etc/systemd/system')
WORK_LOCK = Path('/root/.hermes/dingtalk-listener-state/work.lock')

MAX_CIFRA_APP_ID_LENGTH = 256


def _is_valid_cifra_app_id(value: str | None) -> bool:
    return (
        isinstance(value, str)
        and bool(value)
        and len(value) <= MAX_CIFRA_APP_ID_LENGTH
        and value == value.strip()
        and all(unicodedata.category(character) != 'Cc' for character in value)
    )


def _escape_systemd_environment_value(value: str) -> str:
    return value.replace('\\', '\\\\').replace('"', '\\"').replace('%', '%%')


def allowed_override_paths(root: Path = SYSTEMD_ROOT) -> set[str]:
    return {str(root / (unit + '.service.d') / '90-dingtodo-release.conf') for unit in UNITS}


def _normalize_units(units: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
    selected = tuple(DEFAULT_UNITS if units is None else units)
    if (not selected or len(selected) != len(set(selected))
            or any(unit not in UNITS for unit in selected)):
        raise ValueError('release_units_invalid')
    return selected


def _units_for_override_paths(paths) -> tuple[str, ...]:
    selected_paths = set(paths)
    allowed_paths = allowed_override_paths()
    if not selected_paths or not selected_paths <= allowed_paths:
        raise ValueError('release_override_paths_invalid')
    path_to_unit = {}
    for path in allowed_paths:
        candidate = Path(path)
        directory_name = candidate.parent.name
        suffix = '.service.d'
        if candidate.name != '90-dingtodo-release.conf' or not directory_name.endswith(suffix):
            continue
        unit = directory_name[:-len(suffix)]
        if unit in UNITS:
            path_to_unit[path] = unit
    return tuple(unit for unit in UNITS
                 if any(path_to_unit.get(path) == unit for path in selected_paths))


def load_rollback_record(path: Path) -> dict:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        parent = path.parent.stat()
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) & 0o077 or parent.st_uid != os.geteuid()
                or stat.S_IMODE(parent.st_mode) & 0o022):
            raise ValueError('rollback_record_permissions_unsafe')
        with os.fdopen(fd, 'r', closefd=False) as file:
            record = json.load(file)
    finally:
        os.close(fd)
    if (not isinstance(record, dict) or not isinstance(record.get('previous'), dict)
            or not isinstance(record.get('installed'), dict)
            or not record['previous']
            or set(record['previous']) != set(record['installed'])
            or not set(record['previous']) <= allowed_override_paths()
            or any(v is not None and not isinstance(v, str) for v in record['previous'].values())
            or any(not isinstance(v, str) for v in record['installed'].values())):
        raise ValueError('rollback_record_invalid')
    units = _units_for_override_paths(record['installed'])
    states = record.get('service_states')
    stream_selected = any(unit in STREAM_RELEASE_UNITS for unit in units)
    if stream_selected:
        if (not isinstance(states, dict) or set(states) != set(STREAM_RELEASE_UNITS)
                or any(type(active) is not bool for active in states.values())):
            raise ValueError('rollback_record_invalid')
    elif states is not None:
        if (not isinstance(states, dict) or states
                or any(type(active) is not bool for active in states.values())):
            raise ValueError('rollback_record_invalid')
    return record


def save_rollback_record(path: Path, platform: SystemdPlatform, changes: dict[str, str],
                         *, units: tuple[str, ...] | list[str] | None = None) -> dict:
    if not changes or not set(changes) <= allowed_override_paths():
        raise ValueError('release_override_paths_invalid')
    selected_units = _units_for_override_paths(changes) if units is None else _normalize_units(units)
    if set(selected_units) != set(_units_for_override_paths(changes)):
        raise ValueError('release_units_mismatch')
    if path.is_symlink():
        raise ValueError('rollback_record_symlink')
    parent = path.parent.stat()
    if parent.st_uid != os.geteuid() or stat.S_IMODE(parent.st_mode) & 0o022:
        raise ValueError('rollback_record_permissions_unsafe')
    record = {'previous': {name: platform.read_override(name) for name in changes}, 'installed': changes}
    if any(unit in STREAM_RELEASE_UNITS for unit in selected_units):
        record['service_states'] = {
            unit: platform.is_active(unit + '.service')
            for unit in STREAM_RELEASE_UNITS if unit in selected_units
        }
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as file:
        json.dump(record, file, sort_keys=True)
        file.flush()
        os.fsync(file.fileno())
    directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    return record


def rollback(record: dict, *, platform: SystemdPlatform, lock) -> None:
    previous, installed = record.get('previous'), record.get('installed')
    if (not isinstance(previous, dict) or not isinstance(installed, dict) or not previous
            or set(previous) != set(installed) or not set(previous) <= allowed_override_paths()):
        raise ValueError('rollback_record_invalid')
    def verify_current():
        if any(platform.read_override(path) != content for path, content in installed.items()):
            raise ValueError('rollback_current_configuration_changed')
    units = _units_for_override_paths(previous)
    services = tuple(unit + '.service' for unit in units if unit in STREAM_RELEASE_UNITS)
    recorded_states = record.get('service_states')
    if services:
        if (not isinstance(recorded_states, dict)
                or set(recorded_states) != set(STREAM_RELEASE_UNITS)
                or any(type(active) is not bool for active in recorded_states.values())):
            raise ValueError('rollback_record_invalid')
    elif recorded_states is not None and recorded_states != {}:
        raise ValueError('rollback_record_invalid')
    if recorded_states is None:
        recorded_states = {}
    restore_states = {service: recorded_states[service[:-len('.service')]] for service in services}
    switch(platform=platform, changes=previous, verify_bundle=verify_current, lock=lock,
           services=services, restore_service_states=restore_states)


def plan(bundle: Path, metadata: dict, *, cifra_app_id: str | None = None,
         scheduler_owner_open_id: str | None = None, units: tuple[str, ...] | list[str] | None = None,
         systemd_root: Path = SYSTEMD_ROOT) -> dict[str, str]:
    selected_units = _normalize_units(units)
    includes_cifra_inbox = 'cifra-review-inbox' in selected_units
    if includes_cifra_inbox:
        if not _is_valid_cifra_app_id(cifra_app_id):
            raise ValueError('cifra_app_id_invalid')
    elif cifra_app_id is not None:
        raise ValueError('cifra_app_id_unexpected')
    if scheduler_owner_open_id is not None and not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', scheduler_owner_open_id):
        raise ValueError('scheduler_owner_open_id_invalid')
    if scheduler_owner_open_id is not None and not includes_cifra_inbox:
        raise ValueError('scheduler_owner_open_id_unexpected')
    verify_bundle(bundle, metadata)
    result = {}
    for unit in selected_units:
        directory = systemd_root / (unit + '.service.d')
        baseline = directory / '50-self-assignment.conf'
        if baseline.is_symlink() or not baseline.is_file():
            raise ValueError('service_baseline_missing:' + unit)
        content = render(baseline.read_text(), bundle)
        if unit == 'cifra-review-inbox':
            if scheduler_owner_open_id:
                lines = content.splitlines()
                starts = [i for i, line in enumerate(lines) if line.startswith('ExecStart=') and line != 'ExecStart=']
                if len(starts) != 1:
                    raise ValueError('scheduler_exec_start_invalid')
                lines[starts[0]] += ' --scheduler-owner-open-id ' + scheduler_owner_open_id
                content = '\n'.join(lines) + '\n'
            escaped_app_id = _escape_systemd_environment_value(cifra_app_id)
            content += f'Environment="DINGTODO_REVIEW_APP_ID={escaped_app_id}"\n'
        result[str(directory / '90-dingtodo-release.conf')] = content
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--bundle', type=Path)
    parser.add_argument('--trusted-metadata', type=Path)
    parser.add_argument('--cifra-app-id')
    parser.add_argument('--scheduler-owner-open-id')
    parser.add_argument('--unit', action='append', choices=UNITS, dest='units')
    parser.add_argument('--rollback-record', type=Path)
    parser.add_argument('--rollback', action='store_true')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    if args.rollback:
        if (args.rollback_record is None or args.bundle or args.trusted_metadata or args.cifra_app_id
                or args.scheduler_owner_open_id or args.units):
            raise ValueError('rollback_arguments_invalid')
        record = load_rollback_record(args.rollback_record)
        selected_units = _units_for_override_paths(record['installed'])
        platform = SystemdPlatform(allowed_override_paths())
        if any(platform.read_override(path) != content for path, content in record['installed'].items()):
            raise ValueError('rollback_current_configuration_changed')
        if args.apply:
            rollback(record, platform=platform, lock=work_lock(WORK_LOCK))
        print(json.dumps({'status': 'rolled_back' if args.apply else 'rollback_preview',
                          'units': list(selected_units)}))
        return 0
    selected_units = _normalize_units(args.units)
    if not args.bundle or not args.trusted_metadata:
        raise ValueError('release_arguments_missing')
    if not args.bundle.is_absolute() or args.bundle.is_symlink():
        raise ValueError('bundle_path_invalid')
    if args.trusted_metadata.is_symlink() or not args.trusted_metadata.is_file():
        raise ValueError('trusted_metadata_invalid')
    metadata_stat = args.trusted_metadata.stat()
    metadata_parent = args.trusted_metadata.parent.stat()
    if (metadata_stat.st_uid != os.geteuid() or stat.S_IMODE(metadata_stat.st_mode) & 0o077
            or metadata_parent.st_uid != os.geteuid() or stat.S_IMODE(metadata_parent.st_mode) & 0o022):
        raise ValueError('trusted_metadata_permissions_unsafe')
    metadata = json.loads(args.trusted_metadata.read_text())
    changes = plan(args.bundle, metadata, cifra_app_id=args.cifra_app_id,
                   scheduler_owner_open_id=args.scheduler_owner_open_id, units=selected_units)
    if not args.apply:
        print(json.dumps({'status': 'preview', 'commit': metadata['commit'],
                          'units': list(selected_units)}))
        return 0
    if args.rollback_record is None:
        raise ValueError('rollback_record_required')
    platform = SystemdPlatform(set(changes))
    record = save_rollback_record(args.rollback_record, platform, changes, units=selected_units)
    def verify_for_switch():
        verify_bundle(args.bundle, metadata)
        if any(platform.read_override(path) != content for path, content in record['previous'].items()):
            raise ValueError('rollback_baseline_changed')
    selected_services = tuple(unit + '.service' for unit in selected_units
                              if unit in STREAM_RELEASE_UNITS)
    expected_service_states = {
        service: record['service_states'][service[:-len('.service')]]
        for service in selected_services
    }
    switch(platform=platform, changes=changes,
           verify_bundle=verify_for_switch, lock=work_lock(WORK_LOCK),
           services=selected_services, expected_service_states=expected_service_states)
    print(json.dumps({'status': 'switched', 'commit': metadata['commit']}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
