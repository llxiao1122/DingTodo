#!/usr/bin/env python3
"""Thin DWS event listener that schedules the existing one-shot history worker.

Events are wakeups only. Durable state contains group IDs and generations, never
message content. The actual history read, planning, ledger and external writes
remain in event_worker.py behind its existing gates.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import hashlib
import importlib.util
import fcntl
import json
import math
import os
from pathlib import Path
import queue
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any, Callable, Iterable

try:
    from dws_runtime import build_dws_argv, run_dws_command, start_dws_stream
except ModuleNotFoundError as exc:
    if exc.name != "dws_runtime":
        raise
    _dws_runtime_path = Path(__file__).resolve().with_name("dws_runtime.py")
    _dws_runtime_spec = importlib.util.spec_from_file_location("_dingtodo_dws_runtime", _dws_runtime_path)
    if _dws_runtime_spec is None or _dws_runtime_spec.loader is None:
        raise RuntimeError("dws_runtime_module_unavailable") from None
    _dws_runtime_module = importlib.util.module_from_spec(_dws_runtime_spec)
    _dws_runtime_spec.loader.exec_module(_dws_runtime_module)
    build_dws_argv = _dws_runtime_module.build_dws_argv
    run_dws_command = _dws_runtime_module.run_dws_command
    start_dws_stream = _dws_runtime_module.start_dws_stream
try:
    from dws_parity import DwsParityError, DwsParityLedger
except ModuleNotFoundError as exc:
    if exc.name != "dws_parity":
        raise
    _dws_parity_path = Path(__file__).resolve().with_name("dws_parity.py")
    _dws_parity_spec = importlib.util.spec_from_file_location("_dingtodo_dws_parity", _dws_parity_path)
    if _dws_parity_spec is None or _dws_parity_spec.loader is None:
        raise RuntimeError("dws_parity_module_unavailable") from None
    _dws_parity_module = importlib.util.module_from_spec(_dws_parity_spec)
    _dws_parity_spec.loader.exec_module(_dws_parity_module)
    DwsParityError = _dws_parity_module.DwsParityError
    DwsParityLedger = _dws_parity_module.DwsParityLedger

try:
    from event_lifecycle import normalize_todo_lifecycle_event, validate_queued_todo_event
except ModuleNotFoundError as exc:
    # ``python -I listener_engine.py`` deliberately omits the script directory
    # from sys.path. Load only the adjacent, workspace-owned lifecycle module.
    if exc.name != "event_lifecycle":
        raise
    _lifecycle_path = Path(__file__).resolve().with_name("event_lifecycle.py")
    _lifecycle_spec = importlib.util.spec_from_file_location("_dingtodo_event_lifecycle", _lifecycle_path)
    if _lifecycle_spec is None or _lifecycle_spec.loader is None:
        raise RuntimeError("event_lifecycle_module_unavailable") from None
    _lifecycle_module = importlib.util.module_from_spec(_lifecycle_spec)
    _lifecycle_spec.loader.exec_module(_lifecycle_module)
    normalize_todo_lifecycle_event = _lifecycle_module.normalize_todo_lifecycle_event
    validate_queued_todo_event = _lifecycle_module.validate_queued_todo_event


CHINA_TZ = timezone(timedelta(hours=8))
READY_RE = re.compile(r"^\[event\] ready (?:event_key=\S+|event_count=[1-9][0-9]*)(?:\s|$)")
SUBSCRIPTION_RECEIPT_RE = re.compile(
    r"^\[event\] subscription event_key=([A-Za-z0-9_.-]+) subscribe_id=(\S+)$"
)
READY_RECEIPT_RE = re.compile(
    r"^\[event\] ready event_key=([A-Za-z0-9_.-]+) bus_pid=[0-9]+ subscribe_id=(\S+)$"
)
SUBSCRIBE_ID_RE = re.compile(r"[^\s\x00-\x1f\x7f]{1,256}\Z")
SAFE_REASON_RE = re.compile(r"[A-Za-z0-9_.:-]{1,80}\Z")
MAX_STARTUP_JSON_CHARS = 64_000
MAX_STARTUP_JSON_LINES = 128
MAX_LINE_CHARS = 1_000_000
MAX_PENDING_HISTORY_EVENTS_PER_GROUP = 2000
HISTORY_EVENT_ID_RE = re.compile(r"[a-f0-9]{64}\Z")
HISTORY_EVENT_ERROR_CODES = frozenset({
    "history_event_message_id_missing",
    "history_event_time_missing",
    "history_event_time_invalid",
    "history_event_identity_mismatch",
    "history_event_queue_capacity_exceeded",
})
DEFAULT_COALESCE_SECONDS = 0.75
MAX_COALESCE_SECONDS = 5.0
DEFAULT_READY_TIMEOUT = 60.0
TODO_EVENT_KEYS = (
    "user_todo_task_create",
    "user_todo_task_update",
    "user_todo_task_delete",
)
TODO_CONSUMER_ID = "todo-lifecycle"
TODO_ROLE_TYPES = "creator,executor,participant"
POLARDB_ROOT = Path(os.environ.get("HERMES_POLARDB_RUNTIME", "/root/.hermes/polardb_runtime"))
if str(POLARDB_ROOT) not in sys.path:
    sys.path.insert(0, str(POLARDB_ROOT))


class ScopeError(ValueError):
    """Authoritative configured group scope is missing or ambiguous."""


def _validate_group_ids(group_ids: Iterable[str]) -> tuple[str, ...]:
    try:
        values = tuple(group_ids)
    except TypeError:
        raise ScopeError("configured_group_scope_invalid") from None
    if (
        not values
        or any(not isinstance(value, str) or not value.strip() for value in values)
        or len(values) != len(set(values))
    ):
        raise ScopeError("configured_group_scope_invalid")
    return values


class StateError(RuntimeError):
    """Durable listener state is unreadable or unsafe; fail closed."""


def safe_log(record: dict[str, Any]) -> None:
    """Emit only caller-constructed status fields; never serialize event/provider data."""
    safe: dict[str, Any] = {}
    statuses = {"listener_started", "listener_starting", "listener_ready", "subscriber_unavailable",
                "SMOKE_COMPLETE", "COMPLETED", "REVIEW", "SKIPPED", "IDLE", "DEFERRED",
                "UNKNOWN", "BLOCKED", "PARITY_UNAVAILABLE"}
    status = record.get("status")
    if isinstance(status, str) and status in statuses:
        safe["status"] = status
    for key in (
        "configured_groups", "ready_consumers", "blocked_consumers", "event_count", "pending_groups",
        "attempted_groups", "ready_markers", "failed_groups", "canceled_groups", "cleanup_pending",
    ):
        value = record.get(key)
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            safe[key] = value
    if isinstance(record.get("parity_observer_enabled"), bool):
        safe["parity_observer_enabled"] = record["parity_observer_enabled"]
    if isinstance(record.get("delivery_coverage"), str) and record.get("delivery_coverage") in {
        "unverified_by_ready_marker", "observed_events_only",
    }:
        safe["delivery_coverage"] = record["delivery_coverage"]
    reason = record.get("reason")
    if isinstance(reason, str) and re.fullmatch(r"[A-Za-z0-9_]{1,80}", reason):
        safe["reason"] = reason
    print(json.dumps(safe, ensure_ascii=True, sort_keys=True), flush=True)


def is_ready_marker(line: str) -> bool:
    return bool(READY_RE.search(line.strip()))


def normalize_wakeup(payload: Any) -> str | None:
    """Extract only a documented conversation identity from a flat/enveloped event."""
    aliases = (
        "conversation_id", "conversationId", "openConversationId", "open_conversation_id",
    )
    containers = ("data", "payload", "body", "event")
    visited: set[int] = set()

    def find(value: Any) -> str | None:
        if not isinstance(value, dict) or id(value) in visited:
            return None
        visited.add(id(value))
        for key in aliases:
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate
        for key in containers:
            candidate = value.get(key)
            if isinstance(candidate, dict):
                found = find(candidate)
                if found:
                    return found
            elif isinstance(candidate, list):
                for item in candidate:
                    found = find(item)
                    if found:
                        return found
        return None

    return find(payload)


def _find_history_event_message(payload: Any) -> dict[str, Any] | None:
    id_aliases = ("messageId", "message_id", "openMessageId", "open_message_id")
    containers = ("data", "payload", "body", "event", "content")
    pending = [payload]
    visited: set[int] = set()
    while pending:
        value = pending.pop()
        if isinstance(value, dict):
            if id(value) in visited:
                continue
            visited.add(id(value))
            if any(key in value for key in id_aliases):
                return value
            for key in containers:
                nested = value.get(key)
                if isinstance(nested, (dict, list)):
                    pending.append(nested)
            for nested in value.values():
                if isinstance(nested, (dict, list)):
                    pending.append(nested)
        elif isinstance(value, list):
            pending.extend(value)
    return None


def _normalize_history_event_time(value: Any) -> str | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        if isinstance(value, (int, float)):
            timestamp = float(value)
            if not math.isfinite(timestamp) or timestamp <= 0:
                return None
            if timestamp > 100_000_000_000:
                timestamp /= 1000
            parsed = datetime.fromtimestamp(timestamp, tz=CHINA_TZ)
        elif isinstance(value, str):
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=CHINA_TZ)
            parsed = parsed.astimezone(CHINA_TZ)
        else:
            return None
    except (OverflowError, OSError, TypeError, ValueError):
        return None
    return parsed.isoformat()


def _history_event_hash(group_id: str, message_id: str) -> str:
    return hashlib.sha256(f"{group_id}\0{message_id}".encode("utf-8")).hexdigest()


def configured_group_ids(runtime: Any) -> tuple[str, ...]:
    """Return only authoritative ``todo_scan`` IDs; any scope defect blocks all."""
    if not isinstance(runtime, dict):
        raise ScopeError("runtime_config_unavailable")
    config = runtime.get("config") if isinstance(runtime.get("config"), dict) else runtime
    rows = config.get("config_group") if isinstance(config, dict) else None
    if not isinstance(rows, list):
        raise ScopeError("configured_group_scope_unavailable")
    if any(not isinstance(row, dict) for row in rows):
        raise ScopeError("configured_group_row_invalid")
    scoped = [row for row in rows if str(row.get("purpose") or "") == "todo_scan"]
    if not scoped:
        raise ScopeError("configured_group_scope_empty")
    ids: list[str] = []
    for row in scoped:
        value = row.get("conversation_id")
        if not isinstance(value, str) or not value.strip():
            raise ScopeError("configured_conversation_ids_missing")
        ids.append(value)
    if len(ids) != len(set(ids)):
        raise ScopeError("configured_conversation_ids_ambiguous")
    return tuple(ids)


def load_configured_group_ids(adapter: Any) -> tuple[str, ...]:
    """Read the authoritative config once, failing closed on adapter errors."""
    try:
        runtime = adapter.query({"view": "runtime_config"})
    except Exception:
        raise ScopeError("runtime_config_unavailable") from None
    return configured_group_ids(runtime)


def consume_argv(group_id: str, executable: str = "dws") -> list[str]:
    """Build the official group-scoped IM listener command as flattened NDJSON."""
    if not isinstance(group_id, str) or not group_id.strip():
        raise ScopeError("configured_conversation_ids_missing")
    return build_dws_argv(
        ["event", "+listen-im", "--kind", "group", "--chat-id", group_id,
         "-f", "ndjson"],
        executable=executable,
    )


def todo_consume_argv(executable: str = "dws") -> list[str]:
    """Build one role-scoped consume process for all three TODO lifecycle keys."""
    return build_dws_argv(
        ["event", "consume", *TODO_EVENT_KEYS, "--role-types", TODO_ROLE_TYPES,
         "--flatten", "-f", "ndjson"],
        executable=executable,
    )


def _secure_state_dir(path: Path) -> Path:
    if path.is_symlink():
        raise StateError("state_directory_invalid")
    try:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
    except OSError:
        raise StateError("state_directory_unavailable") from None
    if path.is_symlink():
        raise StateError("state_directory_invalid")
    try:
        path.chmod(0o700)
    except OSError:
        raise StateError("state_directory_permissions_unavailable") from None
    if not path.is_dir() or path.is_symlink():
        raise StateError("state_directory_invalid")
    return path


def _read_json_file(path: Path, empty: dict[str, Any]) -> dict[str, Any]:
    try:
        if path.is_symlink():
            raise StateError("state_file_symlink_rejected")
        with path.open("r", encoding="utf-8") as stream:
            payload = json.load(stream)
    except FileNotFoundError:
        return empty
    except StateError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise StateError("state_file_unreadable") from None
    if not isinstance(payload, dict):
        raise StateError("state_file_invalid")
    return payload


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    tmp_path: str | None = None
    try:
        fd, tmp_path = tempfile.mkstemp(prefix=".listener-state-", dir=path.parent)
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp_path, path)
        tmp_path = None
        dir_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError:
        raise StateError("state_write_failed") from None
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


@contextmanager
def _file_lock(path: Path):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
        os.fchmod(fd, 0o600)
    except OSError:
        raise StateError("lock_file_unavailable") from None
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


class AtomicPendingState:
    """0600 atomic pending generations under an explicitly supplied 0700 dir."""

    MAX_COMPLETED_TODO_EVENTS = 2000

    def __init__(self, state_dir: str | Path):
        self.state_dir = _secure_state_dir(Path(state_dir))
        self.path = self.state_dir / "pending.json"
        self.history_path = self.state_dir / "history-event-replay.json"
        self.lock_path = self.state_dir / "pending.lock"
        with _file_lock(self.lock_path):
            self._load_unlocked()

    def _load_unlocked(self) -> tuple[int, dict[str, int]]:
        payload = _read_json_file(self.path, {"version": 1, "generation": 0, "pending": {}})
        pending = payload.get("pending")
        generation = payload.get("generation")
        if (
            payload.get("version") != 1
            or isinstance(generation, bool)
            or not isinstance(generation, int)
            or generation < 0
            or not isinstance(pending, dict)
        ):
            raise StateError("state_file_invalid")
        normalized: dict[str, int] = {}
        for group_id, value in pending.items():
            if (
                not isinstance(group_id, str)
                or not group_id
                or isinstance(value, bool)
                or not isinstance(value, int)
                or value < 1
                or value > generation
            ):
                raise StateError("state_file_invalid")
            normalized[group_id] = value
        self._load_todo_events_unlocked(payload)
        return generation, normalized

    @staticmethod
    def _load_todo_events_unlocked(
        payload: dict[str, Any],
    ) -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]]]:
        pending = payload.get("pending_todo_events", {})
        completed = payload.get("completed_todo_events", {})
        if not isinstance(pending, dict) or not isinstance(completed, dict):
            raise StateError("todo_event_state_invalid")
        result: list[dict[str, dict[str, str]]] = []
        for section in (pending, completed):
            normalized: dict[str, dict[str, str]] = {}
            for event_id, value in section.items():
                try:
                    event = validate_queued_todo_event(value)
                except (TypeError, ValueError):
                    raise StateError("todo_event_state_invalid") from None
                if not isinstance(event_id, str) or event_id != event["event_id"]:
                    raise StateError("todo_event_state_invalid")
                normalized[event_id] = event
            result.append(normalized)
        if set(result[0]).intersection(result[1]):
            raise StateError("todo_event_state_invalid")
        return result[0], result[1]

    def _todo_event_sections_unlocked(self) -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]]]:
        payload = _read_json_file(self.path, {"version": 1, "generation": 0, "pending": {}})
        if payload.get("version") != 1:
            raise StateError("todo_event_state_invalid")
        return self._load_todo_events_unlocked(payload)

    @staticmethod
    def _load_history_event_sections_unlocked(
        payload: dict[str, Any],
    ) -> tuple[dict[str, dict[str, dict[str, str]]], dict[str, list[str]]]:
        pending = payload.get("pending_history_events", {})
        errors = payload.get("pending_history_event_errors", {})
        if not isinstance(pending, dict) or not isinstance(errors, dict):
            raise StateError("history_event_state_invalid")
        normalized_pending: dict[str, dict[str, dict[str, str]]] = {}
        for group_id, events in pending.items():
            if not isinstance(group_id, str) or not group_id or not isinstance(events, dict):
                raise StateError("history_event_state_invalid")
            if len(events) > MAX_PENDING_HISTORY_EVENTS_PER_GROUP:
                raise StateError("history_event_state_invalid")
            normalized_events: dict[str, dict[str, str]] = {}
            for event_hash, event in events.items():
                event_time = event.get("event_time") if isinstance(event, dict) else None
                normalized_time = _normalize_history_event_time(event_time)
                if (
                    not isinstance(event_hash, str)
                    or not HISTORY_EVENT_ID_RE.fullmatch(event_hash)
                    or normalized_time is None
                    or normalized_time != event_time
                    or set(event) != {"event_time"}
                ):
                    raise StateError("history_event_state_invalid")
                normalized_events[event_hash] = {"event_time": normalized_time}
            normalized_pending[group_id] = normalized_events
        normalized_errors: dict[str, list[str]] = {}
        for group_id, reasons in errors.items():
            if (
                not isinstance(group_id, str)
                or not group_id
                or not isinstance(reasons, list)
                or len(reasons) > len(HISTORY_EVENT_ERROR_CODES)
                or any(not isinstance(reason, str) or reason not in HISTORY_EVENT_ERROR_CODES for reason in reasons)
                or len(set(reasons)) != len(reasons)
            ):
                raise StateError("history_event_state_invalid")
            normalized_errors[group_id] = list(reasons)
        return normalized_pending, normalized_errors

    def _history_event_sections_unlocked(
        self,
    ) -> tuple[dict[str, dict[str, dict[str, str]]], dict[str, list[str]]]:
        if self.history_path.exists():
            if self.history_path.is_symlink():
                raise StateError("history_event_state_symlink_rejected")
            try:
                self.history_path.chmod(0o600)
            except OSError:
                raise StateError("history_event_state_permissions_unavailable") from None
        payload = _read_json_file(
            self.history_path,
            {"version": 1, "pending": {}, "errors": {}},
        )
        if payload.get("version") != 1 or set(payload) != {"version", "pending", "errors"}:
            raise StateError("history_event_state_invalid")
        return self._load_history_event_sections_unlocked({
            "pending_history_events": payload.get("pending"),
            "pending_history_event_errors": payload.get("errors"),
        })

    def _save_history_event_sections_unlocked(
        self,
        pending: dict[str, dict[str, dict[str, str]]],
        errors: dict[str, list[str]],
    ) -> None:
        _atomic_write_json(self.history_path, {
            "version": 1,
            "pending": pending,
            "errors": errors,
        })

    def _save_unlocked(
        self,
        generation: int,
        pending: dict[str, int],
        pending_todo_events: dict[str, dict[str, str]] | None = None,
        completed_todo_events: dict[str, dict[str, str]] | None = None,
    ) -> None:
        if pending_todo_events is None or completed_todo_events is None:
            old_pending, old_completed = self._todo_event_sections_unlocked()
            pending_todo_events = old_pending if pending_todo_events is None else pending_todo_events
            completed_todo_events = old_completed if completed_todo_events is None else completed_todo_events
        payload: dict[str, Any] = {"version": 1, "generation": generation, "pending": pending}
        if pending_todo_events:
            payload["pending_todo_events"] = pending_todo_events
        if completed_todo_events:
            payload["completed_todo_events"] = completed_todo_events
        _atomic_write_json(self.path, payload)

    def snapshot(self) -> dict[str, int]:
        with _file_lock(self.lock_path):
            return self._load_unlocked()[1]

    def notify(self, group_id: str) -> int:
        if not isinstance(group_id, str) or not group_id:
            raise ValueError("group_id_required")
        with _file_lock(self.lock_path):
            generation, pending = self._load_unlocked()
            generation += 1
            pending[group_id] = generation
            self._save_unlocked(generation, pending)
            return generation

    def notify_history_event(self, group_id: str, message_id: str, event_time: Any) -> int:
        if not isinstance(group_id, str) or not group_id:
            raise ValueError("group_id_required")
        if not isinstance(message_id, str) or not message_id.strip():
            raise ValueError("message_id_required")
        normalized_time = _normalize_history_event_time(event_time)
        if normalized_time is None:
            raise ValueError("event_time_invalid")
        event_hash = _history_event_hash(group_id, message_id.strip())
        with _file_lock(self.lock_path):
            generation, pending = self._load_unlocked()
            pending_events, pending_errors = self._history_event_sections_unlocked()
            group_events = pending_events.setdefault(group_id, {})
            prior = group_events.get(event_hash)
            event = {"event_time": normalized_time}
            if prior is not None and prior != event:
                raise StateError("history_event_identity_conflict")
            if prior is None and len(group_events) >= MAX_PENDING_HISTORY_EVENTS_PER_GROUP:
                pending_errors[group_id] = sorted(
                    set(pending_errors.get(group_id, [])) | {"history_event_queue_capacity_exceeded"}
                )
            else:
                group_events[event_hash] = event
            self._save_history_event_sections_unlocked(pending_events, pending_errors)
            generation += 1
            pending[group_id] = generation
            self._save_unlocked(generation, pending)
            return generation

    def notify_history_event_error(self, group_id: str, reason: str) -> int:
        if not isinstance(group_id, str) or not group_id:
            raise ValueError("group_id_required")
        if reason not in HISTORY_EVENT_ERROR_CODES:
            raise ValueError("history_event_error_invalid")
        with _file_lock(self.lock_path):
            generation, pending = self._load_unlocked()
            pending_events, pending_errors = self._history_event_sections_unlocked()
            pending_errors[group_id] = sorted(set(pending_errors.get(group_id, [])) | {reason})
            self._save_history_event_sections_unlocked(pending_events, pending_errors)
            generation += 1
            pending[group_id] = generation
            self._save_unlocked(generation, pending)
            return generation

    def history_event_snapshot(self) -> dict[str, dict[str, dict[str, str]]]:
        with _file_lock(self.lock_path):
            pending, _errors = self._history_event_sections_unlocked()
            return {group: {key: dict(value) for key, value in events.items()} for group, events in pending.items()}

    def history_event_errors_snapshot(self) -> dict[str, list[str]]:
        with _file_lock(self.lock_path):
            _pending, errors = self._history_event_sections_unlocked()
            return {group: list(reasons) for group, reasons in errors.items()}

    def acknowledge_history_events(
        self,
        snapshot: dict[str, dict[str, dict[str, str]]],
        error_snapshot: dict[str, list[str]],
    ) -> None:
        with _file_lock(self.lock_path):
            self._load_unlocked()
            pending, errors = self._history_event_sections_unlocked()
            changed = False
            for group_id, events in snapshot.items():
                current = pending.get(group_id, {})
                for event_hash, event in events.items():
                    if current.get(event_hash) == event:
                        del current[event_hash]
                        changed = True
                if not current and group_id in pending:
                    del pending[group_id]
            for group_id, reasons in error_snapshot.items():
                current_reasons = errors.get(group_id, [])
                retained = [reason for reason in current_reasons if reason not in reasons]
                if retained != current_reasons:
                    changed = True
                    if retained:
                        errors[group_id] = retained
                    else:
                        errors.pop(group_id, None)
            if changed:
                self._save_history_event_sections_unlocked(pending, errors)

    def ensure_pending(self, group_ids: Iterable[str]) -> None:
        with _file_lock(self.lock_path):
            generation, pending = self._load_unlocked()
            changed = False
            for group_id in group_ids:
                if group_id not in pending:
                    generation += 1
                    pending[group_id] = generation
                    changed = True
            if changed:
                self._save_unlocked(generation, pending)

    def force_pending(self, group_ids: Iterable[str]) -> None:
        """Create a fresh durable generation for each group at recovery boundaries."""
        with _file_lock(self.lock_path):
            generation, pending = self._load_unlocked()
            for group_id in group_ids:
                generation += 1
                pending[group_id] = generation
            self._save_unlocked(generation, pending)

    def retain_scope(self, group_ids: Iterable[str]) -> None:
        allowed = set(group_ids)
        with _file_lock(self.lock_path):
            generation, pending = self._load_unlocked()
            pending_history, history_errors = self._history_event_sections_unlocked()
            scoped = {key: value for key, value in pending.items() if key in allowed}
            scoped_history = {key: value for key, value in pending_history.items() if key in allowed}
            scoped_errors = {key: value for key, value in history_errors.items() if key in allowed}
            if scoped_history != pending_history or scoped_errors != history_errors:
                self._save_history_event_sections_unlocked(scoped_history, scoped_errors)
            if scoped != pending:
                self._save_unlocked(generation, scoped)

    def acknowledge(self, snapshot: dict[str, int]) -> None:
        with _file_lock(self.lock_path):
            generation, pending = self._load_unlocked()
            for group_id, seen_generation in snapshot.items():
                if pending.get(group_id) == seen_generation:
                    del pending[group_id]
            self._save_unlocked(generation, pending)

    def enqueue_todo_event(self, event: dict[str, Any]) -> bool:
        normalized = validate_queued_todo_event(event)
        with _file_lock(self.lock_path):
            generation, pending_groups = self._load_unlocked()
            pending_events, completed_events = self._todo_event_sections_unlocked()
            event_id = normalized["event_id"]
            if event_id in completed_events:
                if completed_events[event_id] != normalized:
                    raise StateError("todo_event_identity_conflict")
                return False
            prior = pending_events.get(event_id)
            if prior is not None:
                if prior != normalized:
                    raise StateError("todo_event_identity_conflict")
                return False
            pending_events[event_id] = normalized
            self._save_unlocked(generation + 1, pending_groups, pending_events, completed_events)
            return True

    def task_event_snapshot(self) -> dict[str, dict[str, str]]:
        with _file_lock(self.lock_path):
            pending, _completed = self._todo_event_sections_unlocked()
            return {key: dict(value) for key, value in pending.items()}

    def acknowledge_task_events(self, snapshot: dict[str, dict[str, str]]) -> None:
        with _file_lock(self.lock_path):
            generation, pending_groups = self._load_unlocked()
            pending, completed = self._todo_event_sections_unlocked()
            changed = False
            for event_id, event in snapshot.items():
                if pending.get(event_id) == event:
                    del pending[event_id]
                    completed[event_id] = dict(event)
                    changed = True
            if changed:
                while len(completed) > self.MAX_COMPLETED_TODO_EVENTS:
                    completed.pop(next(iter(completed)))
                self._save_unlocked(generation, pending_groups, pending, completed)

    def completed_task_event(self, event_id: str) -> bool:
        with _file_lock(self.lock_path):
            _pending, completed = self._todo_event_sections_unlocked()
            return event_id in completed


class WorkLock:
    """Exclusive advisory lock shared by listener, opening wake and recovery."""

    def __init__(self, path: str | Path, *, cancel_event: threading.Event | None = None,
                 retry_interval: float = 0.1):
        self.path = Path(path)
        self.cancel_event = cancel_event
        self.retry_interval = max(0.01, float(retry_interval))
        self._context: Any = None
        self._fd: int | None = None

    def __enter__(self) -> "WorkLock":
        if self.cancel_event is None:
            self._context = _file_lock(self.path)
            self._context.__enter__()
            return self

        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            fd = os.open(
                self.path,
                os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            os.fchmod(fd, 0o600)
        except OSError:
            raise StateError("lock_file_unavailable") from None
        try:
            while True:
                if self.cancel_event.is_set():
                    raise WorkLockCancelled("work_lock_canceled")
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self._fd = fd
                    break
                except BlockingIOError:
                    self.cancel_event.wait(self.retry_interval)
        except BaseException:
            os.close(fd)
            raise
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if self._context is not None:
            self._context.__exit__(exc_type, exc, traceback)
            self._context = None
        elif self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            finally:
                os.close(self._fd)
                self._fd = None


class WorkLockCancelled(RuntimeError):
    """A stop request arrived while waiting for the shared work lock."""


class CatchupCoordinator:
    """Serialize history work and acknowledge replay hints only for ledger-backed production."""

    def __init__(self, state: AtomicPendingState, runner: Callable[[], Any], lock_path: str | Path,
                 now: Callable[[], float] | None = None, *, mode: str = "shadow",
                 write_ledger: bool = False):
        self.state = state
        self.runner = runner
        self.lock_path = Path(lock_path)
        self.now = now or time.time
        self.ack_replay_hints = mode == "production" and bool(write_ledger)
        self.failures = 0
        self.retry_not_before = 0.0

    def run_once(self, *, force: bool = False) -> dict[str, Any]:
        with WorkLock(self.lock_path):
            snapshot = self.state.snapshot()
            todo_snapshot = self.state.task_event_snapshot()
            history_snapshot = self.state.history_event_snapshot()
            history_error_snapshot = self.state.history_event_errors_snapshot()
            history_pending = any(history_snapshot.values()) or any(history_error_snapshot.values())
            if not snapshot and not todo_snapshot and not history_pending:
                return {"status": "IDLE", "superseded": False}
            current_time = self.now()
            if not force and current_time < self.retry_not_before:
                return {"status": "DEFERRED", "reason": "worker_backoff", "superseded": False,
                        "pending_groups": len(snapshot)}
            try:
                raw_result = self.runner()
            except Exception as exc:
                result_status = "REVIEW"
                result_code = "worker_error_" + type(exc).__name__
            else:
                result_status = (
                    str(raw_result.get("status") or "UNKNOWN")
                    if isinstance(raw_result, dict) else "UNKNOWN"
                )
                result_code = str(raw_result.get("reason") or "") if isinstance(raw_result, dict) else ""
            history_replay_complete = (
                isinstance(raw_result, dict)
                and raw_result.get("history_replay_complete") is True
            )
            if result_status == "COMPLETED" and history_pending and not history_replay_complete:
                result_status = "REVIEW"
                result_code = "history_replay_unverified"
            if result_status == "COMPLETED":
                self.state.acknowledge(snapshot)
                self.state.acknowledge_task_events(todo_snapshot)
                if history_pending and self.ack_replay_hints:
                    self.state.acknowledge_history_events(history_snapshot, history_error_snapshot)
                self.failures = 0
                self.retry_not_before = 0.0
            elif result_status not in {"SKIPPED", "IDLE", "DEFERRED"}:
                self.failures += 1
                self.retry_not_before = self.now() + min(60 * (2 ** min(self.failures - 1, 4)), 900)
            latest = self.state.snapshot()
            latest_todo = self.state.task_event_snapshot()
            latest_history = self.state.history_event_snapshot()
            latest_history_errors = self.state.history_event_errors_snapshot()
            superseded = (
                any(latest.get(key, 0) > value for key, value in snapshot.items())
                or any(latest_todo.get(key) != value for key, value in todo_snapshot.items())
                or any(key not in todo_snapshot for key in latest_todo)
                or any(
                    key in latest_history and latest_history[key] != value
                    for key, value in history_snapshot.items()
                )
                or any(key not in history_snapshot for key in latest_history)
                or any(
                    key in latest_history_errors and latest_history_errors[key] != value
                    for key, value in history_error_snapshot.items()
                )
                or any(key not in history_error_snapshot for key in latest_history_errors)
            )
            safe_reason = ""
            if re.fullmatch(r"[A-Za-z0-9_]{1,80}", result_code):
                safe_reason = result_code
            return {
                "status": result_status,
                "reason": safe_reason,
                "superseded": superseded,
                "pending_groups": len(latest),
            }

    def run_burst(self, max_passes: int = 2) -> list[dict[str, Any]]:
        """Run immediate follow-up passes only when a newer durable trigger arrived."""
        results = []
        force = False
        for _ in range(max(1, int(max_passes))):
            result = self.run_once(force=force)
            results.append(result)
            if not result["superseded"]:
                break
            force = True
        return results


class WakeupRouter:
    """Validate transport event IDs against current configured scope."""

    def __init__(self, group_id: str, allowed_group_ids: Iterable[str], state: AtomicPendingState,
                 notify: Callable[[], None] | None = None,
                 parity_ledger: DwsParityLedger | None = None):
        self.group_id = group_id
        self.allowed = set(allowed_group_ids)
        self.state = state
        self.notify_callback = notify
        self.parity_ledger = parity_ledger
        self.invalid_count = 0
        self.unconfigured_count = 0
        self.parity_errors = 0

    def handle(self, payload: Any) -> bool:
        conversation_id = normalize_wakeup(payload)
        if conversation_id is None:
            self.invalid_count += 1
            return False
        if conversation_id != self.group_id or conversation_id not in self.allowed:
            self.unconfigured_count += 1
            return False
        message = _find_history_event_message(payload)
        if message is None:
            self.state.notify_history_event_error(conversation_id, "history_event_message_id_missing")
        else:
            message_conversation_id = next((
                message.get(key) for key in (
                    "conversation_id", "conversationId", "openConversationId", "open_conversation_id",
                ) if isinstance(message.get(key), str) and message.get(key).strip()
            ), None)
            message_id = next((
                message.get(key) for key in (
                    "message_id", "messageId", "openMessageId", "open_message_id",
                ) if isinstance(message.get(key), str) and message.get(key).strip()
            ), None)
            event_time = next((
                message.get(key) for key in ("createTime", "create_time") if key in message
            ), None)
            if message_conversation_id != conversation_id:
                self.state.notify_history_event_error(conversation_id, "history_event_identity_mismatch")
            elif not message_id:
                self.state.notify_history_event_error(conversation_id, "history_event_message_id_missing")
            elif event_time in (None, ""):
                self.state.notify_history_event_error(conversation_id, "history_event_time_missing")
            elif _normalize_history_event_time(event_time) is None:
                self.state.notify_history_event_error(conversation_id, "history_event_time_invalid")
            else:
                try:
                    self.state.notify_history_event(conversation_id, str(message_id), event_time)
                except StateError:
                    self.state.notify_history_event_error(conversation_id, "history_event_identity_mismatch")
        if self.parity_ledger is not None:
            try:
                self.parity_ledger.record_event(payload, expected_group_id=conversation_id)
            except DwsParityError:
                self.parity_errors += 1
        if self.notify_callback:
            self.notify_callback()
        return True


class TodoEventRouter:
    """Persist only stable TODO event IDs, then wake the shared worker queue."""

    def __init__(self, state: AtomicPendingState, notify: Callable[[], None] | None = None):
        self.state = state
        self.notify_callback = notify
        self.invalid_count = 0

    def handle(self, payload: Any) -> bool:
        try:
            event = normalize_todo_lifecycle_event(payload)
            enqueued = self.state.enqueue_todo_event(event)
        except (StateError, TypeError, ValueError):
            self.invalid_count += 1
            return False
        if enqueued and self.notify_callback:
            self.notify_callback()
        return enqueued


def subscription_retry_limit(retryable: Any) -> int:
    """Official 0/2/1 additional tries for false/true/unknown."""
    if retryable is False:
        return 0
    if retryable is True:
        return 2
    return 1


def _retry_timestamp(value: Any) -> float | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=CHINA_TZ)
        return parsed.timestamp()
    except (TypeError, ValueError, OverflowError):
        return None


class SubscriptionRetryStore:
    """Persist host retry budgets so respawning cannot reset creation attempts."""

    def __init__(self, state_dir: str | Path):
        self.state_dir = _secure_state_dir(Path(state_dir))
        self.path = self.state_dir / "subscription-attempts.json"
        self.lock_path = self.state_dir / "subscription-attempts.lock"
        with _file_lock(self.lock_path):
            self._load_unlocked()

    def _load_unlocked(self) -> dict[str, dict[str, Any]]:
        payload = _read_json_file(self.path, {"version": 1, "subscriptions": {}})
        rows = payload.get("subscriptions")
        if payload.get("version") != 1 or not isinstance(rows, dict):
            raise StateError("subscription_state_invalid")
        safe: dict[str, dict[str, Any]] = {}
        for group, item in rows.items():
            if not isinstance(group, str) or not group or not isinstance(item, dict):
                raise StateError("subscription_state_invalid")
            attempts, limit = item.get("attempts"), item.get("max_total")
            blocked, retry_at = item.get("blocked"), item.get("retry_at", 0)
            if (
                isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 0
                or isinstance(limit, bool) or not isinstance(limit, int) or limit < 1
                or not isinstance(blocked, bool)
                or isinstance(retry_at, bool) or not isinstance(retry_at, (int, float))
            ):
                raise StateError("subscription_state_invalid")
            safe[group] = {"attempts": attempts, "max_total": limit, "blocked": blocked,
                           "retry_at": float(retry_at)}
        return safe

    def _save_unlocked(self, rows: dict[str, dict[str, Any]]) -> None:
        _atomic_write_json(self.path, {"version": 1, "subscriptions": rows})

    def begin_attempt(self, group_id: str, now: float | None = None) -> bool:
        now = time.time() if now is None else now
        with _file_lock(self.lock_path):
            rows = self._load_unlocked()
            item = rows.get(group_id, {"attempts": 0, "max_total": 2, "blocked": False, "retry_at": 0.0})
            if item["blocked"] or now < item["retry_at"] or item["attempts"] >= item["max_total"]:
                return False
            item["attempts"] += 1
            rows[group_id] = item
            self._save_unlocked(rows)
            return True

    def failed(self, group_id: str, failure: dict[str, Any], now: float | None = None) -> dict[str, Any]:
        now = time.time() if now is None else now
        safe_failure = failure if isinstance(failure, dict) else {}
        retryable = safe_failure.get("retryable") if isinstance(safe_failure.get("retryable"), bool) else None
        state = str(safe_failure.get("state") or safe_failure.get("status") or "").casefold()
        protected_state = state in {"in_flight", "cooldown", "terminal_hold"}
        retry_at = _retry_timestamp(safe_failure.get("next_retry_at")) or 0.0
        retry_after = safe_failure.get("retry_after_seconds")
        try:
            delay = max(0.0, float(retry_after)) if retry_after is not None else 0.0
            if not math.isfinite(delay):
                delay = 0.0
        except (TypeError, ValueError, OverflowError):
            delay = 0.0
        if retry_at:
            delay = max(delay, retry_at - now)
        with _file_lock(self.lock_path):
            rows = self._load_unlocked()
            item = rows.get(group_id, {"attempts": 1, "max_total": 2, "blocked": False, "retry_at": 0.0})
            if retryable is False:
                item["max_total"] = item["attempts"]
            elif retryable is True:
                item["max_total"] = max(item["max_total"], 3)
            else:
                item["max_total"] = min(item["max_total"], max(item["attempts"], 2))
            if protected_state and delay <= 0:
                item["blocked"] = True
            item["retry_at"] = max(float(item.get("retry_at", 0.0)), now + delay)
            rows[group_id] = item
            self._save_unlocked(rows)
            retry = not item["blocked"] and item["attempts"] < item["max_total"]
            # Unknown timing gets a short bounded pause; no tight create loop.
            if retry and delay <= 0:
                delay = min(2 ** max(0, item["attempts"] - 1), 30)
                item["retry_at"] = now + delay
                rows[group_id] = item
                self._save_unlocked(rows)
            return {"retry": retry, "delay_seconds": delay, "attempts": item["attempts"],
                    "max_total": item["max_total"]}

    def ready(self, group_id: str) -> None:
        with _file_lock(self.lock_path):
            rows = self._load_unlocked()
            if group_id in rows:
                del rows[group_id]
                self._save_unlocked(rows)

    def retain_scope(self, group_ids: Iterable[str]) -> None:
        allowed = set(group_ids)
        with _file_lock(self.lock_path):
            rows = self._load_unlocked()
            scoped = {key: value for key, value in rows.items() if key in allowed}
            if scoped != rows:
                self._save_unlocked(scoped)


class _CombinedCancel:
    def __init__(self, events: Iterable[threading.Event]):
        self.events = tuple(events)

    def is_set(self) -> bool:
        return any(event.is_set() for event in self.events)

    def wait(self, timeout: float | None = None) -> bool:
        deadline = None if timeout is None else time.monotonic() + max(0.0, timeout)
        while not self.is_set():
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return self.is_set()
                wait_for = min(0.1, remaining)
            else:
                wait_for = 0.1
            self.events[0].wait(wait_for)
        return True


class _StartupJsonBuffer:
    """Collect only bounded, pretty-printed JSON objects from startup output."""

    def __init__(self):
        self.value = ""
        self.lines = 0

    def feed(self, line: str) -> dict[str, Any] | None:
        if not self.value:
            if not line.lstrip().startswith("{"):
                return None
            self.value = line
            self.lines = 1
        else:
            self.value += line
            self.lines += 1
        if len(self.value) > MAX_STARTUP_JSON_CHARS or self.lines > MAX_STARTUP_JSON_LINES:
            self.value = ""
            self.lines = 0
            return None
        try:
            parsed = json.loads(self.value)
        except (json.JSONDecodeError, TypeError):
            return None
        self.value = ""
        self.lines = 0
        return parsed if isinstance(parsed, dict) else None


def _valid_subscribe_id(value: Any) -> bool:
    return (
        isinstance(value, str)
        and not value.startswith("-")
        and SUBSCRIBE_ID_RE.fullmatch(value) is not None
    )


class SubscriptionCleanupLedger:
    """Private exact-receipt record for cleanup that could not be confirmed."""

    def __init__(self, state_dir: str | Path):
        self.state_dir = _secure_state_dir(Path(state_dir))
        self.path = self.state_dir / "subscription-cleanup-pending.json"
        self.lock_path = self.state_dir / "subscription-cleanup-pending.lock"
        with _file_lock(self.lock_path):
            self._load_unlocked()

    def _load_unlocked(self) -> list[dict[str, Any]]:
        payload = _read_json_file(self.path, {"version": 1, "pending": []})
        pending = payload.get("pending")
        if payload.get("version") != 1 or not isinstance(pending, list):
            raise StateError("cleanup_state_invalid")
        rows: list[dict[str, Any]] = []
        for item in pending:
            if (
                not isinstance(item, dict)
                or not isinstance(item.get("group_id"), str)
                or not item.get("group_id")
                or item.get("ownership") not in {"new", "reused", "unknown"}
                or item.get("status") != "cleanup_pending"
                or (item.get("subscribe_id") is not None and not _valid_subscribe_id(item.get("subscribe_id")))
            ):
                raise StateError("cleanup_state_invalid")
            rows.append({
                "group_id": item["group_id"],
                "subscribe_id": item.get("subscribe_id"),
                "ownership": item["ownership"],
                "status": "cleanup_pending",
            })
        return rows

    def snapshot(self) -> list[dict[str, Any]]:
        with _file_lock(self.lock_path):
            return self._load_unlocked()

    def record_pending(self, group_id: str, subscribe_id: str | None, ownership: str) -> None:
        if ownership not in {"new", "reused", "unknown"}:
            ownership = "unknown"
        if not _valid_subscribe_id(subscribe_id):
            subscribe_id = None
        with _file_lock(self.lock_path):
            rows = self._load_unlocked()
            row = {
                "group_id": group_id,
                "subscribe_id": subscribe_id,
                "ownership": ownership,
                "status": "cleanup_pending",
            }
            if row not in rows:
                rows.append(row)
                _atomic_write_json(self.path, {"version": 1, "pending": rows})

    def resolve(self, group_id: str, subscribe_id: str | None) -> None:
        with _file_lock(self.lock_path):
            rows = self._load_unlocked()
            retained = [
                row for row in rows
                if not (row["group_id"] == group_id and row.get("subscribe_id") == subscribe_id)
            ]
            if retained != rows:
                _atomic_write_json(self.path, {"version": 1, "pending": retained})


class DwsConsumer:
    """One official consume subprocess with concurrent, privacy-safe pipe readers."""

    def __init__(self, argv: list[str], group_id: str, on_event: Callable[[Any], None],
                 on_ready: Callable[[str, bool], None], *, popen_factory: Callable[..., Any] = subprocess.Popen,
                 cleanup_state_dir: str | Path | None = None,
                 stop_runner: Callable[..., Any] | None = None,
                 event_keys: Iterable[str] | None = None):
        self.argv = list(argv)
        self.group_id = group_id
        self.on_event = on_event
        self.on_ready = on_ready
        self.popen_factory = popen_factory
        self.stop_runner = stop_runner
        self.cleanup_ledger = (
            SubscriptionCleanupLedger(cleanup_state_dir) if cleanup_state_dir is not None else None
        )
        self.process: Any = None
        self.ready = threading.Event()
        self._threads: list[threading.Thread] = []
        self._failure: dict[str, Any] = {}
        self._failure_lock = threading.Lock()
        self._receipt_lock = threading.Lock()
        self._receipts: dict[str, dict[str, str]] = {}
        self.event_keys = tuple(event_keys or ("user_im_message_receive_group",))
        if not self.event_keys or len(self.event_keys) != len(set(self.event_keys)):
            raise ValueError("event_keys_invalid")
        self._received_event_keys: set[str] = set()
        self._unknown_receipt = False
        self._subscribe_id_supplied = "--subscribe-id" in self.argv
        self._explicit_subscribe_id = self._requested_subscribe_id()
        self.invalid_lines = 0
        self.closed = False
        self.cleanup_result: dict[str, Any] = {"status": "not_started"}
        self._close_lock = threading.Lock()

    def _requested_subscribe_id(self) -> str | None:
        try:
            index = self.argv.index("--subscribe-id")
            value = self.argv[index + 1]
        except (ValueError, IndexError):
            return None
        return value if _valid_subscribe_id(value) else None

    def start(self) -> None:
        self.process = start_dws_stream(
            self.argv[1:], executable=self.argv[0], popen_factory=self.popen_factory,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1, close_fds=True,
        )
        stdout_thread = threading.Thread(target=self._read_stdout, name="dws-event-stdout", daemon=True)
        stderr_thread = threading.Thread(target=self._read_stderr, name="dws-event-stderr", daemon=True)
        self._threads = [stdout_thread, stderr_thread]
        stdout_thread.start()
        stderr_thread.start()

    def _store_failure(self, value: Any) -> None:
        if not isinstance(value, dict):
            return
        sources: list[dict[str, Any]] = []
        pending = [(value, 0)]
        visited: set[int] = set()
        while pending:
            current, depth = pending.pop()
            if not isinstance(current, dict) or id(current) in visited or depth > 4:
                continue
            visited.add(id(current))
            sources.append(current)
            for key in ("error", "details"):
                nested = current.get(key)
                if isinstance(nested, dict):
                    pending.append((nested, depth + 1))
        safe: dict[str, Any] = {}
        for source in sources:
            retryable = source.get("retryable")
            if isinstance(retryable, bool):
                safe["retryable"] = retryable
            reason = source.get("reason")
            if isinstance(reason, str) and SAFE_REASON_RE.fullmatch(reason):
                safe["reason"] = reason
            delay = source.get("retry_after_seconds")
            if isinstance(delay, (int, float)) and not isinstance(delay, bool):
                try:
                    finite_delay = float(delay)
                except (TypeError, ValueError, OverflowError):
                    finite_delay = -1
                if math.isfinite(finite_delay) and finite_delay >= 0:
                    safe["retry_after_seconds"] = delay
            retry_at = source.get("next_retry_at")
            if isinstance(retry_at, str) and len(retry_at) <= 64 and _retry_timestamp(retry_at) is not None:
                safe["next_retry_at"] = retry_at
            for key in ("state", "status"):
                state = source.get(key)
                if isinstance(state, str) and state.casefold() in {"in_flight", "cooldown", "terminal_hold"}:
                    safe["state"] = state.casefold()
            for key in ("trace_id", "request_id", "attempt_id", "subscribe_id", "subscription_id", "error_id"):
                identifier = source.get(key)
                if _valid_subscribe_id(identifier):
                    safe[key] = identifier
        if safe:
            with self._failure_lock:
                self._failure.update(safe)

    def _record_receipt(self, line: str) -> None:
        stripped = line.strip()
        match = SUBSCRIPTION_RECEIPT_RE.fullmatch(stripped) or READY_RECEIPT_RE.fullmatch(stripped)
        if not match:
            return
        event_key, subscribe_id = match.groups()
        if event_key not in self.event_keys or not _valid_subscribe_id(subscribe_id):
            with self._receipt_lock:
                self._unknown_receipt = True
            return
        if not self._subscribe_id_supplied:
            ownership = "new"
        elif self._explicit_subscribe_id is not None and subscribe_id == self._explicit_subscribe_id:
            ownership = "reused"
        else:
            ownership = "unknown"
        with self._receipt_lock:
            old = self._receipts.get(subscribe_id)
            if old and old.get("ownership") == "new":
                ownership = "new"
            self._receipts[subscribe_id] = {
                "subscribe_id": subscribe_id,
                "ownership": ownership,
            }
            self._received_event_keys.add(event_key)

    def subscription_receipts(self) -> list[dict[str, str]]:
        with self._receipt_lock:
            return [dict(item) for item in self._receipts.values()]

    def receipts_complete(self) -> bool:
        with self._receipt_lock:
            return set(self.event_keys).issubset(self._received_event_keys)

    @staticmethod
    def _read_limited(stream: Any) -> tuple[str, bool]:
        line = stream.readline(MAX_LINE_CHARS + 1)
        if not line:
            return "", False
        overflow = len(line) > MAX_LINE_CHARS
        if overflow and not line.endswith("\n"):
            while line and not line.endswith("\n"):
                line = stream.readline(MAX_LINE_CHARS + 1)
        return ("", True) if overflow else (line, False)

    def _read_stdout(self) -> None:
        startup_json = _StartupJsonBuffer()
        while True:
            try:
                line, overflow = self._read_limited(self.process.stdout)
            except (OSError, ValueError):
                return
            if not line and not overflow:
                return
            if overflow:
                self.invalid_lines += 1
                continue
            if not self.ready.is_set():
                # Startup output is diagnostic only; event payloads wait for ready.
                self._store_failure(startup_json.feed(line))
                continue
            try:
                payload = json.loads(line)
            except (json.JSONDecodeError, TypeError):
                self._store_failure(startup_json.feed(line))
                self.invalid_lines += 1
                continue
            if isinstance(payload, dict):
                self._store_failure(payload)
                try:
                    self.on_event(payload)
                except Exception:
                    # Provider text and callback exceptions never enter logs.
                    self.invalid_lines += 1
            else:
                self.invalid_lines += 1

    def _read_stderr(self) -> None:
        startup_json = _StartupJsonBuffer()
        while True:
            try:
                line, overflow = self._read_limited(self.process.stderr)
            except (OSError, ValueError):
                return
            if not line and not overflow:
                return
            if overflow:
                continue
            self._record_receipt(line)
            if is_ready_marker(line):
                # A receipt is useful for exact lifecycle cleanup; the ready marker
                # itself says only that the IPC consumer reached its ready boundary.
                was_ready = self.ready.is_set()
                self.ready.set()
                try:
                    self.on_ready(self.group_id, was_ready)
                except Exception:
                    pass
                continue
            self._store_failure(startup_json.feed(line))

    @staticmethod
    def _safe_failure(line: str) -> dict[str, Any]:
        try:
            raw = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            return {}
        if not isinstance(raw, dict):
            return {}
        probe = object.__new__(DwsConsumer)
        probe._failure = {}
        probe._failure_lock = threading.Lock()
        probe._store_failure(raw)
        return probe.failure_metadata()

    def failure_metadata(self) -> dict[str, Any]:
        with self._failure_lock:
            return dict(self._failure)

    def drain_exit_metadata(self, timeout: float = 2.0) -> None:
        if self.process is not None and self.process.poll() is not None:
            for thread in self._threads:
                thread.join(timeout=max(0.0, timeout))

    def wait_ready(self, timeout: float, cancel_event: threading.Event | None = None) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if cancel_event is not None and cancel_event.is_set():
                return False
            if self.ready.is_set():
                return self.process.poll() is None
            if self.process.poll() is not None:
                return False
            self.ready.wait(min(0.1, max(0.0, deadline - time.monotonic())))
        if self.ready.is_set():
            return (cancel_event is None or not cancel_event.is_set()) and self.process.poll() is None
        return False

    def wait(self, timeout: float = 0) -> int | None:
        return self.process.poll() if self.process is not None else None

    def _record_cleanup_pending(self, subscribe_id: str | None, ownership: str) -> None:
        if self.cleanup_ledger is not None:
            try:
                self.cleanup_ledger.record_pending(self.group_id, subscribe_id, ownership)
            except Exception:
                self.cleanup_result["metadata_status"] = "write_failed"

    def close(self, grace_seconds: float = 10.0, *, graceful: bool = True) -> dict[str, Any]:
        with self._close_lock:
            if self.closed:
                return dict(self.cleanup_result)
            self.closed = True
            process = self.process
            if process is None:
                self.cleanup_result = {"status": "not_started"}
                return dict(self.cleanup_result)

            already_exited = process.poll() is not None
            requested_exit = not already_exited
            used_terminate = False
            exit_code = process.poll()
            if requested_exit:
                stdin = getattr(process, "stdin", None)
                if stdin is not None:
                    try:
                        stdin.close()  # Keep stdin open during normal consume; EOF is an explicit stop request.
                    except (OSError, ValueError):
                        pass
                try:
                    exit_code = process.wait(timeout=max(0.0, grace_seconds))
                except (subprocess.TimeoutExpired, TimeoutError):
                    used_terminate = True
                    try:
                        process.terminate()
                    except (OSError, ProcessLookupError):
                        pass
                    try:
                        exit_code = process.wait(timeout=min(2.0, max(0.0, grace_seconds)))
                    except (subprocess.TimeoutExpired, TimeoutError):
                        exit_code = None
                except (OSError, ChildProcessError):
                    exit_code = process.poll()

            drain_deadline = time.monotonic() + min(2.0, max(0.1, grace_seconds))
            for thread in self._threads:
                thread.join(timeout=max(0.0, drain_deadline - time.monotonic()))
            for stream in (getattr(process, "stdout", None), getattr(process, "stderr", None)):
                if stream is not None:
                    try:
                        stream.close()
                    except (OSError, ValueError):
                        pass

            receipts = self.subscription_receipts()
            confirmed_graceful_exit = requested_exit and not used_terminate and exit_code == 0 and graceful
            process_alive = process.poll() is None
            pending: list[str] = []
            cleanup_actions: list[str] = []
            if not receipts:
                self._record_cleanup_pending(None, "unknown")
                pending.append("receipt_invalid" if self._unknown_receipt else "receipt_missing")
            for receipt in receipts:
                subscribe_id = receipt["subscribe_id"]
                ownership = receipt["ownership"]
                if ownership == "reused":
                    cleanup_actions.append("reused_preserved")
                    continue
                if ownership != "new":
                    self._record_cleanup_pending(subscribe_id, "unknown")
                    pending.append("ownership_unknown")
                    continue
                if confirmed_graceful_exit:
                    if self.cleanup_ledger is not None:
                        try:
                            self.cleanup_ledger.resolve(self.group_id, subscribe_id)
                        except Exception:
                            self._record_cleanup_pending(subscribe_id, "new")
                            pending.append("metadata_unknown")
                    cleanup_actions.append("graceful")
                    continue

                stopped = False
                if self.stop_runner is not None:
                    command = [self.argv[0], "event", "stop", subscribe_id, "--yes"]
                    try:
                        result = self.stop_runner(
                            command, timeout=10.0, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL, text=True, close_fds=True,
                        )
                        stopped = getattr(result, "returncode", 1) == 0
                    except Exception:
                        stopped = False
                if stopped and process_alive:
                    try:
                        process.wait(timeout=2.0)
                    except (subprocess.TimeoutExpired, TimeoutError, OSError, ChildProcessError):
                        pass
                    process_alive = process.poll() is None
                if stopped and not process_alive:
                    if self.cleanup_ledger is not None:
                        try:
                            self.cleanup_ledger.resolve(self.group_id, subscribe_id)
                        except Exception:
                            self._record_cleanup_pending(subscribe_id, "new")
                            pending.append("metadata_unknown")
                    cleanup_actions.append("stopped_by_receipt")
                else:
                    self._record_cleanup_pending(subscribe_id, "new")
                    pending.append("stop_failed" if not stopped else "process_alive")

            if process_alive:
                pending.append("process_alive")
                if receipts:
                    for receipt in receipts:
                        if receipt["ownership"] == "reused":
                            self._record_cleanup_pending(receipt["subscribe_id"], "reused")
            if pending:
                status = "cleanup_pending"
            elif cleanup_actions and all(action == "reused_preserved" for action in cleanup_actions):
                status = "reused_preserved"
            elif "stopped_by_receipt" in cleanup_actions:
                status = "stopped_by_receipt"
            elif confirmed_graceful_exit:
                status = "graceful"
            else:
                status = "cleanup_pending"
            self.cleanup_result = {
                "status": status,
                "owned_subscriptions": [
                    item["subscribe_id"] for item in receipts if item["ownership"] == "new"
                ],
                "pending_reasons": sorted(set(pending)),
            }
            return dict(self.cleanup_result)


class DwsEventTransport:
    """Injectable subscriber creator with persistent official retry budgeting."""

    def __init__(self, state_dir: str | Path, *, executable: str = "dws",
                 consumer_factory: Callable[..., DwsConsumer] = DwsConsumer,
                 ready_timeout: float = DEFAULT_READY_TIMEOUT,
                 cancel_event: threading.Event | None = None,
                 wait: Callable[[float], bool] | None = None,
                 stop_runner: Callable[..., Any] | None = None,
                 now: Callable[[], float] | None = None):
        self.executable = executable
        self.consumer_factory = consumer_factory
        self.ready_timeout = ready_timeout
        self.cancel_event = cancel_event or threading.Event()
        self.wait_override = wait
        self.now = now or time.time
        self.state_dir = _secure_state_dir(Path(state_dir))
        self.retry_store = SubscriptionRetryStore(self.state_dir)
        self.cleanup_ledger = SubscriptionCleanupLedger(self.state_dir)
        self._cleanup_blocked = bool(self.cleanup_ledger.snapshot())
        self.stop_runner = stop_runner or self._official_stop
        self._launch_lock = threading.Lock()
        self._group_cancel: dict[str, threading.Event] = {}
        self._group_cancel_lock = threading.Lock()

    def _official_stop(self, command: list[str], **kwargs: Any) -> Any:
        if not command:
            raise ValueError("dws_command_required")
        return run_dws_command(command[1:], executable=command[0], **kwargs)

    def cancel(self) -> None:
        # Serializes the last cancellation check with subprocess creation.
        with self._launch_lock:
            self.cancel_event.set()

    def cleanup_pending(self) -> bool:
        if self._cleanup_blocked:
            return True
        try:
            pending = bool(self.cleanup_ledger.snapshot())
        except Exception:
            pending = True
        if pending:
            self._cleanup_blocked = True
        return pending

    def reset_group(self, group_id: str) -> None:
        with self._group_cancel_lock:
            self._group_cancel[group_id] = threading.Event()

    def cancel_group(self, group_id: str) -> None:
        with self._launch_lock:
            with self._group_cancel_lock:
                event = self._group_cancel.get(group_id)
            if event is not None:
                event.set()

    def _cancel_for_group(self, group_id: str) -> _CombinedCancel:
        with self._group_cancel_lock:
            group_cancel = self._group_cancel.setdefault(group_id, threading.Event())
        return _CombinedCancel((self.cancel_event, group_cancel))

    def _wait_cancelable(self, seconds: float, cancel: _CombinedCancel) -> bool:
        if self.wait_override is not None:
            if cancel.is_set():
                return True
            interrupted = self.wait_override(max(0.0, seconds))
            return bool(interrupted) or cancel.is_set()
        deadline = time.monotonic() + max(0.0, seconds)
        while not cancel.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            if cancel.wait(min(remaining, 3600.0)):
                return True
        return True

    def start_group(self, group_id: str, on_event: Callable[[Any], None],
                    on_ready: Callable[[str, bool], None]) -> DwsConsumer | None:
        return self._start_consumer(
            group_id, consume_argv(group_id, self.executable),
            ("user_im_message_receive_group",), on_event, on_ready,
        )

    def start_todo_events(self, on_event: Callable[[Any], None],
                          on_ready: Callable[[str, bool], None]) -> DwsConsumer | None:
        return self._start_consumer(
            TODO_CONSUMER_ID, todo_consume_argv(self.executable),
            TODO_EVENT_KEYS, on_event, on_ready,
        )

    def _start_consumer(self, consumer_id: str, argv: list[str], event_keys: Iterable[str],
                        on_event: Callable[[Any], None],
                        on_ready: Callable[[str, bool], None]) -> DwsConsumer | None:
        expected_keys = tuple(event_keys)
        cancel = self._cancel_for_group(consumer_id)
        if self.cleanup_pending():
            return None
        while not cancel.is_set():
            with self._launch_lock:
                if cancel.is_set() or self.cleanup_pending():
                    return None
                if not self.retry_store.begin_attempt(consumer_id, now=self.now()):
                    return None
                consumer_kwargs = (
                    {"cleanup_state_dir": self.state_dir, "stop_runner": self.stop_runner,
                     "event_keys": expected_keys}
                    if self.consumer_factory is DwsConsumer else {}
                )
                consumer = self.consumer_factory(
                    argv, consumer_id, on_event, on_ready, **consumer_kwargs,
                )
                try:
                    consumer.start()
                except OSError:
                    start_failed = True
                else:
                    start_failed = False
            if start_failed:
                try:
                    consumer.close()
                except Exception:
                    pass
                retry = self.retry_store.failed(consumer_id, {}, now=self.now())
            else:
                ready = consumer.wait_ready(self.ready_timeout, cancel)
                if ready:
                    with self._launch_lock:
                        wait_consumer = getattr(consumer, "wait", None)
                        still_running = not callable(wait_consumer) or wait_consumer(0) is None
                        receipt_reader = getattr(consumer, "subscription_receipts", None)
                        receipts = receipt_reader() if callable(receipt_reader) else None
                        receipts_known = (
                            receipts is None
                            or bool(receipts) and all(item.get("ownership") in {"new", "reused"} for item in receipts)
                        )
                        receipts_complete = getattr(consumer, "receipts_complete", None)
                        if callable(receipts_complete):
                            receipts_known = receipts_known and bool(receipts_complete())
                        no_cleanup_pending = not self.cleanup_pending()
                        if not cancel.is_set() and still_running and receipts_known and no_cleanup_pending:
                            self.retry_store.ready(consumer_id)
                            return consumer
                drain_exit = getattr(consumer, "drain_exit_metadata", None)
                if callable(drain_exit):
                    drain_exit()
                failure = consumer.failure_metadata()
                try:
                    close_method = consumer.close
                    try:
                        close_result = close_method(
                            graceful=not (callable(getattr(consumer, "wait", None)) and consumer.wait(0) is not None)
                        )
                    except TypeError:
                        close_result = close_method()
                except Exception:
                    close_result = {"status": "cleanup_pending"}
                retry = self.retry_store.failed(consumer_id, failure, now=self.now())
                if (
                    (isinstance(close_result, dict) and close_result.get("status") == "cleanup_pending")
                    or self.cleanup_pending()
                ):
                    self._cleanup_blocked = True
                    return None
                if cancel.is_set():
                    return None
            if not retry.get("retry") or cancel.is_set():
                return None
            if self._wait_cancelable(float(retry.get("delay_seconds") or 0), cancel):
                return None
        return None


def next_opening_wake(moment: datetime) -> datetime:
    """Return the next 09:00 local scheduling boundary, without weekday guesses."""
    local = moment.astimezone(CHINA_TZ) if moment.tzinfo else moment.replace(tzinfo=CHINA_TZ)
    boundary = local.replace(hour=9, minute=0, second=0, microsecond=0)
    if local >= boundary:
        boundary += timedelta(days=1)
    return boundary


class ListenerSupervisor:
    """Manage scoped consumers and serialize history catchups through one worker."""

    def __init__(self, *, scope_loader: Callable[[], Iterable[str]], state: AtomicPendingState,
                 coordinator: CatchupCoordinator, transport: Any,
                 enable_todo_events: bool = False,
                 parity_ledger: DwsParityLedger | None = None,
                 coalesce_seconds: float = DEFAULT_COALESCE_SECONDS,
                 now: Callable[[], datetime] | None = None):
        self.scope_loader = scope_loader
        self.state = state
        self.coordinator = coordinator
        self.transport = transport
        self.enable_todo_events = bool(enable_todo_events)
        self.parity_ledger = parity_ledger
        self.coalesce_seconds = coalesce_seconds
        self.now = now or (lambda: datetime.now(CHINA_TZ))
        self.notices: queue.Queue[tuple[str, str]] = queue.Queue()
        self.stop_event = threading.Event()
        self.group_ids: tuple[str, ...] = ()
        self.consumers: dict[str, Any] = {}
        self.start_threads: dict[str, threading.Thread] = {}
        self.todo_consumer: Any = None
        self.todo_start_thread: threading.Thread | None = None
        self.consumer_lock = threading.Lock()
        self.failed_scope = False

    def _load_scope(self) -> tuple[str, ...]:
        value = self.scope_loader()
        if isinstance(value, (tuple, list)) and all(isinstance(group, str) for group in value):
            return _validate_group_ids(value)
        return configured_group_ids(value)

    def _ready_notice(self, group_id: str, reconnected: bool) -> None:
        if self.parity_ledger is not None:
            try:
                self.parity_ledger.record_group_ready(group_id)
            except DwsParityError:
                safe_log({"status": "PARITY_UNAVAILABLE", "reason": "parity_state_unavailable"})
        if reconnected:
            self.notices.put(("reconnect", group_id))

    def _event_callback(self, group_id: str) -> Callable[[Any], None]:
        def route(payload: Any) -> bool:
            # Resolve current scope for every event so removed IDs stop scheduling
            # immediately after a reconnect-driven configuration refresh.
            router = WakeupRouter(
                group_id, self.group_ids, self.state,
                notify=lambda: self.notices.put(("event", group_id)),
                parity_ledger=self.parity_ledger,
            )
            return router.handle(payload)
        return route

    def _todo_event_callback(self) -> Callable[[Any], None]:
        def route(payload: Any) -> bool:
            router = TodoEventRouter(
                self.state,
                notify=lambda: self.notices.put(("todo", "")),
            )
            return router.handle(payload)
        return route

    def _todo_ready_notice(self, _consumer_id: str, reconnected: bool) -> None:
        if reconnected:
            self.notices.put(("todo-reconnect", ""))

    def _start_todo_consumer(self) -> None:
        starter = getattr(self.transport, "start_todo_events", None)
        if not self.enable_todo_events or not callable(starter):
            return
        current = self.todo_start_thread
        if self.todo_consumer is not None or (current is not None and current.is_alive()):
            return

        def start_todo() -> None:
            try:
                consumer = starter(self._todo_event_callback(), self._todo_ready_notice)
            except Exception:
                consumer = None
            cleanup_check = getattr(self.transport, "cleanup_pending", None)
            if consumer is not None and callable(cleanup_check):
                try:
                    if cleanup_check():
                        consumer.close()
                        consumer = None
                except Exception:
                    try:
                        consumer.close()
                    except Exception:
                        pass
                    consumer = None
            with self.consumer_lock:
                if consumer is not None and not self.stop_event.is_set():
                    self.todo_consumer = consumer
                    safe_log({"status": "listener_ready", "ready_consumers": len(self.consumers) + 1,
                              "configured_groups": len(self.group_ids),
                              "delivery_coverage": "unverified_by_ready_marker"})
                elif consumer is not None:
                    try:
                        consumer.close()
                    except Exception:
                        pass
                else:
                    safe_log({"status": "subscriber_unavailable", "reason": "todo_consumer_unavailable"})

        self.todo_start_thread = threading.Thread(
            target=start_todo, name="dws-todo-subscribe", daemon=True,
        )
        self.todo_start_thread.start()

    def _start_consumer(self, group_id: str) -> None:
        with self.consumer_lock:
            current = self.start_threads.get(group_id)
            if group_id in self.consumers or (current is not None and current.is_alive()):
                return

            def start_group() -> None:
                try:
                    consumer = self.transport.start_group(
                        group_id, self._event_callback(group_id), self._ready_notice
                    )
                except Exception:
                    consumer = None
                cleanup_check = getattr(self.transport, "cleanup_pending", None)
                if consumer is not None and callable(cleanup_check):
                    try:
                        if cleanup_check():
                            consumer.close()
                            consumer = None
                    except Exception:
                        try:
                            consumer.close()
                        except Exception:
                            pass
                        consumer = None
                with self.consumer_lock:
                    if consumer is not None and not self.stop_event.is_set() and group_id in self.group_ids:
                        self.consumers[group_id] = consumer
                        safe_log({
                            "status": "listener_ready",
                            "ready_consumers": len(self.consumers),
                            "configured_groups": len(self.group_ids),
                            "delivery_coverage": "unverified_by_ready_marker",
                        })
                    elif consumer is not None:
                        try:
                            consumer.close()
                        except Exception:
                            pass
                    else:
                        safe_log({
                            "status": "subscriber_unavailable",
                            "blocked_consumers": max(0, len(self.group_ids) - len(self.consumers)),
                        })

            thread = threading.Thread(target=start_group, name="dws-subscribe", daemon=True)
            self.start_threads[group_id] = thread
            thread.start()

    def _run_catchup(self) -> list[dict[str, Any]]:
        results = self.coordinator.run_burst()
        if results:
            outcome = results[-1]
            safe_log({
                "status": outcome.get("status"),
                "reason": outcome.get("reason"),
                "pending_groups": outcome.get("pending_groups"),
            })
        return results

    def start(self) -> None:
        self.group_ids = self._load_scope()
        self.state.retain_scope(self.group_ids)
        self.state.force_pending(self.group_ids)  # startup/restart catchup from durable watermarks
        self._run_catchup()
        safe_log({
            "status": "listener_starting",
            "configured_groups": len(self.group_ids),
            "delivery_coverage": "unverified_by_ready_marker",
            "parity_observer_enabled": self.parity_ledger is not None,
        })
        for group_id in self.group_ids:
            if self.stop_event.is_set():
                break
            self._start_consumer(group_id)
        self._start_todo_consumer()

    def _refresh_after_reconnect(self) -> bool:
        try:
            new_scope = self._load_scope()
        except ScopeError:
            self.failed_scope = True
            return False
        old_scope = self.group_ids
        self.group_ids = new_scope
        self.state.retain_scope(new_scope)
        self.state.force_pending(new_scope)
        removed = set(old_scope) - set(new_scope)
        for group_id in removed:
            cancel_group = getattr(self.transport, "cancel_group", None)
            if callable(cancel_group):
                cancel_group(group_id)
            with self.consumer_lock:
                consumer = self.consumers.pop(group_id, None)
                starter = self.start_threads.pop(group_id, None)
            if consumer is not None:
                consumer.close()
            if starter is not None and starter.is_alive():
                # The shared cancel event stops retry waits; a removed group is
                # ignored by its callback immediately via the refreshed scope.
                starter.join(timeout=35.0)
        for group_id in set(new_scope) - set(old_scope):
            reset_group = getattr(self.transport, "reset_group", None)
            if callable(reset_group):
                reset_group(group_id)
            self._start_consumer(group_id)
        self.notices.put(("catchup", ""))
        return True

    def wait_for_startups(self, timeout: float) -> bool:
        """Test/diagnostic boundary for injected transports; not used to gate service."""
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            with self.consumer_lock:
                threads = list(self.start_threads.values())
                if self.todo_start_thread is not None:
                    threads.append(self.todo_start_thread)
            alive = [thread for thread in threads if thread.is_alive()]
            if not alive:
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            alive[0].join(min(remaining, 0.05))

    def _process_notice_batch(self, first: tuple[str, str]) -> None:
        notices = [first]
        batch_start = time.monotonic()
        hard_deadline = batch_start + MAX_COALESCE_SECONDS
        deadline = min(batch_start + max(self.coalesce_seconds, 0.0), hard_deadline)
        while not self.stop_event.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                notice = self.notices.get(timeout=remaining)
            except queue.Empty:
                break
            notices.append(notice)
            deadline = min(time.monotonic() + max(self.coalesce_seconds, 0.0), hard_deadline)
        reconnect = any(kind == "reconnect" for kind, _ in notices)
        if reconnect and not self._refresh_after_reconnect():
            return
        if any(kind in {"event", "reconnect", "catchup", "todo", "todo-reconnect"} for kind, _ in notices):
            self._run_catchup()

    def run_forever(self) -> int:
        next_open = next_opening_wake(self.now())
        self.start()
        after_start = self.now()
        if after_start >= next_open:
            if self.state.snapshot():
                self._run_catchup()
            next_open = next_opening_wake(after_start)
        while not self.stop_event.is_set():
            now = self.now()
            timeout = max(0.0, min(60.0, (next_open - now).total_seconds()))
            try:
                notice = self.notices.get(timeout=timeout)
            except queue.Empty:
                notice = None
            with self.consumer_lock:
                consumers = list(self.consumers.items())
                todo_consumer = self.todo_consumer
            if self.stop_event.is_set():
                break
            for group_id, consumer in consumers:
                if self.stop_event.is_set():
                    break
                if consumer.wait(0) is not None:
                    # The provider normally reconnects inside its long-lived consumer.
                    # A process exit is a bounded supervisor restart boundary.
                    self.state.ensure_pending(self.group_ids)
                    self._run_catchup()
                    self.failed_scope = True
                    break
            if todo_consumer is not None and todo_consumer.wait(0) is not None:
                try:
                    todo_consumer.close()
                except Exception:
                    pass
                with self.consumer_lock:
                    if self.todo_consumer is todo_consumer:
                        self.todo_consumer = None
                # History polling and the daily recovery remain active when the
                # optional lifecycle stream exits; do not force a new subscription.
                safe_log({"status": "subscriber_unavailable", "reason": "todo_consumer_exited"})
            if notice is not None:
                self._process_notice_batch(notice)
            now = self.now()
            if now >= next_open:
                if self.state.snapshot():
                    self._run_catchup()
                next_open = next_opening_wake(now)
            if self.failed_scope:
                break
        self.close()
        return 1 if self.failed_scope else 0

    def request_stop(self) -> None:
        self.stop_event.set()
        cancel = getattr(self.transport, "cancel", None)
        if callable(cancel):
            cancel()
        else:
            cancel_event = getattr(self.transport, "cancel_event", None)
            if isinstance(cancel_event, threading.Event):
                cancel_event.set()
        self.notices.put(("stop", ""))

    def close(self) -> None:
        self.request_stop()
        with self.consumer_lock:
            starters = list(self.start_threads.values())
        for starter in starters:
            starter.join(timeout=35.0)
        with self.consumer_lock:
            consumers = list(self.consumers.values())
            self.consumers.clear()
            if self.todo_consumer is not None:
                consumers.append(self.todo_consumer)
                self.todo_consumer = None
        closers = [
            threading.Thread(target=consumer.close, name="dws-consumer-cleanup", daemon=True)
            for consumer in consumers
        ]
        for closer in closers:
            closer.start()
        close_deadline = time.monotonic() + 35.0
        for closer in closers:
            closer.join(timeout=max(0.0, close_deadline - time.monotonic()))


def run_worker_subprocess(argv: list[str], *, timeout: float = 1800.0,
                          cancel_event: threading.Event | None = None) -> dict[str, Any]:
    """Capture worker output and expose only a small result; never log its payload."""
    try:
        process = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   text=True, close_fds=True, start_new_session=True)
    except OSError as exc:
        return {"status": "REVIEW", "reason": "worker_start_failed_" + type(exc).__name__}
    deadline = time.monotonic() + timeout
    cancelled = False
    while True:
        if cancel_event is not None and cancel_event.is_set():
            cancelled = True
            break
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            stdout, _stderr = process.communicate(timeout=min(0.25, remaining))
            break
        except subprocess.TimeoutExpired:
            continue
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except OSError:
            process.terminate()
        # Drain both pipes and let the worker and its bounded children exit cleanly.
        process.communicate()
        return {"status": "REVIEW", "reason": "worker_cancelled" if cancelled else "worker_timeout"}
    try:
        payload = json.loads(stdout or "")
    except (json.JSONDecodeError, TypeError):
        return {"status": "REVIEW", "reason": "worker_result_invalid"}
    if not isinstance(payload, dict):
        return {"status": "REVIEW", "reason": "worker_result_invalid"}
    allowed = {"status", "reason", "messages", "writes", "groups_attempted", "groups_complete",
               "external_todos_created", "group_messages_sent", "history_replay_complete"}
    result = {key: payload[key] for key in allowed if key in payload}
    if "history_replay_complete" in result and not isinstance(result["history_replay_complete"], bool):
        result.pop("history_replay_complete", None)
    if process.returncode != 0 and result.get("status") == "COMPLETED":
        result["status"] = "REVIEW"
        result["reason"] = "worker_exit_status_nonzero"
    return result


def run_transport_smoke(*, groups: Iterable[str], transport: Any, duration_seconds: int) -> dict[str, Any]:
    """Run one bounded, concurrent, read-only consumer attempt per configured group."""
    scoped_groups = _validate_group_ids(groups)
    cancel_event = getattr(transport, "cancel_event", None)
    if not isinstance(cancel_event, threading.Event):
        cancel_event = threading.Event()
        transport.cancel_event = cancel_event
    cancel_transport = getattr(transport, "cancel", None)

    def request_cancel() -> None:
        if callable(cancel_transport):
            cancel_transport()
        else:
            cancel_event.set()

    guard = threading.Lock()
    attempted: set[str] = set()
    outcomes: dict[str, str] = {}
    ready_signals: set[str] = set()
    handles: list[tuple[str, Any]] = []
    event_count = 0

    def on_ready(group_id: str, _reconnected: bool) -> None:
        with guard:
            if group_id in scoped_groups:
                ready_signals.add(group_id)

    def event_callback(group_id: str) -> Callable[[Any], None]:
        def count(payload: Any) -> None:
            nonlocal event_count
            if normalize_wakeup(payload) != group_id or group_id not in scoped_groups:
                return
            with guard:
                event_count += 1
        return count

    def start_one(group_id: str) -> None:
        with guard:
            if cancel_event.is_set():
                outcomes[group_id] = "canceled"
                return
            attempted.add(group_id)
        try:
            handle = transport.start_group(group_id, event_callback(group_id), on_ready)
        except Exception:
            with guard:
                outcomes[group_id] = "canceled" if cancel_event.is_set() else "failed"
            return
        if handle is None:
            with guard:
                outcomes[group_id] = "canceled" if cancel_event.is_set() else "failed"
            return
        wait_handle = getattr(handle, "wait", None)
        if callable(wait_handle) and wait_handle(0) is not None:
            try:
                handle.close(graceful=False)
            except TypeError:
                try:
                    handle.close()
                except Exception:
                    pass
            except Exception:
                pass
            with guard:
                outcomes[group_id] = "failed"
            return
        with guard:
            outcomes[group_id] = "ready"
            handles.append((group_id, handle))

    workers = [
        threading.Thread(target=start_one, args=(group_id,), name="dws-smoke-subscribe", daemon=True)
        for group_id in scoped_groups
    ]
    timer = threading.Timer(max(0.0, float(duration_seconds)), request_cancel)
    timer.daemon = True
    timer.start()
    for worker in workers:
        worker.start()
    while not cancel_event.wait(0.1):
        cleanup_ledger = getattr(transport, "cleanup_ledger", None)
        cleanup_status = getattr(transport, "cleanup_pending", None)
        if callable(cleanup_status):
            try:
                if cleanup_status():
                    request_cancel()
                    break
            except Exception:
                request_cancel()
                break
        elif cleanup_ledger is not None:
            try:
                if cleanup_ledger.snapshot():
                    request_cancel()
                    break
            except Exception:
                request_cancel()
                break
    request_cancel()
    timer.cancel()

    # DwsConsumer close is bounded. Waiting for every startup thread also waits
    # for a process created just before cancellation to drain its receipt pipes.
    join_deadline = time.monotonic() + 35.0
    for worker in workers:
        worker.join(timeout=max(0.0, join_deadline - time.monotonic()))
    with guard:
        started_handles = list(handles)
    for group_id, handle in started_handles:
        wait_handle = getattr(handle, "wait", None)
        if callable(wait_handle) and wait_handle(0) is not None:
            with guard:
                outcomes[group_id] = "failed"
    with guard:
        unfinished = [group for group in scoped_groups if group not in outcomes]
        for group_id in unfinished:
            outcomes[group_id] = "canceled"
        ready_handles = list(handles)

    close_errors = 0
    close_results: list[dict[str, Any]] = []
    close_lock = threading.Lock()

    def close_one(handle: Any) -> None:
        nonlocal close_errors
        try:
            result = handle.close()
            if isinstance(result, dict):
                with close_lock:
                    close_results.append(result)
        except Exception:
            with close_lock:
                close_errors += 1

    closers = [threading.Thread(target=close_one, args=(handle,), name="dws-smoke-cleanup", daemon=True)
               for _group, handle in ready_handles]
    for closer in closers:
        closer.start()
    close_deadline = time.monotonic() + 35.0
    for closer in closers:
        closer.join(timeout=max(0.0, close_deadline - time.monotonic()))
        if closer.is_alive():
            close_errors += 1

    with guard:
        counts = {
            "attempted_groups": len(attempted),
            "ready_markers": len(ready_signals),
            "ready_consumers": sum(value == "ready" for value in outcomes.values()),
            "failed_groups": sum(value == "failed" for value in outcomes.values()),
            "canceled_groups": sum(value == "canceled" for value in outcomes.values()),
            "event_count": event_count,
        }
    cleanup_ledger = getattr(transport, "cleanup_ledger", None)
    try:
        pending_cleanup = len(cleanup_ledger.snapshot()) if cleanup_ledger is not None else 0
    except Exception:
        pending_cleanup = 1
    cleanup_status = getattr(transport, "cleanup_pending", None)
    try:
        if callable(cleanup_status) and cleanup_status() and pending_cleanup == 0:
            pending_cleanup = 1
    except Exception:
        pending_cleanup = max(1, pending_cleanup)
    close_pending = sum(
        result.get("status") in {"cleanup_pending", "cleanup_unknown"}
        for result in close_results
    )
    cleanup_pending = max(pending_cleanup, close_pending) + close_errors
    return {
        "status": "SMOKE_COMPLETE",
        **counts,
        "cleanup_pending": cleanup_pending,
        "delivery_coverage": "unverified_by_ready_marker",
        "success": (
            counts["ready_consumers"] == len(scoped_groups)
            and counts["failed_groups"] == 0
            and counts["canceled_groups"] == 0
            and cleanup_pending == 0
        ),
    }


def run_daily_recovery(*, state: AtomicPendingState, scope_loader: Callable[[], Iterable[str]],
                       coordinator: CatchupCoordinator, force: bool = False,
                       recovery_date: date | None = None) -> dict[str, Any]:
    """Force at most one configured catchup per Shanghai day; failed work stays pending."""
    group_ids = _validate_group_ids(scope_loader())
    run_date = recovery_date or datetime.now(CHINA_TZ).date()
    recovery_lock = state.state_dir / "daily-recovery.lock"
    recovery_record = state.state_dir / "daily-recovery.json"
    with _file_lock(recovery_lock):
        payload = _read_json_file(recovery_record, {"version": 1, "last_run_date": ""})
        if payload.get("version") != 1 or not isinstance(payload.get("last_run_date"), str):
            raise StateError("daily_recovery_state_invalid")
        if not force and payload.get("last_run_date") == run_date.isoformat():
            return {
                "status": "SKIPPED",
                "reason": "daily_recovery_already_run",
                "recovery_date": run_date.isoformat(),
                "pending_groups": len(state.snapshot()),
            }
        state.retain_scope(group_ids)
        state.force_pending(group_ids)
        _atomic_write_json(recovery_record, {
            "version": 1,
            "last_run_date": run_date.isoformat(),
        })
    results = coordinator.run_burst()
    if results:
        return {**results[-1], "recovery_date": run_date.isoformat()}
    return {"status": "IDLE", "pending_groups": len(state.snapshot()), "recovery_date": run_date.isoformat()}


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scoped DWS listener; history stays in event_worker.py")
    parser.add_argument("--state-dir", required=True, help="private durable state directory")
    parser.add_argument("--dws", default="dws", help="DWS executable")
    parser.add_argument("--recover-once", action="store_true", help="perform one daily recovery catchup")
    parser.add_argument("--force-recovery", action="store_true", help="explicitly rerun today's recovery")
    parser.add_argument("--transport-smoke", action="store_true", help="connect only; never schedule history work")
    parser.add_argument("--duration-seconds", type=int, help="required bound for transport smoke")
    parser.add_argument(
        "--enable-todo-events", action="store_true", default=False,
        help="opt in to the role-scoped TODO lifecycle consumer",
    )
    parser.add_argument("--mode", choices=("shadow", "test", "production"), default="shadow")
    parser.add_argument("--write-ledger", action="store_true")
    parser.add_argument("--allow-external-writes", action="store_true")
    parser.add_argument("--llm-jev", action="store_true")
    parser.add_argument("--jev-v2-policy", action="store_true")
    parser.add_argument("--semantic-fallback", action="store_true")
    parser.add_argument("--title-fields", action="store_true")
    parser.add_argument("--date-role-fallback", action="store_true")
    parser.add_argument("--supervised-rollout", action="store_true")
    parser.add_argument("--not-before", metavar="RFC3339+08:00")
    parser.add_argument("--resource-dir", default="/root/.hermes/cache/scratch/dingtalk_auto_todo_events")
    return parser


def _worker_argv(args: argparse.Namespace, pending_event_state_dir: str | Path | None = None) -> list[str]:
    script = Path(__file__).resolve().with_name("event_worker.py")
    argv = [sys.executable, str(script), "--once", "--summary-only", "--mode", args.mode]
    for flag, enabled in (
        ("--write-ledger", args.write_ledger), ("--allow-external-writes", args.allow_external_writes),
        ("--llm-jev", args.llm_jev), ("--jev-v2-policy", args.jev_v2_policy),
        ("--semantic-fallback", args.semantic_fallback), ("--title-fields", args.title_fields),
        ("--date-role-fallback", args.date_role_fallback), ("--supervised-rollout", args.supervised_rollout),
    ):
        if enabled:
            argv.append(flag)
    if args.not_before:
        argv.extend(("--not-before", args.not_before))
    if pending_event_state_dir is not None:
        argv.extend(("--pending-event-state-dir", str(pending_event_state_dir)))
    argv.extend(("--resource-dir", args.resource_dir))
    if args.recover_once:
        argv.append("--daily-recovery")
    return argv


def main(argv: list[str] | None = None) -> int:
    args = _argument_parser().parse_args(argv)
    if args.allow_external_writes and (args.mode != "production" or not args.write_ledger):
        _argument_parser().error("external writes require production mode and --write-ledger")
    if args.llm_jev and args.jev_v2_policy:
        _argument_parser().error("--llm-jev and --jev-v2-policy are mutually exclusive")
    native_fallbacks = any((args.semantic_fallback, args.title_fields, args.date_role_fallback))
    if native_fallbacks and not args.jev_v2_policy:
        _argument_parser().error("native fallback flags require --jev-v2-policy")
    if args.supervised_rollout and (args.mode != "production" or not args.jev_v2_policy or not native_fallbacks):
        _argument_parser().error("--supervised-rollout requires production JevV2 and a native fallback")
    if args.mode == "production" and native_fallbacks and not args.supervised_rollout:
        _argument_parser().error("production native fallbacks require --supervised-rollout")
    if any((args.llm_jev, args.jev_v2_policy, native_fallbacks)) and not args.write_ledger:
        _argument_parser().error("resolver flags require --write-ledger for durable source idempotency")
    if args.force_recovery and not args.recover_once:
        _argument_parser().error("--force-recovery requires --recover-once")
    if args.transport_smoke and (args.duration_seconds is None or not 1 <= args.duration_seconds <= 1800):
        _argument_parser().error("--transport-smoke requires --duration-seconds in 1..1800")
    if args.transport_smoke and (args.write_ledger or args.allow_external_writes):
        _argument_parser().error("transport smoke is read-only and cannot enable writes")
    if args.recover_once and args.transport_smoke:
        _argument_parser().error("--recover-once and --transport-smoke are mutually exclusive")

    # Runtime adapters are imported only in the explicitly invoked operational CLI.
    from workflows.polardb_work_adapter.query import WorkQueryAdapter

    state = AtomicPendingState(args.state_dir)
    adapter = WorkQueryAdapter()
    scope_loader = lambda: load_configured_group_ids(adapter)
    if args.recover_once:
        cancel = threading.Event()
        previous_handlers: dict[int, Any] = {}
        for sig in (signal.SIGTERM, signal.SIGINT):
            previous_handlers[sig] = signal.signal(sig, lambda _signum, _frame: cancel.set())
        runner = lambda: run_worker_subprocess(
            _worker_argv(args, pending_event_state_dir=args.state_dir), cancel_event=cancel
        )
        coordinator = CatchupCoordinator(
            state, runner, Path(args.state_dir) / "work.lock",
            mode=args.mode, write_ledger=args.write_ledger,
        )
        try:
            safe = run_daily_recovery(
                state=state, scope_loader=scope_loader, coordinator=coordinator,
                force=args.force_recovery,
            )
        except (ScopeError, StateError) as exc:
            safe_log({"status": "BLOCKED", "reason": str(exc)})
            return 2
        finally:
            for sig, handler in previous_handlers.items():
                signal.signal(sig, handler)
        safe_log({"status": str(safe.get("status") or "UNKNOWN"),
                  "reason": str(safe.get("reason") or ""),
                  "pending_groups": len(state.snapshot())})
        return 2 if safe.get("status") in {"REVIEW", "UNKNOWN"} else 0

    if args.transport_smoke:
        cancel = threading.Event()
        transport = DwsEventTransport(args.state_dir, executable=args.dws, cancel_event=cancel)
        groups = scope_loader()
        if not groups:
            return 2
        previous_handlers: dict[int, Any] = {}
        def cancel_smoke() -> None:
            cancel_transport = getattr(transport, "cancel", None)
            if callable(cancel_transport):
                cancel_transport()
            else:
                cancel.set()
        for sig in (signal.SIGTERM, signal.SIGINT):
            previous_handlers[sig] = signal.signal(sig, lambda _signum, _frame: cancel_smoke())
        try:
            report = run_transport_smoke(
                groups=groups, transport=transport, duration_seconds=args.duration_seconds,
            )
            safe_log(report)
            return 0 if report.get("success") else 2
        finally:
            cancel_smoke()
            for sig, handler in previous_handlers.items():
                signal.signal(sig, handler)

    stop = threading.Event()

    def runner() -> dict[str, Any]:
        argv = _worker_argv(args, pending_event_state_dir=args.state_dir)
        for event in state.task_event_snapshot().values():
            argv.extend(("--todo-lifecycle-event", json.dumps(event, separators=(",", ":"))))
        return run_worker_subprocess(argv, cancel_event=stop)

    coordinator = CatchupCoordinator(
        state, runner, Path(args.state_dir) / "work.lock",
        mode=args.mode, write_ledger=args.write_ledger,
    )
    transport = DwsEventTransport(args.state_dir, executable=args.dws, cancel_event=stop)
    try:
        parity_ledger = DwsParityLedger.from_environment()
    except DwsParityError:
        parity_ledger = None
        safe_log({"status": "PARITY_UNAVAILABLE", "reason": "parity_state_unavailable"})
    supervisor = ListenerSupervisor(scope_loader=scope_loader, state=state,
                                    coordinator=coordinator, transport=transport,
                                    enable_todo_events=args.enable_todo_events,
                                    parity_ledger=parity_ledger)
    previous_handlers: dict[int, Any] = {}
    for sig in (signal.SIGTERM, signal.SIGINT):
        previous_handlers[sig] = signal.signal(sig, lambda _signum, _frame: supervisor.request_stop())
    try:
        return supervisor.run_forever()
    except (ScopeError, StateError) as exc:
        safe_log({"status": "BLOCKED", "reason": str(exc)})
        return 2
    finally:
        supervisor.close()
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    raise SystemExit(main())
