"""Private identity-only comparison ledger for DWS events and history scans."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import tempfile
import time
from typing import Any, Iterable


CHINA_TZ = timezone(timedelta(hours=8))
RETENTION_SECONDS = 30 * 24 * 60 * 60
DEFAULT_EVENT_GRACE_SECONDS = 20 * 60
MESSAGE_ID_KEYS = ("messageId", "message_id", "openMessageId", "open_message_id")
CONVERSATION_ID_KEYS = (
    "conversationId", "conversation_id", "openConversationId", "open_conversation_id",
)
TIME_KEYS = ("createTime", "create_time", "time")
CONTAINERS = ("data", "payload", "body", "event", "content")


class DwsParityError(RuntimeError):
    """The private parity state is unavailable or invalid."""


def _secure_state_dir(path: Path) -> Path:
    if path.is_symlink():
        raise DwsParityError("parity_state_directory_invalid")
    try:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        path.chmod(0o700)
    except OSError:
        raise DwsParityError("parity_state_directory_unavailable") from None
    if not path.is_dir() or path.is_symlink():
        raise DwsParityError("parity_state_directory_invalid")
    return path


def _message_object(payload: Any) -> dict[str, Any] | None:
    pending = [payload]
    visited: set[int] = set()
    while pending:
        value = pending.pop()
        if isinstance(value, dict):
            if id(value) in visited:
                continue
            visited.add(id(value))
            if any(key in value for key in MESSAGE_ID_KEYS):
                return value
            for key in CONTAINERS:
                nested = value.get(key)
                if isinstance(nested, (dict, list)):
                    pending.append(nested)
            for nested in value.values():
                if isinstance(nested, (dict, list)):
                    pending.append(nested)
        elif isinstance(value, list):
            pending.extend(value)
    return None


def _as_identity(value: Any, keys: Iterable[str]) -> str:
    if not isinstance(value, dict):
        return ""
    for key in keys:
        candidate = value.get(key)
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return ""


def _timestamp(value: Any, *, fallback: float) -> float:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            numeric = float(value)
            if numeric > 100_000_000_000:
                numeric /= 1000
            if numeric > 0:
                return numeric
        except (OverflowError, ValueError):
            return fallback
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=CHINA_TZ)
            return parsed.timestamp()
        except (TypeError, ValueError, OverflowError):
            return fallback
    return fallback


class DwsParityLedger:
    """Keep only salted-independent hashes and counts, never message content."""

    def __init__(self, state_dir: str | Path, *, now: Any = time.time):
        self.state_dir = _secure_state_dir(Path(state_dir))
        self.path = self.state_dir / "event-history-parity.json"
        self.lock_path = self.state_dir / "event-history-parity.lock"
        self.now = now
        if self.path.exists():
            if self.path.is_symlink():
                raise DwsParityError("parity_state_file_invalid")
            try:
                self.path.chmod(0o600)
            except OSError:
                raise DwsParityError("parity_state_permissions_unavailable") from None
        with self._locked():
            payload = self._load_unlocked()
            if not payload:
                self._save_unlocked(self._empty(self.now()))

    @classmethod
    def from_environment(cls) -> DwsParityLedger | None:
        path = os.environ.get("DWS_PARITY_STATE_DIR", "").strip()
        return cls(path) if path else None

    def _locked(self):
        try:
            fd = os.open(
                self.lock_path,
                os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            os.fchmod(fd, 0o600)
        except OSError:
            raise DwsParityError("parity_state_lock_unavailable") from None

        class Lock:
            def __enter__(lock_self):
                fcntl.flock(fd, fcntl.LOCK_EX)
                return None

            def __exit__(lock_self, _type: Any, _value: Any, _traceback: Any) -> None:
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                finally:
                    os.close(fd)

        return Lock()

    @staticmethod
    def _empty(now: float) -> dict[str, Any]:
        return {
            "version": 1,
            "started_at": now,
            "last_event_at": None,
            "last_history_at": None,
            "events_missing_identity": 0,
            "history_scans": 0,
            "ready_groups": {},
            "scans": {},
            "events": {},
            "history": {},
        }

    def _load_unlocked(self) -> dict[str, Any]:
        if self.path.is_symlink():
            raise DwsParityError("parity_state_file_invalid")
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}
        except (OSError, UnicodeError, json.JSONDecodeError):
            raise DwsParityError("parity_state_file_unreadable") from None
        if (
            not isinstance(payload, dict)
            or payload.get("version") != 1
            or not isinstance(payload.get("events"), dict)
            or not isinstance(payload.get("history"), dict)
            or not isinstance(payload.get("ready_groups"), dict)
            or not isinstance(payload.get("scans"), dict)
            or isinstance(payload.get("started_at"), bool)
            or not isinstance(payload.get("started_at"), (int, float))
            or not math.isfinite(float(payload["started_at"]))
        ):
            raise DwsParityError("parity_state_file_invalid")
        for source in ("events", "history"):
            for key, row in payload[source].items():
                if (
                    not isinstance(key, str)
                    or not re.fullmatch(r"[a-f0-9]{64}", key)
                    or not isinstance(row, dict)
                    or not isinstance(row.get("group_hash"), str)
                    or not re.fullmatch(r"[a-f0-9]{64}", row["group_hash"])
                    or any(
                        isinstance(row.get(field), bool)
                        or not isinstance(row.get(field), (int, float))
                        or not math.isfinite(float(row[field]))
                        for field in ("created_at", "seen_at")
                    )
                ):
                    raise DwsParityError("parity_state_file_invalid")
        for key, ready_at in payload["ready_groups"].items():
            if (
                not isinstance(key, str)
                or not re.fullmatch(r"[a-f0-9]{64}", key)
                or isinstance(ready_at, bool)
                or not isinstance(ready_at, (int, float))
                or not math.isfinite(float(ready_at))
            ):
                raise DwsParityError("parity_state_file_invalid")
        for key, scan in payload["scans"].items():
            if (
                not isinstance(key, str)
                or not re.fullmatch(r"[a-f0-9]{64}", key)
                or not isinstance(scan, dict)
                or any(
                    isinstance(scan.get(field), bool)
                    or not isinstance(scan.get(field), (int, float))
                    or not math.isfinite(float(scan[field]))
                    for field in ("start_at", "end_at", "completed_at")
                )
                or scan["end_at"] < scan["start_at"]
            ):
                raise DwsParityError("parity_state_file_invalid")
        for field in ("events_missing_identity", "history_scans"):
            count = payload.get(field, 0)
            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                raise DwsParityError("parity_state_file_invalid")
        return payload

    def _save_unlocked(self, payload: dict[str, Any]) -> None:
        tmp_path: str | None = None
        try:
            fd, tmp_path = tempfile.mkstemp(prefix=".parity-state-", dir=self.state_dir)
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(payload, stream, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(tmp_path, self.path)
            tmp_path = None
            dir_fd = os.open(self.state_dir, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            raise DwsParityError("parity_state_write_failed") from None
        finally:
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

    @staticmethod
    def _message_hash(conversation_id: str, message_id: str) -> str:
        return hashlib.sha256(f"{conversation_id}\0{message_id}".encode("utf-8")).hexdigest()

    def _prune(self, payload: dict[str, Any], now: float) -> None:
        cutoff = now - RETENTION_SECONDS
        for source in ("events", "history"):
            rows = payload[source]
            payload[source] = {
                key: value for key, value in rows.items()
                if isinstance(value, dict)
                and isinstance(value.get("created_at"), (int, float))
                and value["created_at"] >= cutoff
            }
        payload["ready_groups"] = {
            key: value for key, value in payload["ready_groups"].items()
            if value >= cutoff
        }
        payload["scans"] = {
            key: value for key, value in payload["scans"].items()
            if value["completed_at"] >= cutoff
        }

    @staticmethod
    def _group_hash(conversation_id: str) -> str:
        return hashlib.sha256(conversation_id.encode("utf-8")).hexdigest()

    def _record(
        self,
        source: str,
        conversation_id: str,
        message_id: str,
        created_at: float,
    ) -> bool:
        now = float(self.now())
        key = self._message_hash(conversation_id, message_id)
        group_hash = self._group_hash(conversation_id)
        with self._locked():
            payload = self._load_unlocked() or self._empty(now)
            if created_at < float(payload["started_at"]):
                return False
            rows = payload[source]
            is_new = key not in rows
            rows[key] = {
                "created_at": float(created_at),
                "group_hash": group_hash,
                "seen_at": now,
            }
            payload["last_event_at" if source == "events" else "last_history_at"] = now
            self._prune(payload, now)
            self._save_unlocked(payload)
            return is_new

    def record_event(self, payload: Any, *, expected_group_id: str) -> bool:
        now = float(self.now())
        message = _message_object(payload)
        conversation_id = _as_identity(message, CONVERSATION_ID_KEYS) or expected_group_id
        message_id = _as_identity(message, MESSAGE_ID_KEYS)
        if conversation_id != expected_group_id or not message_id:
            with self._locked():
                current = self._load_unlocked() or self._empty(now)
                if not message_id:
                    current["events_missing_identity"] = int(current.get("events_missing_identity") or 0) + 1
                    self._save_unlocked(current)
            return False
        create_time = next((message.get(key) for key in TIME_KEYS if key in message), None)
        return self._record("events", conversation_id, message_id, _timestamp(create_time, fallback=now))

    def record_group_ready(self, conversation_id: str) -> None:
        if not isinstance(conversation_id, str) or not conversation_id.strip():
            raise DwsParityError("parity_group_identity_invalid")
        now = float(self.now())
        with self._locked():
            payload = self._load_unlocked() or self._empty(now)
            payload["ready_groups"][self._group_hash(conversation_id)] = now
            self._save_unlocked(payload)

    def record_history(
        self,
        conversation_id: str,
        messages: Iterable[dict[str, Any]],
        *,
        scan_start: Any = None,
        scan_end: Any = None,
    ) -> int:
        if not isinstance(conversation_id, str) or not conversation_id.strip():
            raise DwsParityError("parity_group_identity_invalid")
        now = float(self.now())
        group_hash = self._group_hash(conversation_id)
        candidates: dict[str, float] = {}
        for message in messages:
            if not isinstance(message, dict):
                continue
            message_id = _as_identity(message, MESSAGE_ID_KEYS)
            if not message_id:
                message_id = str(message.get("message_id") or "").strip()
            if not message_id:
                continue
            actual_conversation = _as_identity(message, CONVERSATION_ID_KEYS)
            if not actual_conversation:
                actual_conversation = str(message.get("conversation_id") or "").strip()
            if actual_conversation != conversation_id:
                continue
            create_time = next((message.get(key) for key in TIME_KEYS if key in message), None)
            if create_time is None:
                create_time = message.get("create_time")
            created_at = _timestamp(create_time, fallback=0)
            if created_at > 0:
                candidates[self._message_hash(conversation_id, message_id)] = created_at
        with self._locked():
            payload = self._load_unlocked() or self._empty(now)
            started_at = float(payload["started_at"])
            rows = payload["history"]
            recorded = 0
            for key, created_at in candidates.items():
                if created_at < started_at:
                    continue
                if key not in rows:
                    recorded += 1
                rows[key] = {"created_at": created_at, "group_hash": group_hash, "seen_at": now}
            payload["history_scans"] = int(payload.get("history_scans") or 0) + 1
            payload["last_history_at"] = now
            start_at = _timestamp(scan_start, fallback=0)
            end_at = _timestamp(scan_end, fallback=0)
            if start_at > 0 and end_at >= start_at:
                payload["scans"][group_hash] = {
                    "start_at": start_at,
                    "end_at": end_at,
                    "completed_at": now,
                }
            self._prune(payload, now)
            self._save_unlocked(payload)
            return recorded

    def summary(self, *, now: float | None = None, grace_seconds: int = DEFAULT_EVENT_GRACE_SECONDS) -> dict[str, Any]:
        current_time = float(self.now() if now is None else now)
        with self._locked():
            payload = self._load_unlocked() or self._empty(current_time)
        event_rows = payload["events"]
        history_rows = payload["history"]
        event_keys = set(event_rows)
        history_keys = set(history_rows)
        matched = event_keys & history_keys
        event_only = event_keys - history_keys
        event_pending: set[str] = set()
        event_missing: set[str] = set()
        for key in event_only:
            event = event_rows[key]
            scan = payload["scans"].get(event["group_hash"])
            scan_covers_event = (
                isinstance(scan, dict)
                and float(scan["completed_at"]) >= float(event["seen_at"])
                and float(scan["start_at"]) <= float(event["created_at"]) <= float(scan["end_at"])
            )
            if scan_covers_event:
                event_missing.add(key)
            else:
                event_pending.add(key)
        history_pending_event: set[str] = set()
        history_only: set[str] = set()
        for key in history_keys - event_keys:
            message = history_rows[key]
            ready_at = payload["ready_groups"].get(message["group_hash"])
            if ready_at is None or float(message["created_at"]) < float(ready_at):
                continue
            if current_time - float(message["seen_at"]) < grace_seconds:
                history_pending_event.add(key)
            else:
                history_only.add(key)
        eligible_events = len(matched) + len(event_missing)
        return {
            "status": "collecting" if not event_keys or not payload["history_scans"] else "comparing",
            "started_at": datetime.fromtimestamp(float(payload["started_at"]), CHINA_TZ).isoformat(),
            "event_messages": len(event_keys),
            "history_messages": len(history_keys),
            "matched_messages": len(matched),
            "complete_history_scans": int(payload.get("history_scans") or 0),
            "event_pending_history": len(event_pending),
            "event_missing_after_complete_scan": len(event_missing),
            "history_pending_event": len(history_pending_event),
            "history_without_event": len(history_only),
            "events_missing_identity": int(payload.get("events_missing_identity") or 0),
            "event_coverage_percent": round(len(matched) / eligible_events * 100, 1) if eligible_events else None,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description="Show DWS event/history parity counts without message data")
    parser.add_argument("--state-dir", default=os.environ.get("DWS_PARITY_STATE_DIR"))
    args = parser.parse_args()
    if not args.state_dir:
        parser.error("--state-dir or DWS_PARITY_STATE_DIR is required")
    ledger = DwsParityLedger(args.state_dir)
    print(json.dumps(ledger.summary(), ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
