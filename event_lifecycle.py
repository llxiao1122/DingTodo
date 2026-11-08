"""Minimal TODO lifecycle event contract and exact-readback synchronization."""

from __future__ import annotations

from typing import Any, Iterable


TODO_EVENT_KEYS = frozenset({
    "user_todo_task_create",
    "user_todo_task_update",
    "user_todo_task_delete",
})

_CONTAINERS = ("data", "payload", "body", "event")
_ALIASES = {
    "event_key": ("type", "event_key", "eventKey"),
    "event_id": ("event_id", "eventId"),
    "task_id": ("task_id", "taskId"),
}


def _find_field(payload: Any, aliases: tuple[str, ...]) -> Any:
    pending = [payload]
    visited: set[int] = set()
    while pending:
        value = pending.pop(0)
        if not isinstance(value, dict) or id(value) in visited:
            continue
        visited.add(id(value))
        for key in aliases:
            if key in value:
                candidate = value[key]
                if isinstance(candidate, (str, int)) and not isinstance(candidate, bool):
                    return candidate
        for key in _CONTAINERS:
            nested = value.get(key)
            if isinstance(nested, dict):
                pending.append(nested)
    return None


def _stable_id(value: Any, *, field: str) -> str:
    if not isinstance(value, (str, int)) or isinstance(value, bool):
        raise ValueError(f"{field}_missing")
    normalized = str(value).strip()
    if not normalized or len(normalized) > 256 or any(ord(char) < 32 or ord(char) == 127 for char in normalized):
        raise ValueError(f"{field}_invalid")
    return normalized


def normalize_todo_lifecycle_event(payload: Any) -> dict[str, str]:
    """Return only the event key and stable event/task IDs; discard the payload."""
    event_key = _stable_id(_find_field(payload, _ALIASES["event_key"]), field="event_key")
    if event_key not in TODO_EVENT_KEYS:
        raise ValueError("unsupported_todo_event")
    return {
        "event_key": event_key,
        "event_id": _stable_id(_find_field(payload, _ALIASES["event_id"]), field="event_id"),
        "task_id": _stable_id(_find_field(payload, _ALIASES["task_id"]), field="task_id"),
    }


def validate_queued_todo_event(event: Any) -> dict[str, str]:
    if not isinstance(event, dict):
        raise ValueError("todo_event_invalid")
    normalized = normalize_todo_lifecycle_event(event)
    if any(event.get(key) != normalized[key] for key in ("event_key", "event_id", "task_id")):
        raise ValueError("todo_event_invalid")
    return normalized


def sync_todo_lifecycle_events(
    events: Iterable[dict[str, Any]],
    actions: Iterable[dict[str, Any]],
    *,
    dws: Any,
    writer: Any,
) -> dict[str, Any]:
    """Read each exact taskId, then update only its uniquely linked local action."""
    action_rows = [row for row in actions if isinstance(row, dict)]
    result: dict[str, Any] = {"processed": 0, "ignored": 0, "errors": []}
    seen: dict[str, dict[str, str]] = {}
    for raw in events:
        try:
            event = validate_queued_todo_event(raw)
        except (TypeError, ValueError):
            result["errors"].append("todo_event_invalid")
            continue
        prior = seen.get(event["event_id"])
        if prior is not None:
            if prior != event:
                result["errors"].append("todo_event_identity_conflict")
            continue
        seen[event["event_id"]] = event
        try:
            detail = dws.get_todo(event["task_id"])
        except Exception:
            result["errors"].append("todo_readback_failed")
            continue
        if (
            not isinstance(detail, dict)
            or str(detail.get("taskId") or "").strip() != event["task_id"]
        ):
            result["errors"].append("todo_readback_mismatch")
            continue

        matches = [
            row for row in action_rows
            if str(row.get("external_task_id") or "").strip() == event["task_id"]
        ]
        if not matches:
            # Current-user events include unrelated tasks; they are not evidence
            # that this pipeline owns the task, and are safe to acknowledge.
            result["ignored"] += 1
            continue
        if len(matches) != 1:
            result["errors"].append("todo_action_link_ambiguous")
            continue
        action = matches[0]
        action_id = action.get("action_id")
        if action_id in (None, ""):
            result["errors"].append("todo_action_id_missing")
            continue

        if event["event_key"] == "user_todo_task_delete":
            if detail.get("isDeleted") is not True:
                # A missing task response is not proof of deletion. Require an
                # explicit tombstone from exact readback before syncing it.
                result["errors"].append("todo_delete_tombstone_unverified")
                continue
            status, last_error = "REVIEW", "external_task_deleted"
        else:
            if detail.get("isDeleted") is True or not isinstance(detail.get("isDone"), bool):
                result["errors"].append("todo_status_readback_incomplete")
                continue
            status = "SUCCESS" if detail["isDone"] else "EXTERNAL_CREATED"
            last_error = None

        request: dict[str, Any] = {
            "operation": "save_action_result",
            "action_id": action_id,
            "status": status,
            "last_error": last_error,
        }
        attempts = action.get("attempt_count")
        if isinstance(attempts, int) and not isinstance(attempts, bool) and attempts >= 0:
            request["attempt_count"] = attempts
        try:
            writer.execute(request)
        except Exception:
            result["errors"].append("todo_action_sync_failed")
            continue
        result["processed"] += 1
    return result
