"""Validation policy for the bounded PolarDB writer."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any


LOCAL_TZ = timezone(timedelta(hours=8))
WRITE_OPERATIONS = frozenset(
    {
        "record_message",
        "claim_action",
        "claim_child_action",
        "save_action_result",
        "save_assignment_audit",
        "link_external_task",
        "advance_scan_state",
        "review_area_duty",
        "resolve_review_ignore",
        "claim_review_create",
        "repair_review_duplicate_family",
        "detach_review_duplicate_action",
    }
)
ACTION_STATUSES = frozenset({"CLAIMED", "EXTERNAL_CREATED", "FAILED", "SUCCESS", "IGNORED", "REVIEW"})
CHILD_SCOPE_KEYS = frozenset({"asset_inventory_permissions", "asset_tags", "online_process"})
ASSIGNMENT_AUDIT_FIELDS = frozenset({
    "rule_version", "source", "rule_id", "primary_owner", "collaborators",
    "awareness_only", "awareness_names", "manual_assignment_required", "assignment_basis",
    "handover_evidence_ids", "handover_source", "handover_status", "hygiene_review",
    "database_conflict", "conflict_resolution", "legacy_configuration_recheck_required",
})
ASSIGNMENT_AUDIT_SOURCES = frozenset({
    "rule-driven-task-routing/references/confirmed-assignment-habits.md",
    "event_worker/explicit-assignment-fallback",
})
HANDOVER_SOURCES = frozenset({
    "dws_readback", "reviewed_area_duty", "user_confirmed_month_anchor",
    "not_required_explicit_assignment",
})

_ALLOWED_TRANSITIONS = {
    None: ACTION_STATUSES,
    "CLAIMED": {"CLAIMED", "EXTERNAL_CREATED", "FAILED", "REVIEW", "IGNORED"},
    "REVIEW": {"REVIEW", "CLAIMED", "EXTERNAL_CREATED", "IGNORED"},
    "EXTERNAL_CREATED": {"EXTERNAL_CREATED", "SUCCESS", "FAILED", "REVIEW"},
    "FAILED": {"FAILED", "CLAIMED", "REVIEW"},
    "IGNORED": {"IGNORED", "REVIEW"},
    "SUCCESS": {"SUCCESS"},
}


class InvalidWriteRequest(ValueError):
    pass


def _text(value: Any, field: str, *, required: bool = False, max_length: int = 512) -> str:
    text = "" if value is None else str(value).strip()
    if required and not text:
        raise InvalidWriteRequest(f"missing:{field}")
    if len(text) > max_length:
        raise InvalidWriteRequest(f"too_long:{field}")
    return text


def _positive_int(value: Any, field: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise InvalidWriteRequest(f"invalid:{field}") from exc
    if result <= 0 or result > 18_446_744_073_709_551_615:
        raise InvalidWriteRequest(f"invalid:{field}")
    return result


def _ascii_text(value: Any, field: str, *, required: bool = False, max_length: int = 128) -> str:
    text = _text(value, field, required=required, max_length=max_length)
    try:
        text.encode("ascii")
    except UnicodeEncodeError as exc:
        raise InvalidWriteRequest(f"non_ascii:{field}") from exc
    return text


def _iso_date(value: Any, field: str) -> date:
    text = _text(value, field, required=True, max_length=10)
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise InvalidWriteRequest(f"invalid:{field}") from exc


def local_datetime(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = _text(value, field, required=True, max_length=40)
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise InvalidWriteRequest(f"invalid:{field}") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(LOCAL_TZ).replace(tzinfo=None)
    return parsed


def _json_value(value: Any, field: str) -> Any:
    if value is None:
        return None
    if not isinstance(value, (dict, list, str, int, float, bool)):
        raise InvalidWriteRequest(f"invalid:{field}")
    return value


def _bounded_names(value: Any, field: str, *, maximum: int = 6) -> list[str]:
    if not isinstance(value, list) or len(value) > maximum:
        raise InvalidWriteRequest(f"invalid:{field}")
    rows = [_text(item, field, required=True, max_length=64) for item in value]
    if len(rows) != len(set(rows)):
        raise InvalidWriteRequest(f"duplicate:{field}")
    return rows


def _normalize_assignment_audit(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise InvalidWriteRequest("assignment_audit_must_be_object")
    unknown = set(value) - ASSIGNMENT_AUDIT_FIELDS
    if unknown:
        raise InvalidWriteRequest("unknown_assignment_audit_fields:" + ",".join(sorted(unknown)))
    for field in ("awareness_only", "manual_assignment_required"):
        if type(value.get(field)) is not bool:
            raise InvalidWriteRequest(f"invalid:{field}")
    if "legacy_configuration_recheck_required" in value and type(value["legacy_configuration_recheck_required"]) is not bool:
        raise InvalidWriteRequest("invalid:legacy_configuration_recheck_required")
    rule_id = _text(value.get("rule_id"), "rule_id", max_length=8) or None
    if rule_id is not None and rule_id not in {"A", "B", "C", "D", "E", "F", "G"}:
        raise InvalidWriteRequest("invalid:rule_id")
    source = _text(value.get("source"), "source", required=True, max_length=160)
    if source not in ASSIGNMENT_AUDIT_SOURCES:
        raise InvalidWriteRequest("invalid:source")
    handover_source = _text(value.get("handover_source"), "handover_source", max_length=64) or None
    if handover_source is not None and handover_source not in HANDOVER_SOURCES:
        raise InvalidWriteRequest("invalid:handover_source")
    evidence_ids = value.get("handover_evidence_ids", [])
    if not isinstance(evidence_ids, list) or len(evidence_ids) > 8:
        raise InvalidWriteRequest("invalid:handover_evidence_ids")
    evidence_ids = [_text(item, "handover_evidence_ids", required=True, max_length=128) for item in evidence_ids]
    if len(evidence_ids) != len(set(evidence_ids)):
        raise InvalidWriteRequest("duplicate:handover_evidence_ids")
    database_conflict = value.get("database_conflict")
    if database_conflict is not None:
        if not isinstance(database_conflict, dict) or set(database_conflict) != {"configured_owner", "selected_owner"}:
            raise InvalidWriteRequest("invalid:database_conflict")
        database_conflict = {
            key: _text(database_conflict[key], f"database_conflict.{key}", required=True, max_length=64)
            for key in ("configured_owner", "selected_owner")
        }
    normalized = {
        "rule_version": _text(value.get("rule_version"), "rule_version", required=True, max_length=80),
        "source": source,
        "rule_id": rule_id,
        "primary_owner": _text(value.get("primary_owner"), "primary_owner", max_length=64) or None,
        "collaborators": _bounded_names(value.get("collaborators", []), "collaborators"),
        "awareness_only": value["awareness_only"],
        "awareness_names": _bounded_names(value.get("awareness_names", []), "awareness_names"),
        "manual_assignment_required": value["manual_assignment_required"],
        "assignment_basis": _text(value.get("assignment_basis"), "assignment_basis", required=True, max_length=80),
        "handover_evidence_ids": evidence_ids,
        "handover_source": handover_source,
    }
    for field in ("handover_status", "hygiene_review", "conflict_resolution"):
        if value.get(field) not in (None, ""):
            normalized[field] = _text(value[field], field, max_length=128)
    if database_conflict is not None:
        normalized["database_conflict"] = database_conflict
    if "legacy_configuration_recheck_required" in value:
        normalized["legacy_configuration_recheck_required"] = value["legacy_configuration_recheck_required"]
    if len(json.dumps(normalized, ensure_ascii=False, separators=(",", ":")).encode("utf-8")) > 4096:
        raise InvalidWriteRequest("assignment_audit_too_large")
    return normalized


def transition_allowed(current: str | None, target: str) -> bool:
    return target in _ALLOWED_TRANSITIONS.get(current, set())


def cursor_is_not_behind(
    old_time: Any,
    old_id: Any,
    new_time: Any,
    new_id: Any,
) -> bool:
    if old_time in (None, ""):
        return True
    old_dt = local_datetime(old_time, "old_last_msg_time")
    new_dt = local_datetime(new_time, "last_msg_time")
    if new_dt > old_dt:
        return True
    if new_dt < old_dt:
        return False
    return _text(new_id, "last_msg_id", required=True, max_length=512) >= _text(old_id, "old_last_msg_id", max_length=512)


def _reject_unknown(request: dict[str, Any], allowed: set[str]) -> None:
    unknown = set(request) - allowed
    if unknown:
        raise InvalidWriteRequest("unknown_parameters:" + ",".join(sorted(unknown)))


_REVIEW_ACTION_SNAPSHOT_FIELDS = frozenset({
    "action_id", "conversation_id", "message_id", "action_key", "status", "business_key",
    "external_task_id", "todo_id", "duplicate_of_action_id", "parent_action_id", "updated_at",
})


def _review_action_id(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise InvalidWriteRequest(f"invalid:{field}")
    if isinstance(value, str):
        if len(value) > 20 or not value.isascii():
            raise InvalidWriteRequest(f"invalid:{field}")
        text = value.strip()
        if not text or not text.isdecimal():
            raise InvalidWriteRequest(f"invalid:{field}")
        result = int(text)
    else:
        result = value
    if result <= 0 or result > 18_446_744_073_709_551_615:
        raise InvalidWriteRequest(f"invalid:{field}")
    return result


def _review_snapshot_text(value: Any, field: str, *, max_length: int, nullable: bool = False) -> str | None:
    if value is None:
        if nullable:
            return None
        raise InvalidWriteRequest(f"missing:{field}")
    if not isinstance(value, str):
        raise InvalidWriteRequest(f"invalid:{field}")
    if len(value) > max_length:
        raise InvalidWriteRequest(f"too_long:{field}")
    try:
        value.encode("ascii")
    except UnicodeEncodeError as exc:
        raise InvalidWriteRequest(f"non_ascii:{field}") from exc
    text = value.strip()
    if not text:
        if nullable:
            return None
        raise InvalidWriteRequest(f"missing:{field}")
    return text


def _normalize_review_action_snapshot(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != _REVIEW_ACTION_SNAPSHOT_FIELDS:
        raise InvalidWriteRequest("invalid:review_action_snapshot")
    return {
        "action_id": _review_action_id(value["action_id"], "snapshot_action_id"),
        "conversation_id": _review_snapshot_text(value["conversation_id"], "conversation_id", max_length=128),
        "message_id": _review_snapshot_text(value["message_id"], "message_id", max_length=128),
        "action_key": _review_snapshot_text(value["action_key"], "action_key", max_length=64),
        "status": _review_snapshot_text(value["status"], "status", max_length=32),
        "business_key": _review_snapshot_text(value["business_key"], "business_key", max_length=64, nullable=True),
        "external_task_id": _review_snapshot_text(value["external_task_id"], "external_task_id", max_length=128, nullable=True),
        "todo_id": _review_nullable_review_action_id(value["todo_id"], "todo_id"),
        "duplicate_of_action_id": _review_nullable_review_action_id(value["duplicate_of_action_id"], "duplicate_of_action_id"),
        "parent_action_id": _review_nullable_review_action_id(value["parent_action_id"], "parent_action_id"),
        "updated_at": _review_action_datetime(value["updated_at"]),
    }


def _review_nullable_review_action_id(value: Any, field: str) -> int | None:
    return None if value is None else _review_action_id(value, field)


def _review_action_datetime(value: Any) -> datetime:
    if not isinstance(value, datetime):
        if (not isinstance(value, str) or len(value) > 40
                or ("T" not in value and " " not in value)):
            raise InvalidWriteRequest("invalid:updated_at")
    return local_datetime(value, "updated_at")


def normalize_request(request: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(request, dict):
        raise InvalidWriteRequest("request_must_be_object")
    operation = _text(request.get("operation"), "operation", required=True, max_length=64)
    if operation not in WRITE_OPERATIONS:
        raise InvalidWriteRequest("invalid:operation")
    dry_run = request.get("dry_run", False)
    if not isinstance(dry_run, bool):
        raise InvalidWriteRequest("invalid:dry_run")

    if operation in {"resolve_review_ignore", "claim_review_create"}:
        allowed = {"operation", "dry_run", "action_id", "conversation_id", "message_id",
                   "expected_updated_at", "expected_business_key"}
        _reject_unknown(request, allowed)
        return {
            "operation": operation, "dry_run": dry_run,
            "action_id": _positive_int(request.get("action_id"), "action_id"),
            "conversation_id": _ascii_text(request.get("conversation_id"), "conversation_id", required=True, max_length=128),
            "message_id": _ascii_text(request.get("message_id"), "message_id", required=True, max_length=128),
            "expected_updated_at": local_datetime(request.get("expected_updated_at"), "expected_updated_at"),
            "expected_business_key": _ascii_text(request.get("expected_business_key"), "expected_business_key", max_length=64) or None,
        }

    if operation == "detach_review_duplicate_action":
        allowed = {
            "operation", "dry_run", "target_action_id", "expected_canonical_action_id",
            "expected_target", "expected_canonical",
        }
        _reject_unknown(request, allowed)
        target_action_id = _review_action_id(request.get("target_action_id"), "target_action_id")
        canonical_action_id = _review_action_id(
            request.get("expected_canonical_action_id"), "expected_canonical_action_id",
        )
        if target_action_id == canonical_action_id:
            raise InvalidWriteRequest("review_duplicate_actions_must_be_distinct")
        target = _normalize_review_action_snapshot(request.get("expected_target"))
        canonical = _normalize_review_action_snapshot(request.get("expected_canonical"))
        if target["action_id"] != target_action_id or canonical["action_id"] != canonical_action_id:
            raise InvalidWriteRequest("review_duplicate_snapshot_id_mismatch")
        for snapshot in (target, canonical):
            if snapshot["action_key"] != "todo" or snapshot["status"] != "REVIEW":
                raise InvalidWriteRequest("review_duplicate_snapshot_not_review")
            if snapshot["parent_action_id"] is not None:
                raise InvalidWriteRequest("review_duplicate_snapshot_has_parent")
        if target["duplicate_of_action_id"] != canonical_action_id:
            raise InvalidWriteRequest("review_duplicate_target_pointer_mismatch")
        if canonical["duplicate_of_action_id"] is not None:
            raise InvalidWriteRequest("review_duplicate_canonical_has_pointer")
        if target["external_task_id"] is not None or target["todo_id"] is not None:
            raise InvalidWriteRequest("review_duplicate_target_linked")
        return {
            "operation": operation,
            "dry_run": dry_run,
            "target_action_id": target_action_id,
            "expected_canonical_action_id": canonical_action_id,
            "expected_target": target,
            "expected_canonical": canonical,
        }

    if operation == "repair_review_duplicate_family":
        allowed = {"operation", "dry_run", "canonical_action_id", "old_canonical_action_id",
                   "duplicate_action_ids", "expected_rows"}
        _reject_unknown(request, allowed)
        canonical_id = _positive_int(request.get("canonical_action_id"), "canonical_action_id")
        old_canonical_id = _positive_int(request.get("old_canonical_action_id"), "old_canonical_action_id")
        raw_duplicates = request.get("duplicate_action_ids")
        if not isinstance(raw_duplicates, list) or not 1 <= len(raw_duplicates) <= 10:
            raise InvalidWriteRequest("invalid:duplicate_action_ids")
        duplicate_ids = [_positive_int(value, "duplicate_action_id") for value in raw_duplicates]
        if (len(duplicate_ids) != len(set(duplicate_ids))
                or canonical_id in duplicate_ids or old_canonical_id in duplicate_ids
                or canonical_id == old_canonical_id):
            raise InvalidWriteRequest("invalid:duplicate_action_ids")
        raw_rows = request.get("expected_rows")
        if not isinstance(raw_rows, list) or len(raw_rows) != len(duplicate_ids) + 2:
            raise InvalidWriteRequest("invalid:expected_rows")
        rows = []
        row_ids = set()
        for raw in raw_rows:
            if not isinstance(raw, dict) or set(raw) != {
                    "action_id", "conversation_id", "message_id", "action_key", "status",
                    "business_key", "external_task_id", "duplicate_of_action_id",
                    "parent_action_id", "updated_at"}:
                raise InvalidWriteRequest("invalid:expected_row")
            action_id = _positive_int(raw.get("action_id"), "expected_action_id")
            if action_id in row_ids:
                raise InvalidWriteRequest("duplicate:expected_action_id")
            row_ids.add(action_id)
            duplicate_of = (_positive_int(raw["duplicate_of_action_id"], "duplicate_of_action_id")
                            if raw.get("duplicate_of_action_id") is not None else None)
            parent_id = (_positive_int(raw["parent_action_id"], "parent_action_id")
                         if raw.get("parent_action_id") is not None else None)
            rows.append({
                "action_id": action_id,
                "conversation_id": _ascii_text(raw.get("conversation_id"), "conversation_id", required=True, max_length=128),
                "message_id": _ascii_text(raw.get("message_id"), "message_id", required=True, max_length=128),
                "action_key": _ascii_text(raw.get("action_key"), "action_key", required=True, max_length=64),
                "status": _text(raw.get("status"), "status", required=True, max_length=32),
                "business_key": _ascii_text(raw.get("business_key"), "business_key", max_length=64) or None,
                "external_task_id": _ascii_text(raw.get("external_task_id"), "external_task_id", max_length=128) or None,
                "duplicate_of_action_id": duplicate_of,
                "parent_action_id": parent_id,
                "updated_at": local_datetime(raw.get("updated_at"), "updated_at"),
            })
        if row_ids != {canonical_id, old_canonical_id, *duplicate_ids}:
            raise InvalidWriteRequest("expected_row_id_set_mismatch")
        if any(row["status"] != "REVIEW" or row["action_key"] != "todo" for row in rows):
            raise InvalidWriteRequest("review_duplicate_family_status_mismatch")
        return {
            "operation": operation,
            "dry_run": dry_run,
            "canonical_action_id": canonical_id,
            "old_canonical_action_id": old_canonical_id,
            "duplicate_action_ids": duplicate_ids,
            "expected_rows": rows,
        }

    if operation == "record_message":
        allowed = {
            "operation", "dry_run", "conversation_id", "message_id", "group_name", "sender_id",
            "sender_name", "message_time", "text", "msg_type", "resource_refs", "quoted_message",
            "raw_payload", "source",
        }
        _reject_unknown(request, allowed)
        return {
            "operation": operation,
            "dry_run": dry_run,
            "conversation_id": _text(request.get("conversation_id"), "conversation_id", required=True, max_length=256),
            "message_id": _text(request.get("message_id"), "message_id", required=True, max_length=512),
            "group_name": _text(request.get("group_name"), "group_name", required=True, max_length=256),
            "sender_id": _text(request.get("sender_id"), "sender_id", max_length=512) or None,
            "sender_name": _text(request.get("sender_name"), "sender_name", max_length=256) or None,
            "message_time": local_datetime(request.get("message_time"), "message_time"),
            "text": _text(request.get("text"), "text", max_length=100_000) or None,
            "msg_type": _text(request.get("msg_type"), "msg_type", max_length=64) or None,
            "resource_refs": _json_value(request.get("resource_refs"), "resource_refs"),
            "quoted_message": _json_value(request.get("quoted_message"), "quoted_message"),
            "raw_payload": _json_value(request.get("raw_payload"), "raw_payload"),
            "source": _text(request.get("source") or "hermes", "source", required=True, max_length=64),
        }

    if operation == "claim_action":
        allowed = {
            "operation", "dry_run", "conversation_id", "message_id", "action_key", "business_key", "status",
        }
        _reject_unknown(request, allowed)
        status = _text(request.get("status") or "CLAIMED", "status", max_length=32)
        if status not in {"CLAIMED", "REVIEW", "IGNORED"}:
            raise InvalidWriteRequest("invalid:status")
        return {
            "operation": operation,
            "dry_run": dry_run,
            "conversation_id": _ascii_text(request.get("conversation_id"), "conversation_id", required=True, max_length=128),
            "message_id": _ascii_text(request.get("message_id"), "message_id", required=True, max_length=128),
            "action_key": _ascii_text(request.get("action_key"), "action_key", required=True, max_length=64),
            "business_key": _ascii_text(request.get("business_key"), "business_key", max_length=64) or None,
            "status": status,
        }

    if operation == "claim_child_action":
        allowed = {
            "operation", "dry_run", "contract_version", "conversation_id", "message_id",
            "action_key", "parent_action_id", "parent_task_id", "business_scope", "scope_key", "status",
        }
        _reject_unknown(request, allowed)
        contract_version = _text(request.get("contract_version"), "contract_version", required=True, max_length=32)
        if contract_version != "child-actions/1.0":
            raise InvalidWriteRequest("child_contract_version_unsupported")
        conversation_id = _ascii_text(request.get("conversation_id"), "conversation_id", required=True, max_length=128)
        message_id = _ascii_text(request.get("message_id"), "message_id", required=True, max_length=128)
        parent_task_id = _ascii_text(request.get("parent_task_id"), "parent_task_id", required=True, max_length=128)
        business_scope = _ascii_text(request.get("business_scope"), "business_scope", required=True, max_length=64)
        scope_key = _text(request.get("scope_key"), "scope_key", required=True, max_length=64)
        if scope_key not in CHILD_SCOPE_KEYS:
            raise InvalidWriteRequest("invalid:scope_key")
        action_key = _ascii_text(request.get("action_key"), "action_key", required=True, max_length=64)
        identity = "\0".join((conversation_id, message_id, business_scope, scope_key))
        expected_action_key = "child:" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]
        if action_key != expected_action_key:
            raise InvalidWriteRequest("child_action_key_mismatch")
        status = _text(request.get("status") or "CLAIMED", "status", required=True, max_length=24)
        if status != "CLAIMED":
            raise InvalidWriteRequest("invalid:status")
        return {
            "operation": operation,
            "dry_run": dry_run,
            "contract_version": contract_version,
            "conversation_id": conversation_id,
            "message_id": message_id,
            "action_key": action_key,
            "parent_action_id": _positive_int(request.get("parent_action_id"), "parent_action_id"),
            "parent_task_id": parent_task_id,
            "business_scope": business_scope,
            "scope_key": scope_key,
            "status": status,
        }

    if operation == "save_assignment_audit":
        allowed = {"operation", "dry_run", "action_id", "assignment_audit"}
        _reject_unknown(request, allowed)
        return {
            "operation": operation,
            "dry_run": dry_run,
            "action_id": _positive_int(request.get("action_id"), "action_id"),
            "assignment_audit": _normalize_assignment_audit(request.get("assignment_audit")),
        }

    if operation in {"save_action_result", "link_external_task"}:
        allowed = {
            "operation", "dry_run", "action_id", "status", "external_task_id", "todo_id",
            "duplicate_of_action_id", "last_error", "attempt_count",
        }
        _reject_unknown(request, allowed)
        action_id = _positive_int(request.get("action_id"), "action_id")
        external_task_id = _ascii_text(request.get("external_task_id"), "external_task_id", max_length=128) or None
        if operation == "link_external_task":
            if not external_task_id:
                raise InvalidWriteRequest("missing:external_task_id")
            status = "EXTERNAL_CREATED"
        else:
            status = _text(request.get("status"), "status", required=True, max_length=32)
            if status not in ACTION_STATUSES:
                raise InvalidWriteRequest("invalid:status")
        attempt_count = request.get("attempt_count")
        if attempt_count is not None:
            attempt_count = int(attempt_count)
            if attempt_count < 0 or attempt_count > 4_294_967_295:
                raise InvalidWriteRequest("invalid:attempt_count")
        return {
            "operation": operation,
            "dry_run": dry_run,
            "action_id": action_id,
            "status": status,
            "external_task_id": external_task_id,
            "todo_id": _positive_int(request["todo_id"], "todo_id") if request.get("todo_id") is not None else None,
            "duplicate_of_action_id": _positive_int(request["duplicate_of_action_id"], "duplicate_of_action_id") if request.get("duplicate_of_action_id") is not None else None,
            "last_error": _text(request.get("last_error"), "last_error", max_length=4000) or None,
            "attempt_count": attempt_count,
        }

    if operation == "review_area_duty":
        allowed = {
            "operation", "dry_run", "duty_id", "area_name", "staff_name", "cycle_start", "cycle_end",
        }
        _reject_unknown(request, allowed)
        cycle_start = _iso_date(request.get("cycle_start"), "cycle_start")
        cycle_end = _iso_date(request.get("cycle_end"), "cycle_end")
        if cycle_end < cycle_start:
            raise InvalidWriteRequest("cycle_end_before_cycle_start")
        return {
            "operation": operation,
            "dry_run": dry_run,
            "duty_id": _positive_int(request.get("duty_id"), "duty_id"),
            "area_name": _text(request.get("area_name"), "area_name", required=True, max_length=128),
            "staff_name": _text(request.get("staff_name"), "staff_name", required=True, max_length=128),
            "cycle_start": cycle_start,
            "cycle_end": cycle_end,
        }

    allowed = {"operation", "dry_run", "group_name", "conversation_id", "last_msg_time", "last_msg_id"}
    _reject_unknown(request, allowed)
    return {
        "operation": operation,
        "dry_run": dry_run,
        "group_name": _text(request.get("group_name"), "group_name", required=True, max_length=256),
        "conversation_id": _text(request.get("conversation_id"), "conversation_id", max_length=256) or None,
        "last_msg_time": local_datetime(request.get("last_msg_time"), "last_msg_time"),
        "last_msg_id": _text(request.get("last_msg_id"), "last_msg_id", required=True, max_length=512),
    }
