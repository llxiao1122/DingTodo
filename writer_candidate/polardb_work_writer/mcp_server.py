#!/usr/bin/env python3
"""MCP server exposing only the bounded PolarDB work_write tool."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from workflows.polardb_work_writer.policy import InvalidWriteRequest  # noqa: E402
from workflows.polardb_work_writer.writer import WorkWriter, WriteError  # noqa: E402

SERVER_INFO = {"name": "hermes-polardb-work-writer", "version": "0.2.0"}
_REVIEW_ACTION_SNAPSHOT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "action_id", "conversation_id", "message_id", "action_key", "status", "business_key",
        "external_task_id", "todo_id", "duplicate_of_action_id", "parent_action_id", "updated_at",
    ],
    "properties": {
        "action_id": {"type": "integer", "minimum": 1},
        "conversation_id": {"type": "string", "minLength": 1, "maxLength": 128},
        "message_id": {"type": "string", "minLength": 1, "maxLength": 128},
        "action_key": {"type": "string", "enum": ["todo"]},
        "status": {"type": "string", "enum": ["REVIEW"]},
        "business_key": {"type": ["string", "null"], "maxLength": 64},
        "external_task_id": {"type": ["string", "null"], "maxLength": 128},
        "todo_id": {"type": ["integer", "null"], "minimum": 1},
        "duplicate_of_action_id": {"type": ["integer", "null"], "minimum": 1},
        "parent_action_id": {"type": ["integer", "null"], "minimum": 1},
        "updated_at": {"type": "string", "minLength": 1, "maxLength": 40},
    },
}
TOOL_SCHEMA = {
    "name": "work_write",
    "description": (
        "Perform one allowlisted transactional write to the agentic work ledger, including scoped child actions and bounded assignment audit. "
        "No arbitrary SQL, schema changes, deletion, task completion, or group messaging."
    ),
    "inputSchema": {
        "type": "object",
        "additionalProperties": False,
        "required": ["operation"],
        "properties": {
            "operation": {
                "type": "string",
                "enum": [
                    "record_message",
                    "claim_action",
                    "claim_child_action",
                    "save_action_result",
                    "save_assignment_audit",
                    "link_external_task",
                    "advance_scan_state",
                    "review_area_duty",
                    "detach_review_duplicate_action",
                ],
            },
            "dry_run": {"type": "boolean", "default": False},
            "conversation_id": {"type": "string"},
            "message_id": {"type": "string"},
            "group_name": {"type": "string"},
            "message_time": {"type": "string"},
            "sender_id": {"type": "string"},
            "sender_name": {"type": "string"},
            "text": {"type": "string"},
            "msg_type": {"type": "string"},
            "resource_refs": {"type": ["object", "array", "string", "null"]},
            "quoted_message": {"type": ["object", "array", "string", "null"]},
            "raw_payload": {"type": ["object", "array", "string", "null"]},
            "source": {"type": "string"},
            "action_key": {"type": "string"},
            "business_key": {"type": "string"},
            "status": {"type": "string"},
            "action_id": {"type": "integer", "minimum": 1},
            "external_task_id": {"type": "string"},
            "todo_id": {"type": "integer", "minimum": 1},
            "duplicate_of_action_id": {"type": "integer", "minimum": 1},
            "last_error": {"type": "string"},
            "attempt_count": {"type": "integer", "minimum": 0},
            "contract_version": {"type": "string", "maxLength": 32},
            "parent_action_id": {"type": "integer", "minimum": 1},
            "parent_task_id": {"type": "string", "maxLength": 128},
            "business_scope": {"type": "string", "maxLength": 64},
            "scope_key": {
                "type": "string",
                "enum": ["asset_inventory_permissions", "asset_tags", "online_process"],
            },
            "assignment_audit": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "rule_version", "source", "rule_id", "primary_owner", "collaborators",
                    "awareness_only", "awareness_names", "manual_assignment_required", "assignment_basis",
                    "handover_evidence_ids", "handover_source",
                ],
                "properties": {
                    "rule_version": {"type": "string", "maxLength": 80},
                    "source": {"type": "string", "maxLength": 160},
                    "rule_id": {"type": ["string", "null"], "maxLength": 8},
                    "primary_owner": {"type": ["string", "null"], "maxLength": 64},
                    "collaborators": {"type": "array", "maxItems": 6, "items": {"type": "string", "maxLength": 64}},
                    "awareness_only": {"type": "boolean"},
                    "awareness_names": {"type": "array", "maxItems": 6, "items": {"type": "string", "maxLength": 64}},
                    "manual_assignment_required": {"type": "boolean"},
                    "assignment_basis": {"type": "string", "maxLength": 80},
                    "handover_evidence_ids": {"type": "array", "maxItems": 8, "items": {"type": "string", "maxLength": 128}},
                    "handover_source": {"type": ["string", "null"], "maxLength": 64},
                    "handover_status": {"type": "string", "maxLength": 128},
                    "hygiene_review": {"type": "string", "maxLength": 128},
                    "database_conflict": {
                        "type": ["object", "null"],
                        "additionalProperties": False,
                        "required": ["configured_owner", "selected_owner"],
                        "properties": {
                            "configured_owner": {"type": "string", "maxLength": 64},
                            "selected_owner": {"type": "string", "maxLength": 64},
                        },
                    },
                    "conflict_resolution": {"type": "string", "maxLength": 128},
                    "legacy_configuration_recheck_required": {"type": "boolean"},
                },
            },
            "last_msg_time": {"type": "string"},
            "last_msg_id": {"type": "string"},
            "duty_id": {"type": "integer", "minimum": 1},
            "area_name": {"type": "string"},
            "staff_name": {"type": "string"},
            "cycle_start": {"type": "string"},
            "cycle_end": {"type": "string"},
            "target_action_id": {"type": "integer", "minimum": 1},
            "expected_canonical_action_id": {"type": "integer", "minimum": 1},
            "expected_target": _REVIEW_ACTION_SNAPSHOT_SCHEMA,
            "expected_canonical": _REVIEW_ACTION_SNAPSHOT_SCHEMA,
        },
        "allOf": [
            {
                "if": {
                    "properties": {"operation": {"const": "detach_review_duplicate_action"}},
                    "required": ["operation"],
                },
                "then": {
                    "required": [
                        "target_action_id", "expected_canonical_action_id",
                        "expected_target", "expected_canonical",
                    ],
                },
            },
        ],
    },
    "annotations": {
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": False,
    },
}


writer = WorkWriter()


def _write(message: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(message, ensure_ascii=False, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def _error(request_id: Any, code: int, message: str) -> None:
    _write({"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}})


def _tool_result(payload: dict[str, Any], *, is_error: bool = False) -> dict[str, Any]:
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return {"content": [{"type": "text", "text": text}], "structuredContent": payload, "isError": is_error}


def handle(request: dict[str, Any]) -> None:
    method = request.get("method")
    request_id = request.get("id")
    params = request.get("params") or {}
    if method == "initialize":
        requested = params.get("protocolVersion")
        version = requested if isinstance(requested, str) and requested else "2024-11-05"
        _write({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": version,
                "capabilities": {"tools": {}},
                "serverInfo": SERVER_INFO,
                "instructions": "Use work_write only for allowlisted transactional work-ledger writes.",
            },
        })
        return
    if method in {"notifications/initialized", "notifications/cancelled", "notifications/progress"}:
        return
    if method == "ping":
        _write({"jsonrpc": "2.0", "id": request_id, "result": {}})
        return
    if method == "tools/list":
        _write({"jsonrpc": "2.0", "id": request_id, "result": {"tools": [TOOL_SCHEMA]}})
        return
    if method == "tools/call":
        if params.get("name") != "work_write":
            _error(request_id, -32602, "unknown_tool")
            return
        arguments = params.get("arguments") or {}
        if not isinstance(arguments, dict):
            _error(request_id, -32602, "arguments_must_be_object")
            return
        try:
            result = writer.execute(arguments)
            _write({"jsonrpc": "2.0", "id": request_id, "result": _tool_result(result)})
        except InvalidWriteRequest as exc:
            _write({"jsonrpc": "2.0", "id": request_id, "result": _tool_result({"error": str(exc)}, is_error=True)})
        except WriteError as exc:
            _write({"jsonrpc": "2.0", "id": request_id, "result": _tool_result({"error": str(exc)}, is_error=True)})
        except Exception:
            _write({"jsonrpc": "2.0", "id": request_id, "result": _tool_result({"error": "writer_unavailable"}, is_error=True)})
        return
    if request_id is not None:
        _error(request_id, -32601, f"method_not_found:{method}")


def main() -> int:
    for raw in sys.stdin:
        if not raw.strip():
            continue
        try:
            request = json.loads(raw)
        except json.JSONDecodeError:
            _error(None, -32700, "parse_error")
            continue
        if isinstance(request, dict):
            handle(request)
        else:
            _error(None, -32600, "invalid_request")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
