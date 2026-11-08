"""Fixed, parameterized read views over PolarDB's agentic work database."""
from __future__ import annotations

import base64
import binascii
import json
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any

from .db import connect, db_config


_ALLOWED_VIEWS = frozenset({
    "open_todos", "todos", "todo", "messages", "message", "actions", "runtime_config", "scan_state",
})
_TODO_STATUSES = frozenset({"待办", "已完成", "已撤销"})
_ACTION_STATUSES = frozenset({"CLAIMED", "EXTERNAL_CREATED", "FAILED", "SUCCESS", "IGNORED", "REVIEW"})
_MAX_LIMIT = 100
_MAX_TEXT = 200


def _jsonable(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


def _text(value: Any, field: str, *, required: bool = False, max_length: int = _MAX_TEXT) -> str:
    text = str(value or "").strip()
    if required and not text:
        raise ValueError(f"missing:{field}")
    if len(text) > max_length:
        raise ValueError(f"too_long:{field}")
    return text


def _limit(value: Any) -> int:
    if value in (None, ""):
        return 50
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("limit_must_be_integer") from exc
    if not 1 <= result <= _MAX_LIMIT:
        raise ValueError(f"limit_must_be_1..{_MAX_LIMIT}")
    return result


def _parse_bound(value: Any, field: str) -> datetime | None:
    text = _text(value, field, max_length=32)
    if not text:
        return None
    try:
        if len(text) == 10:
            return datetime.combine(date.fromisoformat(text), time.min)
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid:{field}") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _encode_cursor(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_cursor(value: Any) -> dict[str, Any] | None:
    text = _text(value, "page_token", max_length=512)
    if not text:
        return None
    try:
        padded = text + "=" * (-len(text) % 4)
        decoded = json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid:page_token") from exc
    if not isinstance(decoded, dict) or decoded.get("kind") not in {"todos", "messages", "actions", "scan_state"}:
        raise ValueError("invalid:page_token")
    return decoded


def _cursor_id(page: dict[str, Any]) -> int:
    raw = page.get("id")
    try:
        result = int(str(raw))
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid:page_token.id") from exc
    if result <= 0:
        raise ValueError("invalid:page_token.id")
    return result


def _cursor_time(page: dict[str, Any]) -> datetime:
    parsed = _parse_bound(page.get("updated_at") or page.get("message_time"), "page_token.time")
    if parsed is None:
        raise ValueError("invalid:page_token.time")
    return parsed


def _base_result(view: str) -> dict[str, Any]:
    database = db_config()["database"]
    return {
        "source": "local_mysql" if database == "agentic_local" else "polardb",
        "database": database,
        "view": view,
        "snapshot_at": datetime.now(timezone.utc).isoformat(),
        "rows": [],
        "matched_count": 0,
        "returned_count": 0,
        "has_more": False,
        "next_page_token": None,
    }


def _count(cur, table: str, where: str, params: list[Any]) -> int:
    cur.execute(f"SELECT COUNT(*) AS matched_count FROM {table} WHERE {where}", tuple(params))
    row = cur.fetchone() or {}
    return int(row.get("matched_count") or 0)


class WorkQueryAdapter:
    """Read-only semantic adapter; no arbitrary SQL reaches this class."""

    def query(self, request: dict[str, Any] | None = None) -> dict[str, Any]:
        request = dict(request or {})
        unknown = set(request) - {
            "view", "todo_id", "conversation_id", "message_id", "group_name", "sender_name",
            "external_task_id", "status", "action_status", "since", "until", "query", "limit", "page_token",
        }
        if unknown:
            raise ValueError("unknown_parameters:" + ",".join(sorted(unknown)))
        view = _text(request.get("view") or "open_todos", "view", required=True, max_length=32)
        if view not in _ALLOWED_VIEWS:
            raise ValueError("invalid:view")
        limit = _limit(request.get("limit"))
        page = _decode_cursor(request.get("page_token"))
        query_text = _text(request.get("query"), "query")
        since = _parse_bound(request.get("since"), "since")
        until = _parse_bound(request.get("until"), "until")
        if since and until and since >= until:
            raise ValueError("since_must_be_before_until")

        if view == "runtime_config":
            return self._runtime_config()
        if view in {"open_todos", "todos", "todo"}:
            return self._todos(view, request, query_text, limit, page)
        if view in {"messages", "message"}:
            return self._messages(view, request, query_text, since, until, limit, page)
        if view == "actions":
            return self._actions(request, since, until, limit, page)
        return self._scan_state(request, limit, page)

    def _runtime_config(self) -> dict[str, Any]:
        result = _base_result("runtime_config")
        conn = connect()
        try:
            with conn.cursor() as cur:
                queries = (
                    ("config_group", "SELECT id, group_name, conversation_id, purpose FROM config_group"),
                    ("config_staff", "SELECT * FROM config_staff"),
                    ("config_duty_rotation", "SELECT id, seq, staff_name, anchor_date, active FROM config_duty_rotation WHERE active = 1"),
                    ("config_area_duty", "SELECT id, area_name, staff_name, cycle_start, cycle_end, reviewed FROM config_area_duty"),
                    ("config_calendar", "SELECT * FROM config_calendar"),
                )
                config: dict[str, list[dict[str, Any]]] = {}
                for name, sql in queries:
                    cur.execute(sql)
                    config[name] = [_jsonable(row) for row in cur.fetchall()]
                result["config"] = config
                result["matched_count"] = sum(len(rows) for rows in config.values())
                result["returned_count"] = result["matched_count"]
                return result
        finally:
            conn.close()

    def _todos(self, view: str, request: dict[str, Any], query_text: str, limit: int, page: dict[str, Any] | None) -> dict[str, Any]:
        result = _base_result(view)
        where: list[str] = []
        params: list[Any] = []
        todo_id = request.get("todo_id")
        if view == "todo" and todo_id in (None, ""):
            raise ValueError("missing:todo_id")
        if todo_id not in (None, ""):
            try:
                todo_id = int(todo_id)
            except (TypeError, ValueError) as exc:
                raise ValueError("todo_id_must_be_integer") from exc
            if todo_id <= 0:
                raise ValueError("todo_id_must_be_positive")
            where.append("id=%s")
            params.append(todo_id)
        status = _text(request.get("status"), "status", max_length=32)
        if view == "open_todos":
            status = "待办"
        if status:
            if status not in _TODO_STATUSES:
                raise ValueError("invalid:status")
            where.append("status=%s")
            params.append(status)
        if query_text:
            where.append("(INSTR(title,%s)>0 OR INSTR(detail,%s)>0)")
            params.extend([query_text, query_text])
        if page:
            if page.get("kind") != "todos":
                raise ValueError("page_token_view_mismatch")
            page_time = _cursor_time(page)
            page_id = _cursor_id(page)
            where.append("(updated_at < %s OR (updated_at = %s AND id < %s))")
            params.extend([page_time, page_time, page_id])
        clause = " AND ".join(where) or "1=1"
        conn = connect()
        try:
            with conn.cursor() as cur:
                result["matched_count"] = _count(cur, "todo", clause, params.copy())
                cur.execute(
                    f"SELECT id, title, detail, due_date, status, created_at, updated_at, task_key FROM todo WHERE {clause} ORDER BY updated_at DESC, id DESC LIMIT %s",
                    tuple(params + [limit + 1]),
                )
                rows = [_jsonable(row) for row in cur.fetchall()]
                result["has_more"] = len(rows) > limit
                result["rows"] = rows[:limit]
                if result["has_more"] and result["rows"]:
                    last = result["rows"][-1]
                    result["next_page_token"] = _encode_cursor({"kind": "todos", "updated_at": last["updated_at"], "id": last["id"]})
                result["returned_count"] = len(result["rows"])
                return result
        finally:
            conn.close()

    def _messages(
        self, view: str, request: dict[str, Any], query_text: str, since: datetime | None, until: datetime | None,
        limit: int, page: dict[str, Any] | None,
    ) -> dict[str, Any]:
        result = _base_result(view)
        where: list[str] = []
        params: list[Any] = []
        conversation_id = _text(request.get("conversation_id"), "conversation_id", max_length=256)
        message_id = _text(request.get("message_id"), "message_id", max_length=512)
        if view == "message" and (not conversation_id or not message_id):
            raise ValueError("message_requires_conversation_id_and_message_id")
        if conversation_id:
            where.append("conversation_id=%s")
            params.append(conversation_id)
        if message_id:
            where.append("message_id=%s")
            params.append(message_id)
        group_name = _text(request.get("group_name"), "group_name", max_length=256)
        if group_name:
            where.append("group_name=%s")
            params.append(group_name)
        sender_name = _text(request.get("sender_name"), "sender_name", max_length=256)
        if sender_name:
            where.append("sender_name=%s")
            params.append(sender_name)
        if since:
            where.append("message_time >= %s")
            params.append(since)
        if until:
            where.append("message_time < %s")
            params.append(until)
        if query_text:
            where.append("INSTR(`text`,%s)>0")
            params.append(query_text)
        if page:
            if page.get("kind") != "messages":
                raise ValueError("page_token_view_mismatch")
            page_time = _cursor_time(page)
            page_id = _cursor_id(page)
            where.append("(message_time > %s OR (message_time = %s AND id > %s))")
            params.extend([page_time, page_time, page_id])
        clause = " AND ".join(where) or "1=1"
        conn = connect()
        try:
            with conn.cursor() as cur:
                result["matched_count"] = _count(cur, "message_inbox", clause, params.copy())
                cur.execute(
                    f"SELECT id, conversation_id, message_id, group_name, sender_id, sender_name, message_time, `text`, msg_type, resource_refs, quoted_message, ingested_at, source FROM message_inbox WHERE {clause} ORDER BY message_time ASC, id ASC LIMIT %s",
                    tuple(params + [limit + 1]),
                )
                rows = [_jsonable(row) for row in cur.fetchall()]
                result["has_more"] = len(rows) > limit
                result["rows"] = rows[:limit]
                if result["has_more"] and result["rows"]:
                    last = result["rows"][-1]
                    result["next_page_token"] = _encode_cursor({"kind": "messages", "message_time": last["message_time"], "id": last["id"]})
                result["returned_count"] = len(result["rows"])
                return result
        finally:
            conn.close()

    def _actions(
        self, request: dict[str, Any], since: datetime | None, until: datetime | None, limit: int, page: dict[str, Any] | None,
    ) -> dict[str, Any]:
        result = _base_result("actions")
        where: list[str] = []
        params: list[Any] = []
        for field in ("conversation_id", "message_id", "external_task_id"):
            value = _text(request.get(field), field, max_length=512)
            if value:
                where.append(f"{field}=%s")
                params.append(value)
        action_status = _text(request.get("action_status"), "action_status", max_length=32)
        if action_status:
            if action_status not in _ACTION_STATUSES:
                raise ValueError("invalid:action_status")
            where.append("status=%s")
            params.append(action_status)
        if since:
            where.append("updated_at >= %s")
            params.append(since)
        if until:
            where.append("updated_at < %s")
            params.append(until)
        if page:
            if page.get("kind") != "actions":
                raise ValueError("page_token_view_mismatch")
            page_time = _cursor_time(page)
            page_id = _cursor_id(page)
            where.append("(updated_at > %s OR (updated_at = %s AND id > %s))")
            params.extend([page_time, page_time, page_id])
        clause = " AND ".join(where) or "1=1"
        conn = connect()
        try:
            with conn.cursor() as cur:
                result["matched_count"] = _count(cur, "message_action", clause, params.copy())
                cur.execute(
                    f"SELECT id, id AS action_id, conversation_id, message_id, action_key, business_key, status, external_task_id, todo_id, duplicate_of_action_id, parent_action_id, rule_audit, "
                    "COALESCE(external_task_id, (SELECT canonical.external_task_id FROM message_action canonical "
                    "WHERE canonical.id=message_action.duplicate_of_action_id)) AS effective_external_task_id, "
                    "COALESCE(duplicate_of_action_id, id) AS canonical_action_id, "
                    f"attempt_count, last_error, claimed_at, created_at, updated_at FROM message_action WHERE {clause} ORDER BY updated_at ASC, id ASC LIMIT %s",
                    tuple(params + [limit + 1]),
                )
                rows = [_jsonable(row) for row in cur.fetchall()]
                result["has_more"] = len(rows) > limit
                result["rows"] = rows[:limit]
                if result["has_more"] and result["rows"]:
                    last = result["rows"][-1]
                    result["next_page_token"] = _encode_cursor({"kind": "actions", "updated_at": last["updated_at"], "id": last["id"]})
                result["returned_count"] = len(result["rows"])
                return result
        finally:
            conn.close()

    def _scan_state(self, request: dict[str, Any], limit: int, page: dict[str, Any] | None) -> dict[str, Any]:
        result = _base_result("scan_state")
        where: list[str] = []
        params: list[Any] = []
        for field in ("conversation_id", "group_name"):
            value = _text(request.get(field), field, max_length=256)
            if value:
                where.append(f"{field}=%s")
                params.append(value)
        if page:
            if page.get("kind") != "scan_state":
                raise ValueError("page_token_view_mismatch")
            page_group = _text(page.get("group_name"), "page_token.group_name", max_length=256)
            where.append("group_name > %s")
            params.append(page_group)
        clause = " AND ".join(where) or "1=1"
        conn = connect()
        try:
            with conn.cursor() as cur:
                result["matched_count"] = _count(cur, "scan_state", clause, params.copy())
                cur.execute(
                    f"SELECT group_name, conversation_id, last_msg_time, last_msg_id, updated_at FROM scan_state WHERE {clause} ORDER BY group_name ASC LIMIT %s",
                    tuple(params + [limit + 1]),
                )
                rows = [_jsonable(row) for row in cur.fetchall()]
                result["has_more"] = len(rows) > limit
                result["rows"] = rows[:limit]
                if result["has_more"] and result["rows"]:
                    result["next_page_token"] = _encode_cursor({"kind": "scan_state", "group_name": result["rows"][-1]["group_name"]})
                result["returned_count"] = len(result["rows"])
                return result
        finally:
            conn.close()


def work_query(request: dict[str, Any] | None = None) -> dict[str, Any]:
    return WorkQueryAdapter().query(request)
