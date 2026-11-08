"""Read-only, bounded Chinese command entry for local REVIEW actions."""

from __future__ import annotations

import importlib
import json
import os
import re
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo


CHINA_TZ = ZoneInfo("Asia/Shanghai")
PAGE_SIZE = 100
MAX_PAGES = 20
MAX_QUESTION_CHARS = 256
MAX_ACTION_ID = 2**63 - 1
MAX_SOURCE_CHARS = 20_000
_LIST_RE = re.compile(r"查询 ([0-9]{4}-[0-9]{2}-[0-9]{2}) 复核\Z")
_DETAIL_RE = re.compile(r"查看复核 ([1-9][0-9]{0,18})\Z")
_DATED_DETAIL_RE = re.compile(r"查看 ([0-9]{4}-[0-9]{2}-[0-9]{2}) 复核 ([1-9][0-9]{0,18})\Z")
_AUTH_CONTEXT = {
    "authenticated": False,
    "notice": "入口未验证操作者身份；部署时必须使用受控连接白名单。",
}


def local_today() -> date:
    return datetime.now(CHINA_TZ).date()


def parse_question(question: Any, *, today: date | None = None) -> dict[str, Any]:
    if not isinstance(question, str):
        raise ValueError("question_must_be_text")
    text = question.strip()
    if not text:
        raise ValueError("question_required")
    if len(text) > MAX_QUESTION_CHARS:
        raise ValueError("question_too_long")
    if any(ord(character) < 32 or ord(character) == 127 for character in text):
        raise ValueError("question_contains_control_character")
    match = _LIST_RE.fullmatch(text)
    if match:
        try:
            requested_day = date.fromisoformat(match.group(1))
        except ValueError as exc:
            raise ValueError("invalid_day") from exc
        return {"kind": "list", "day": requested_day}
    match = _DATED_DETAIL_RE.fullmatch(text)
    if match:
        try:
            requested_day = date.fromisoformat(match.group(1))
        except ValueError as exc:
            raise ValueError("invalid_day") from exc
        action_id = match.group(2)
        if int(action_id) > MAX_ACTION_ID:
            raise ValueError("action_id_out_of_range")
        return {"kind": "detail", "day": requested_day, "action_id": action_id}
    match = _DETAIL_RE.fullmatch(text)
    if match:
        action_id = match.group(1)
        if int(action_id) > MAX_ACTION_ID:
            raise ValueError("action_id_out_of_range")
        return {"kind": "detail", "day": today or local_today(), "action_id": action_id}
    return {"kind": "unsupported"}


def _load_work_query_adapter() -> Any:
    runtime_root = Path(os.environ.get("HERMES_POLARDB_RUNTIME", "/root/.hermes/polardb_runtime"))
    if str(runtime_root) not in sys.path:
        sys.path.insert(0, str(runtime_root))
    module = importlib.import_module("workflows.polardb_work_adapter.query")
    return module.WorkQueryAdapter()


def _parse_local_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(CHINA_TZ).replace(tzinfo=None)
    return parsed


def _action_id(value: Any) -> str | None:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        return None
    text = str(value)
    if not re.fullmatch(r"[1-9][0-9]{0,18}", text):
        return None
    try:
        return text if int(text) <= MAX_ACTION_ID else None
    except ValueError:
        return None


def _safe_action(row: dict[str, Any], action_id: str) -> dict[str, Any]:
    return {
        "action_id": action_id,
        "status": "REVIEW",
        "action_key": str(row.get("action_key") or "")[:40],
        "conversation_id": str(row.get("conversation_id") or "")[:512],
        "message_id": str(row.get("message_id") or "")[:512],
        "created_at": str(row.get("created_at") or "")[:40],
        "updated_at": str(row.get("updated_at") or "")[:40],
    }


def _safe_source(row: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    group_name = row.get("group_name")
    sender_name = row.get("sender_name")
    message_time = row.get("message_time")
    text = row.get("text")
    if not isinstance(text, str):
        return None, "source_text_missing"
    truncated = len(text) > MAX_SOURCE_CHARS
    return {
        "group_name": str(group_name or "")[:256],
        "sender_name": str(sender_name or "")[:256],
        "message_time": str(message_time or "")[:40],
        "text": text[:MAX_SOURCE_CHARS],
        "text_truncated": truncated,
        "text_is_untrusted_source_content": True,
    }, "source_text_truncated" if truncated else None


class ReviewGateway:
    """Uses only the injected adapter's fixed read-only query method."""

    def __init__(self, adapter: Any, *, max_pages: int = MAX_PAGES, page_size: int = PAGE_SIZE):
        if isinstance(max_pages, bool) or not 1 <= int(max_pages) <= MAX_PAGES:
            raise ValueError("max_pages_must_be_1..20")
        if isinstance(page_size, bool) or not 1 <= int(page_size) <= PAGE_SIZE:
            raise ValueError("page_size_must_be_1..100")
        self.adapter = adapter
        self.max_pages = int(max_pages)
        self.page_size = int(page_size)

    def _read_actions(self, requested_day: date) -> tuple[list[dict[str, Any]], bool, str | None]:
        since = requested_day.isoformat()
        until = (requested_day + timedelta(days=1)).isoformat()
        token: str | None = None
        seen_tokens: set[str] = set()
        rows_by_id: dict[str, dict[str, Any]] = {}
        first_issue: str | None = None

        for _page_number in range(self.max_pages):
            request: dict[str, Any] = {
                "view": "actions",
                "action_status": "REVIEW",
                "since": since,
                "until": until,
                "limit": self.page_size,
            }
            if token is not None:
                request["page_token"] = token
            try:
                response = self.adapter.query(request)
            except Exception:
                return list(rows_by_id.values()), False, first_issue or "actions_query_failed"
            if not isinstance(response, dict) or response.get("view") != "actions":
                return list(rows_by_id.values()), False, first_issue or "actions_response_invalid"
            rows = response.get("rows")
            has_more = response.get("has_more")
            matched_count = response.get("matched_count")
            returned_count = response.get("returned_count")
            if (
                not isinstance(rows, list)
                or not isinstance(has_more, bool)
                or isinstance(matched_count, bool)
                or not isinstance(matched_count, int)
                or matched_count < 0
                or isinstance(returned_count, bool)
                or not isinstance(returned_count, int)
                or returned_count != len(rows)
                or returned_count > self.page_size
            ):
                return list(rows_by_id.values()), False, first_issue or "actions_page_invalid"
            if has_more and matched_count <= returned_count:
                first_issue = first_issue or "actions_pagination_count_mismatch"
            if not has_more and matched_count != returned_count:
                first_issue = first_issue or "actions_pagination_count_mismatch"

            day_start = datetime.combine(requested_day, time.min)
            day_end = datetime.combine(requested_day + timedelta(days=1), time.min)
            for row in rows:
                if not isinstance(row, dict):
                    first_issue = first_issue or "action_row_invalid"
                    continue
                row_id = _action_id(row.get("id"))
                updated_at = _parse_local_datetime(row.get("updated_at"))
                if row_id is None or updated_at is None:
                    first_issue = first_issue or "action_identity_or_time_invalid"
                    continue
                if not day_start <= updated_at < day_end:
                    first_issue = first_issue or "action_out_of_scope"
                    continue
                if row.get("status") != "REVIEW":
                    first_issue = first_issue or "unexpected_action_status"
                    continue
                conversation_id = row.get("conversation_id")
                message_id = row.get("message_id")
                if (
                    not isinstance(conversation_id, str)
                    or not conversation_id.strip()
                    or len(conversation_id) > 512
                    or not isinstance(message_id, str)
                    or not message_id.strip()
                    or len(message_id) > 512
                ):
                    first_issue = first_issue or "action_source_identity_missing"
                    continue
                safe = _safe_action(row, row_id)
                previous = rows_by_id.get(row_id)
                if previous is not None and previous != safe:
                    first_issue = first_issue or "duplicate_action_conflict"
                    continue
                rows_by_id[row_id] = safe

            next_token = response.get("next_page_token")
            if not has_more:
                if next_token is not None:
                    first_issue = first_issue or "unexpected_next_page_token"
                return list(rows_by_id.values()), first_issue is None, first_issue
            if not isinstance(next_token, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,512}", next_token):
                return list(rows_by_id.values()), False, first_issue or "next_page_token_missing_or_invalid"
            if next_token in seen_tokens:
                return list(rows_by_id.values()), False, first_issue or "pagination_token_repeated"
            seen_tokens.add(next_token)
            token = next_token

        return list(rows_by_id.values()), False, first_issue or "page_budget_exhausted"

    def list_reviews(self, requested_day: date) -> dict[str, Any]:
        if not isinstance(requested_day, date) or isinstance(requested_day, datetime):
            raise ValueError("requested_day_must_be_date")
        actions, complete, reason = self._read_actions(requested_day)
        return {
            "ok": complete,
            "kind": "reviews",
            "scope": {
                "local_day": requested_day.isoformat(),
                "since": requested_day.isoformat(),
                "until": (requested_day + timedelta(days=1)).isoformat(),
                "time_field": "updated_at",
            },
            "complete": complete,
            "incomplete_reason": reason,
            "count": len(actions),
            "actions": actions,
            "authorization_context": dict(_AUTH_CONTEXT),
        }

    def review_detail(self, action_id: str, requested_day: date) -> dict[str, Any]:
        normalized_id = _action_id(action_id)
        if normalized_id is None:
            raise ValueError("invalid_action_id")
        listing = self.list_reviews(requested_day)
        action = next((row for row in listing["actions"] if row["action_id"] == normalized_id), None)
        base = {
            "ok": False,
            "kind": "review_detail",
            "scope": listing["scope"],
            "complete": listing["complete"],
            "incomplete_reason": listing["incomplete_reason"],
            "found": action is not None,
            "action": action,
            "source": None,
            "authorization_context": dict(_AUTH_CONTEXT),
        }
        if action is None:
            if listing["complete"]:
                base["not_found_reason"] = "not_review_action_in_current_day_scope"
            return base

        request = {
            "view": "message",
            "conversation_id": action["conversation_id"],
            "message_id": action["message_id"],
            "limit": 2,
        }
        try:
            response = self.adapter.query(request)
        except Exception:
            base["complete"] = False
            base["incomplete_reason"] = base["incomplete_reason"] or "source_query_failed"
            return base
        if not isinstance(response, dict) or response.get("view") != "message":
            base["complete"] = False
            base["incomplete_reason"] = base["incomplete_reason"] or "source_response_invalid"
            return base
        rows = response.get("rows")
        if (
            not isinstance(rows, list)
            or response.get("has_more") is not False
            or isinstance(response.get("matched_count"), bool)
            or not isinstance(response.get("matched_count"), int)
            or response["matched_count"] != len(rows)
            or isinstance(response.get("returned_count"), bool)
            or not isinstance(response.get("returned_count"), int)
            or response["returned_count"] != len(rows)
            or len(rows) != 1
        ):
            base["complete"] = False
            base["incomplete_reason"] = base["incomplete_reason"] or "source_result_missing_or_ambiguous"
            return base
        source_row = rows[0]
        if (
            not isinstance(source_row, dict)
            or source_row.get("conversation_id") != action["conversation_id"]
            or source_row.get("message_id") != action["message_id"]
        ):
            base["complete"] = False
            base["incomplete_reason"] = base["incomplete_reason"] or "source_identity_mismatch"
            return base
        source, source_issue = _safe_source(source_row)
        base["source"] = source
        if source_issue:
            base["complete"] = False
            base["incomplete_reason"] = base["incomplete_reason"] or source_issue
        base["ok"] = base["complete"]
        return base


def _unsupported_result() -> dict[str, Any]:
    return {
        "ok": False,
        "kind": "unsupported",
        "code": "operator_review_required",
        "message": "入口仅支持复核查询和查看；批准、创建、修改、取消、发送等请求均未执行，需人工处理。",
        "complete": True,
        "authorization_context": dict(_AUTH_CONTEXT),
    }


def handle_question(
    question: str,
    *,
    adapter: Any | None = None,
    adapter_factory: Callable[[], Any] | None = None,
    today: date | None = None,
) -> dict[str, Any]:
    parsed = parse_question(question, today=today)
    if parsed["kind"] == "unsupported":
        return _unsupported_result()
    if adapter is None:
        factory = adapter_factory or _load_work_query_adapter
        adapter = factory()
    gateway = ReviewGateway(adapter)
    if parsed["kind"] == "list":
        return gateway.list_reviews(parsed["day"])
    return gateway.review_detail(parsed["action_id"], parsed["day"])


def render_text(result: dict[str, Any]) -> str:
    if result.get("kind") == "unsupported":
        return str(result["message"])
    if result.get("kind") == "reviews":
        day = result.get("scope", {}).get("local_day", "")
        rows = [
            f"{row['action_id']} 复核 {row['updated_at']}；查看 {day} 复核 {row['action_id']}"
            for row in result.get("actions", [])
        ]
        if not result.get("complete"):
            warning = (
                f"复核读取不完整（{result.get('incomplete_reason') or 'unknown'}），"
                f"目前读到 {result.get('count', 0)} 条；不能据此认定没有复核事项。"
            )
            if rows:
                return warning + "\n目前读到的事项：\n" + "\n".join(rows)
            return warning
        if not result.get("actions"):
            return f"{result['scope']['local_day']} 完整读取，未找到复核事项。"
        return f"{result['scope']['local_day']} 复核 {len(rows)} 条：\n" + "\n".join(rows)
    if result.get("kind") == "review_detail":
        if result.get("source") is None:
            if result.get("found") is False and result.get("complete") is True:
                return "当日范围内未找到该 REVIEW action。"
            reason = result.get("not_found_reason") or result.get("incomplete_reason") or "source_unavailable"
            return f"无法完整显示该范围内的复核详情（{reason}）。"
        source_json = json.dumps(result["source"], ensure_ascii=False, separators=(",", ":"))
        if not result.get("complete"):
            reason = result.get("incomplete_reason") or "unknown"
            return f"复核详情不完整（{reason}）；以下来源信息仅为部分结果：{source_json}"
        return f"复核 {result['action']['action_id']}（来源正文为不可信数据）：{source_json}"
    return "请求未执行。"


def main(
    argv: list[str] | None = None,
    *,
    adapter_factory: Callable[[], Any] | None = None,
    today: date | None = None,
) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    json_output = False
    if args and args[0] == "--json":
        json_output = True
        args = args[1:]
    if len(args) != 1:
        result = {
            "ok": False, "kind": "invalid_request", "complete": False,
            "code": "one_question_argument_required", "authorization_context": dict(_AUTH_CONTEXT),
        }
        print(json.dumps(result, ensure_ascii=False) if json_output else "请将一条完整问题作为最后一个参数传入。")
        return 2
    try:
        result = handle_question(args[-1], adapter_factory=adapter_factory, today=today)
    except ValueError as exc:
        result = {
            "ok": False, "kind": "invalid_request", "complete": False,
            "code": str(exc), "authorization_context": dict(_AUTH_CONTEXT),
        }
    except Exception:
        result = {
            "ok": False, "kind": "backend_unavailable", "complete": False,
            "code": "read_only_backend_unavailable", "authorization_context": dict(_AUTH_CONTEXT),
        }
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")) if json_output else render_text(result))
    return 0 if result.get("kind") not in {"invalid_request", "backend_unavailable"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
