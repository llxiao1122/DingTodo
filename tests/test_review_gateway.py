from __future__ import annotations

import json
import ast
import sys
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from review_gateway import ReviewGateway, handle_question, main, parse_question, render_text  # noqa: E402
import review_gateway  # noqa: E402


TODAY = date(2026, 10, 9)


def _page(view, rows=(), *, more=False, token=None, matched=None):
    return {
        "view": view,
        "rows": list(rows),
        "matched_count": len(rows) if matched is None else matched,
        "returned_count": len(rows),
        "has_more": more,
        "next_page_token": token,
    }


def _action(action_id, *, status="REVIEW", updated_at="2026-10-09 10:20:00", conversation_id="conv-1", message_id="msg-1"):
    return {
        "id": action_id,
        "conversation_id": conversation_id,
        "message_id": message_id,
        "action_key": "todo",
        "business_key": f"business-{action_id}",
        "status": status,
        "external_task_id": None,
        "todo_id": None,
        "created_at": "2026-10-09 10:10:00",
        "updated_at": updated_at,
        "last_error": "must not be exposed",
    }


class TrapQueryAdapter:
    """Only the read-only actions/message views are callable in gateway tests."""

    def __init__(self, responder):
        self.responder = responder
        self.requests = []
        self.forbidden_calls = []

    def query(self, request):
        self.requests.append(dict(request))
        if request.get("view") not in {"actions", "message"}:
            self.forbidden_calls.append(request)
            raise AssertionError("gateway attempted an unsupported view")
        return self.responder(request)

    def __getattr__(self, name):
        if name in {"execute", "write", "create", "update", "delete", "send", "advance_cursor"}:
            self.forbidden_calls.append(name)
            raise AssertionError(f"forbidden adapter side effect: {name}")
        raise AttributeError(name)


def test_query_parser_accepts_only_bounded_chinese_review_commands():
    assert parse_question("查询 2026-10-09 复核", today=TODAY) == {
        "kind": "list", "day": date(2026, 10, 9),
    }
    assert parse_question("查看复核 42", today=TODAY) == {
        "kind": "detail", "day": TODAY, "action_id": "42",
    }
    assert parse_question("查看 2026-10-08 复核 311", today=TODAY) == {
        "kind": "detail", "day": date(2026, 10, 8), "action_id": "311",
    }
    for question in (
        "查询最近复核", "查询 2026-10-09 复核; 删除全部", "查看复核 -1",
        "批准 42", "创建待办", "修改 42", "取消 42", "发送通知",
    ):
        assert parse_question(question, today=TODAY)["kind"] == "unsupported"
    with pytest.raises(ValueError):
        parse_question("查询 " + "9" * 300 + " 复核", today=TODAY)


def test_listing_uses_day_bounds_review_filter_all_pages_and_deduplicates():
    page_two = _action(2, message_id="msg-2")
    page_one = _action(1)
    pages = {
        None: _page("actions", [page_one, page_one], more=True, token="cursor-2", matched=3),
        "cursor-2": _page("actions", [page_two]),
    }
    adapter = TrapQueryAdapter(lambda request: pages[request.get("page_token")])

    result = ReviewGateway(adapter).list_reviews(date(2026, 10, 9))

    assert result["complete"] is True
    assert [row["action_id"] for row in result["actions"]] == ["1", "2"]
    assert all("last_error" not in row for row in result["actions"])
    assert [request["view"] for request in adapter.requests] == ["actions", "actions"]
    assert all(request["action_status"] == "REVIEW" for request in adapter.requests)
    assert all(request["since"] == "2026-10-09" for request in adapter.requests)
    assert all(request["until"] == "2026-10-10" for request in adapter.requests)
    assert adapter.requests[1]["page_token"] == "cursor-2"
    assert all(request["limit"] <= 100 for request in adapter.requests)
    assert adapter.forbidden_calls == []
    assert result["authorization_context"]["authenticated"] is False


def test_listing_marks_page_budget_exhaustion_incomplete_and_never_claims_empty():
    adapter = TrapQueryAdapter(
        lambda request: _page(
            "actions", [_action(int(request.get("page_token", "0").replace("c", "") or 0) + 1)],
            more=True, matched=2,
            token="c" + str(int(request.get("page_token", "c0")[1:]) + 1),
        )
    )
    gateway = ReviewGateway(adapter, max_pages=2)

    result = gateway.list_reviews(TODAY)

    assert result["complete"] is False
    assert result["incomplete_reason"] == "page_budget_exhausted"
    assert len(result["actions"]) == 2
    assert "无复核事项" not in render_text(result)


def test_bad_or_repeated_pagination_token_is_incomplete():
    adapter = TrapQueryAdapter(lambda request: _page("actions", [_action(1)], more=True, token="same", matched=2))
    result = ReviewGateway(adapter).list_reviews(TODAY)
    assert result["complete"] is False
    assert result["incomplete_reason"] == "pagination_token_repeated"


def test_missing_page_token_or_invalid_response_counts_is_incomplete():
    responses = [
        _page("actions", [], more=True, token=None),
        {"view": "actions", "rows": [], "has_more": False, "next_page_token": None},
    ]
    for response in responses:
        gateway = ReviewGateway(TrapQueryAdapter(lambda _request, response=response: response))
        result = gateway.list_reviews(TODAY)
        assert result["complete"] is False
        assert result["actions"] == []
        assert "未找到复核事项" not in render_text(result)


def test_detail_requires_review_action_in_today_scope_and_exact_message_identity():
    review = _action(42)
    message = {
        "conversation_id": "conv-1", "message_id": "msg-1", "group_name": "铁炉西工班群",
        "sender_name": "报告人", "message_time": "2026-10-09 10:00:00",
        "text": "请于10月10日前完成盘点", "resource_refs": "private refs must not be exposed",
    }

    def responder(request):
        if request["view"] == "actions":
            return _page("actions", [review])
        return _page("message", [message])

    adapter = TrapQueryAdapter(responder)
    result = ReviewGateway(adapter).review_detail("42", TODAY)

    assert result["complete"] is True
    assert result["action"]["action_id"] == "42"
    assert result["source"]["text"] == message["text"]
    assert "resource_refs" not in result["source"]
    source_request = adapter.requests[-1]
    assert source_request == {
        "view": "message", "conversation_id": "conv-1", "message_id": "msg-1", "limit": 2,
    }
    assert adapter.forbidden_calls == []


def test_explicit_dated_detail_opens_yesterday_row_even_when_today_is_different():
    requested_day = date(2026, 10, 8)
    action = _action(
        311, updated_at="2026-10-08 23:40:00", conversation_id="yesterday-conv",
        message_id="yesterday-msg",
    )
    message = {
        "conversation_id": "yesterday-conv", "message_id": "yesterday-msg",
        "group_name": "工班群", "sender_name": "报告人", "message_time": "2026-10-08 23:35:00",
        "text": "昨日来源正文",
    }

    def responder(request):
        if request["view"] == "actions":
            return _page("actions", [action])
        return _page("message", [message])

    adapter = TrapQueryAdapter(responder)
    result = handle_question(
        "查看 2026-10-08 复核 311", adapter=adapter, today=TODAY,
    )

    assert result["complete"] is True
    assert result["action"]["action_id"] == "311"
    assert result["source"]["text"] == "昨日来源正文"
    action_request = adapter.requests[0]
    assert action_request["since"] == "2026-10-08"
    assert action_request["until"] == "2026-10-09"
    assert adapter.requests[1] == {
        "view": "message", "conversation_id": "yesterday-conv",
        "message_id": "yesterday-msg", "limit": 2,
    }


def test_dated_detail_rejects_wrong_day_or_action_id_without_source_read():
    for question, row in (
        ("查看 2026-10-08 复核 311", _action(311, updated_at="2026-10-09 00:01:00")),
        ("查看 2026-10-08 复核 311", _action(312, updated_at="2026-10-08 12:00:00")),
        ("查看 2026-10-08 复核 311", _action(311, status="SUCCESS", updated_at="2026-10-08 12:00:00")),
    ):
        adapter = TrapQueryAdapter(lambda request, row=row: _page("actions", [row]))
        result = handle_question(question, adapter=adapter, today=TODAY)
        assert result["source"] is None
        assert all(request["view"] == "actions" for request in adapter.requests)


def test_listing_hints_use_dated_detail_command_and_incomplete_detail_renders_as_incomplete():
    action = _action(311, updated_at="2026-10-08 23:40:00")
    list_adapter = TrapQueryAdapter(lambda _request: _page("actions", [action]))
    listing_text = render_text(ReviewGateway(list_adapter).list_reviews(date(2026, 10, 8)))
    assert "查看 2026-10-08 复核 311" in listing_text

    message = {
        "conversation_id": "conv-1", "message_id": "msg-1", "group_name": "工班群",
        "sender_name": "报告人", "message_time": "2026-10-08 12:00:00", "text": "部分来源可用",
    }

    def responder(request):
        if request["view"] == "actions":
            return _page("actions", [action], matched=2)
        return _page("message", [message])

    detail = ReviewGateway(TrapQueryAdapter(responder)).review_detail("311", date(2026, 10, 8))
    assert detail["source"]["text"] == "部分来源可用"
    assert detail["complete"] is False
    rendered = render_text(detail)
    assert "不完整" in rendered
    assert "部分来源可用" in rendered


def test_detail_refuses_non_review_or_out_of_scope_action_without_source_read():
    for row in (
        _action(42, status="SUCCESS"),
        _action(42, updated_at="2026-10-08 23:59:59"),
    ):
        adapter = TrapQueryAdapter(lambda _request, row=row: _page("actions", [row]))
        result = ReviewGateway(adapter).review_detail("42", TODAY)
        assert result["complete"] is False
        assert result["source"] is None
        assert all(request["view"] == "actions" for request in adapter.requests)


def test_exact_message_missing_or_ambiguous_is_incomplete():
    action = _action(42)
    for rows in ([], [
        {"conversation_id": "conv-1", "message_id": "different", "text": "wrong"},
    ], [
        {"conversation_id": "conv-1", "message_id": "msg-1", "text": "one"},
        {"conversation_id": "conv-1", "message_id": "msg-1", "text": "two"},
    ]):
        def responder(request, rows=rows):
            return _page(request["view"], [action] if request["view"] == "actions" else rows)
        result = ReviewGateway(TrapQueryAdapter(responder)).review_detail("42", TODAY)
        assert result["complete"] is False
        assert result["source"] is None


def test_question_frontend_never_loads_backend_for_unsupported_mutation():
    calls = []

    def forbidden_factory():
        calls.append("loaded")
        raise AssertionError("unsupported command must not load any adapter")

    assert handle_question("批准复核 42", adapter_factory=forbidden_factory, today=TODAY)["kind"] == "unsupported"
    assert calls == []


def test_cli_consumes_last_argument_and_outputs_json_from_read_only_backend(capsys):
    adapter = TrapQueryAdapter(lambda _request: _page("actions", []))

    code = main(["--json", "查询 2026-10-09 复核"], adapter_factory=lambda: adapter, today=TODAY)
    captured = capsys.readouterr()

    assert code == 0
    result = json.loads(captured.out)
    assert result["complete"] is True
    assert result["actions"] == []
    assert result["authorization_context"]["authenticated"] is False


def test_gateway_module_has_no_model_writer_or_dws_dependencies():
    source = Path(review_gateway.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.append(node.module or "")

    assert all(not any(word in name.lower() for word in ("llm", "model", "writer", "dws")) for name in imports)
    assert "WorkWriter" not in source
    assert "DwsClient" not in source
    assert "subprocess" not in imports
    assert not hasattr(review_gateway, "WorkWriter")
