from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from event_worker import EventProcessor  # noqa: E402
from jev_v2 import JevV2Resolver  # noqa: E402


class FakeWriter:
    def __init__(self):
        self.requests = []

    def execute(self, request):
        self.requests.append(dict(request))
        if request["operation"] == "claim_action":
            return {"created": True, "action_id": 19}
        return {"ok": True}


class FakeDws:
    def __init__(self):
        self.search_calls = 0
        self.create_calls = 0

    def resolve_exact(self, names):
        return {name: "LIVE-" + name for name in names}

    def search_todos(self, _query):
        self.search_calls += 1
        return []

    def create_todo(self, _plan, priority=20):
        self.create_calls += 1
        return {"task_id": "synthetic-task"}


def _processor(dws, writer, resolver):
    runtime = {
        "config_group": [{
            "group_name": "铁炉西工班群", "conversation_id": "synthetic-conversation", "purpose": "todo_scan",
        }],
        "config_staff": [[1, "李林骁", "STALE-ID", "leader"]],
        "active_routing_loaded": True,
        "responsibility_rules": {},
    }
    return EventProcessor(
        runtime,
        {"name": "李林骁", "user_id": "SELF"},
        mode="production",
        write_ledger=True,
        allow_external_writes=True,
        llm_resolver=resolver,
        dws=dws,
        writer=writer,
    )


def test_unreadable_image_only_source_stays_review_without_model_or_dws_write():
    calls = []
    resolver = JevV2Resolver(new_policy=True, transport=lambda *args, **kwargs: calls.append((args, kwargs)) or {})
    writer, dws = FakeWriter(), FakeDws()
    processor = _processor(dws, writer, resolver)

    plans = processor.process_batch([{
        "conversation_id": "synthetic-conversation", "message_id": "synthetic-image-source",
        "group": "铁炉西工班群", "create_time": "2026-10-18 10:00:00",
        "sender_id": "synthetic-sender", "msg_type": "image",
        "text": "[图片消息]", "resource_refs": [{
            "resourceId": "synthetic-image", "resourceIdType": "image", "contentType": "image/jpeg",
        }],
    }])

    assert plans[0]["decision"] == "REVIEW"
    assert plans[0]["reason"] == "image_content_not_recognized"
    assert calls == []
    assert dws.search_calls == 0
    assert dws.create_calls == 0


def test_partial_related_context_fails_closed_before_search_or_create():
    calls = []
    resolver = JevV2Resolver(new_policy=True, transport=lambda *args, **kwargs: calls.append((args, kwargs)) or {})
    writer, dws = FakeWriter(), FakeDws()
    processor = _processor(dws, writer, resolver)

    plans = processor.process_batch([{
        "conversation_id": "synthetic-conversation", "message_id": "synthetic-partial-source",
        "group": "铁炉西工班群", "create_time": "2026-10-18 10:00:00",
        "sender_id": "synthetic-sender", "text": "请于10月20日前完成资产盘点权限确认。",
        "reply_to_message_id": "synthetic-missing-parent", "context_status": "partial",
        "context_missing_message_ids": ["synthetic-missing-parent"],
    }])

    assert plans[0]["decision"] == "REVIEW"
    assert plans[0]["reason"] == "source_context_incomplete"
    assert calls == []
    assert dws.search_calls == 0
    assert dws.create_calls == 0


def test_partial_topic_context_keeps_its_source_as_review_without_stopping_other_messages():
    calls = []
    resolver = JevV2Resolver(new_policy=True, transport=lambda *args, **kwargs: calls.append((args, kwargs)) or {})
    writer, dws = FakeWriter(), FakeDws()
    processor = _processor(dws, writer, resolver)

    plans = processor.process_batch([{
        "conversation_id": "synthetic-conversation", "message_id": "synthetic-topic-source",
        "group": "铁炉西工班群", "create_time": "2026-10-18 10:00:00",
        "sender_id": "synthetic-sender", "text": "请于10月20日前完成资产盘点权限确认。",
        "context_status": "complete", "thread_context_status": "partial",
    }])

    assert plans[0]["decision"] == "REVIEW"
    assert "source_topic_context_incomplete" in plans[0]["warnings"]
    assert calls == []
    assert dws.search_calls == 0
    assert dws.create_calls == 0
