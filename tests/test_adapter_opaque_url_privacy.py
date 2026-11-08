"""Provider-boundary URL rejection and full-payload redaction checks."""
from __future__ import annotations

import json
from datetime import date
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from jev_v2 import JevV2Resolver
from llm_adapter import (
    DateRoleFallbackResolver,
    LlmError,
    SemanticFallbackResolver,
    TitleFieldResolver,
)
from routing import extract_due_candidates


OPAQUE_REFERENCES = [
    "https://oapi.dingtalk.com/robot/send?auth_key=FAKE_TEST_CREDENTIAL",
    "引用钉钉消息：dingtalk://chat/message?conversationId=FAKE_TEST_CONVERSATION&messageId=FAKE_TEST_MESSAGE",
]


def _event(text):
    return {
        "text": text,
        "at_users": [{"name": "王萌", "userId": "MENTION_SENTINEL"}],
        "create_time": "2026-10-07 09:00:00",
    }


def _candidate(text):
    rows = extract_due_candidates(text, date(2026, 10, 7), include_context=True)
    assert rows
    unknown = next((row for row in rows if row.get("role") == "unknown"), rows[0])
    unknown["role"] = "unknown"
    return unknown


def _construct(adapter, event, calls):
    def factory(**_kwargs):
        class Agent:
            def run_conversation(self, user_message, *, system_message, conversation_history):
                calls.append((user_message, system_message, conversation_history))
                context = json.loads(user_message)
                if "unknown_date_candidates" in context:
                    candidate = context["unknown_date_candidates"][0]
                    result = {"roles": [{
                        "key": candidate["key"],
                        "role": "historical",
                        "evidence": candidate["source_span"][:8],
                    }]}
                elif "available_context" in context:
                    result = {"title": "完成库存核对", "evidence_spans": ["核对库存"]}
                else:
                    result = {
                        "applicability": "applicable",
                        "followup": "required",
                        "title_span": None,
                        "due_key": "none",
                        "same_work": {},
                        "evidence_spans": ["核对库存"],
                    }
                return {"completed": True, "final_response": json.dumps(result, ensure_ascii=False)}
        return Agent()

    if adapter == "jev_v2":
        resolver = JevV2Resolver(new_policy=True, transport=lambda payload, _timeout: (
            calls.append((payload,)),
            {"answers": {
                key: {"type": "noul", "noul": 0.99}
                for key, question in payload["questions"].items()
                if question["type"] == "noul"
            }},
        )[1])
        return resolver, lambda: resolver.resolve(event, {"decision": "REVIEW"})
    resolver_types = {
        "semantic": SemanticFallbackResolver,
        "title": TitleFieldResolver,
        "date_role": DateRoleFallbackResolver,
    }
    resolver = resolver_types[adapter](agent_factory=factory)
    if adapter == "semantic":
        return resolver, lambda: resolver.resolve_context(event, {"decision": "REVIEW", "title": "核对库存"})
    if adapter == "title":
        return resolver, lambda: resolver.resolve(event, {"decision": "REVIEW"})
    candidate = _candidate(event["text"])
    return resolver, lambda: resolver.resolve_roles(event, date_candidates=[candidate])


@pytest.mark.parametrize("adapter", ["jev_v2", "semantic", "title", "date_role"])
@pytest.mark.parametrize("opaque_reference", OPAQUE_REFERENCES)
def test_opaque_url_sources_fail_before_each_provider_call(adapter, opaque_reference):
    calls = []
    text = f"2024年3月1日已完成历史核对。请核对库存 fileId=RESOURCE_SENTINEL @王萌。{opaque_reference}"
    resolver, invoke = _construct(adapter, _event(text), calls)

    expected_code = {
        "jev_v2": "jev_redaction_incomplete",
        "semantic": "semantic_fallback_redaction_incomplete",
        "title": "title_redaction_incomplete",
        "date_role": "date_role_redaction_incomplete",
    }[adapter]
    with pytest.raises(LlmError, match=expected_code):
        invoke()
    assert calls == []


@pytest.mark.parametrize("adapter", ["jev_v2", "semantic", "title", "date_role"])
def test_benign_text_request_is_checked_as_final_json_and_keeps_shared_redaction(adapter):
    calls = []
    text = "2024年3月1日已完成历史核对。请核对库存 fileId=RESOURCE_SENTINEL @王萌。"
    event = _event(text)
    resolver, invoke = _construct(adapter, event, calls)

    invoke()

    assert len(calls) == 1
    if adapter == "jev_v2":
        serialized = json.dumps(calls[0][0], ensure_ascii=False)
    else:
        user_message, system_message, history = calls[0]
        serialized = json.dumps({
            "user_message": user_message,
            "system_message": system_message,
            "conversation_history": history,
        }, ensure_ascii=False)
    assert "RESOURCE_SENTINEL" not in serialized
    assert "MENTION_SENTINEL" not in serialized
    assert "@王萌" not in serialized
    assert "[RESOURCE_ID]" in serialized
    assert "[PERSON_MENTION]" in serialized
