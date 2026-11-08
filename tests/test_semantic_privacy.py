"""Semantic fallback privacy contract through fake native and typed transports."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from event_worker import EventProcessor
from jev_v2 import JevV2Resolver
from llm_adapter import (
    LlmError,
    SemanticFallbackResolver,
    TitleFieldResolver,
    _has_unredacted_jev_content,
)
from test_jev_v2 import _runtime


SOURCE = (
    "请各工班及时核对库存，更新台账并完成复核；有疑问及时反馈，确保库区记录完整。"
    " fileId=SOURCE-RESOURCE-701 @王萌"
)
OLD_BUSINESS_TEXT = "既有库存核对事项 mediaId=BUSINESS-RESOURCE-802 @赵莉。"
TITLE_TEXT = "检查库存记录 fileId=TITLE-RESOURCE-903 @陈鹭"


class _CapturingAgent:
    def __init__(self, response_factory):
        self.response_factory = response_factory
        self.calls: list[tuple[str, dict]] = []

    def run_conversation(self, user_message, **kwargs):
        self.calls.append((user_message, kwargs))
        request = json.loads(user_message)
        response = self.response_factory(request)
        return {"completed": True, "final_response": json.dumps(response, ensure_ascii=False)}


def _semantic_response(request, mode="valid"):
    title = request["title_candidates"][0]["text"]
    evidence = title[: min(12, len(title))]
    response = {
        "applicability": "applicable",
        "followup": "required",
        "title_span": None,
        "due_key": "none",
        "same_work": {row["key"]: "different" for row in request["same_work_candidates"]},
        "evidence_spans": [evidence],
    }
    if mode == "invalid_quote":
        response["evidence_spans"] = ["not present in sanitized source"]
    elif mode == "extra_field":
        response["owner"] = "hidden owner"
    elif mode == "invalid_candidate_key":
        response["same_work"] = {"business_99": "different"}
    return response


def test_semantic_payload_redacts_source_title_and_business_text_and_omits_hidden_fields():
    agent = _CapturingAgent(_semantic_response)
    resolver = SemanticFallbackResolver(agent_factory=lambda **_kwargs: agent)
    message = {
        "text": "请核对 fileId=SOURCE-RESOURCE-701 @王萌。",
        "create_time": "2026-10-06 10:00:00",
        "conversation_id": "HIDDEN-CONVERSATION-1001",
        "message_id": "HIDDEN-MESSAGE-1002",
        "group": "HIDDEN-GROUP-1003",
        "sender": "HIDDEN-SENDER-1004",
        "sender_id": "HIDDEN-SENDER-ID-1005",
        "_jev_v2_policy_context": {"workgroup_role_mentioned": True},
        "human_confirmation": "HIDDEN-HUMAN-CONFIRMATION-1006",
    }
    plan = {
        "decision": "REVIEW",
        "reason": "jev_v2_scope_or_followup_uncertain",
        "title": TITLE_TEXT,
        "route_module": "HIDDEN-ROUTE-1007",
        "route_owner": "HIDDEN-OWNER-1008",
        "reference_label": "HIDDEN-REFERENCE-1009",
        "semantic_evidence": {"user_confirmation": "HIDDEN-SEMANTIC-CONFIRMATION-1010"},
    }
    dates = [{
        "key": "date_0",
        "due_date": "2026-10-08",
        "due_iso": "2026-10-08T18:00:00+08:00",
        "source_context": "fileId=DATE-CONTEXT-1011 @隐藏姓名",
        "source_span": "fileId=DATE-SPAN-1012",
        "reference_label": "HIDDEN-DATE-REFERENCE-1013",
    }]
    business = [{
        "key": "business_0",
        "text": OLD_BUSINESS_TEXT,
        "create_time": "2026-10-05 09:00:00",
        "owner": "HIDDEN-CANDIDATE-OWNER-1014",
        "route": "HIDDEN-CANDIDATE-ROUTE-1015",
        "message_id": "HIDDEN-CANDIDATE-MESSAGE-1016",
        "reference_label": "HIDDEN-CANDIDATE-REFERENCE-1017",
    }]

    result = resolver.resolve_context(message, plan, date_candidates=dates, business_candidates=business)

    assert result["same_work"] == {"business_0": "different"}
    assert resolver.last_title_candidates == [{"key": "title_0", "text": TITLE_TEXT}]
    assert len(agent.calls) == 1
    encoded = agent.calls[0][0]
    assert _has_unredacted_jev_content(encoded) is False
    payload = json.loads(encoded)
    assert result["evidence_spans"] == [payload["title_candidates"][0]["text"][:12]]
    assert set(payload) == {"text", "title_candidates", "date_candidates", "same_work_candidates", "policy_context"}
    assert payload["title_candidates"] == [{
        "key": "title_0",
        "text": "检查库存记录 fileId=[RESOURCE_ID] [PERSON_MENTION]",
    }]
    assert set(payload["same_work_candidates"][0]) == {"key", "text"}
    assert payload["same_work_candidates"][0]["text"] == "既有库存核对事项 mediaId=[RESOURCE_ID] [PERSON_MENTION]。"
    assert payload["date_candidates"] == [{
        "key": "date_0",
        "due_date": "2026-10-08",
        "due_iso": "2026-10-08T18:00:00+08:00",
    }]
    serialized_for_assertions = encoded + json.dumps(agent.calls[0][1], ensure_ascii=False)
    for marker in (
        "SOURCE-RESOURCE-701", "TITLE-RESOURCE-903", "BUSINESS-RESOURCE-802",
        "HIDDEN-CONVERSATION-1001", "HIDDEN-MESSAGE-1002", "HIDDEN-GROUP-1003",
        "HIDDEN-SENDER-1004", "HIDDEN-SENDER-ID-1005", "HIDDEN-ROUTE-1007",
        "HIDDEN-OWNER-1008", "HIDDEN-REFERENCE-1009", "HIDDEN-HUMAN-CONFIRMATION-1006",
        "HIDDEN-SEMANTIC-CONFIRMATION-1010", "DATE-CONTEXT-1011", "DATE-SPAN-1012",
        "HIDDEN-DATE-REFERENCE-1013", "HIDDEN-CANDIDATE-OWNER-1014",
        "HIDDEN-CANDIDATE-ROUTE-1015", "HIDDEN-CANDIDATE-MESSAGE-1016",
        "HIDDEN-CANDIDATE-REFERENCE-1017", "王萌", "赵莉", "陈鹭",
    ):
        assert marker not in serialized_for_assertions


@pytest.mark.parametrize(
    "location",
    ["source", "title", "business_candidate", "hidden_policy_field"],
)
def test_unsupported_encoding_or_hidden_policy_field_fails_before_agent_call(location):
    agent = _CapturingAgent(_semantic_response)
    resolver = SemanticFallbackResolver(agent_factory=lambda **_kwargs: agent)
    message = {"text": "请核对库存。"}
    plan = {"title": "核对库存"}
    business = []
    if location == "source":
        message["text"] = "请核对 fileId%253DSOURCE-DOUBLE-1101。"
    elif location == "title":
        plan["title"] = "核对 fileId%253DTITLE-DOUBLE-1102"
    elif location == "business_candidate":
        business = [{"key": "business_0", "text": "既有事项 messageId%253ABUSINESS-DOUBLE-1103"}]
    else:
        message["_jev_v2_policy_context"] = {
            "workgroup_role_mentioned": True,
            "owner_id": "HIDDEN-OWNER-1104",
        }

    with pytest.raises((LlmError, ValueError)):
        resolver.resolve_context(message, plan, business_candidates=business)
    assert agent.calls == []


def test_short_local_title_with_redacted_identifier_cannot_be_selected():
    agent = _CapturingAgent(lambda request: {
        "applicability": "applicable",
        "followup": "required",
        "title_span": "title_0",
        "due_key": "none",
        "same_work": {},
        "evidence_spans": ["核对库存"],
    })
    resolver = SemanticFallbackResolver(agent_factory=lambda **_kwargs: agent)
    with pytest.raises(ValueError, match="semantic_fallback_title_invalid"):
        resolver.resolve_context(
            {"text": "核对库存 fileId=SHORT-TITLE-1105"},
            {"title": "核对库存 fileId=SHORT-TITLE-1105"},
        )
    assert len(agent.calls) == 1


def test_overlong_semantic_source_fails_before_agent_call_without_truncation():
    agent = _CapturingAgent(_semantic_response)
    resolver = SemanticFallbackResolver(
        max_source_chars=8,
        agent_factory=lambda **_kwargs: agent,
    )
    with pytest.raises(LlmError, match="semantic_fallback_source_incomplete"):
        resolver.resolve_context({"text": "请核对库存记录并更新台账。"}, {"title": "核对库存"})
    assert agent.calls == []


def test_truncated_prior_business_candidate_fails_before_agent_call():
    agent = _CapturingAgent(_semantic_response)
    resolver = SemanticFallbackResolver(agent_factory=lambda **_kwargs: agent)
    with pytest.raises(LlmError, match="semantic_fallback_business_candidate_incomplete"):
        resolver.resolve_context(
            {"text": "请核对库存并更新台账。"},
            {"title": "核对库存"},
            business_candidates=[{
                "key": "business_0",
                "text": "既有库存核对事项"[:8],
                "_semantic_source_truncated": True,
            }],
        )
    assert agent.calls == []


def test_overlong_typed_jev_source_fails_before_transport_without_truncation():
    calls = []
    resolver = JevV2Resolver(new_policy=True, transport=lambda request, _timeout: calls.append(request))
    with pytest.raises(LlmError, match="jev_v2_source_incomplete"):
        resolver.resolve({"text": "库存核对" * 1001}, {"decision": "REVIEW"})
    assert calls == []


def _typed_jev_transport(calls):
    def transport(request, _timeout):
        calls.append(request)
        answers = {}
        for name, question in request["questions"].items():
            if question["type"] == "noul":
                if name == "prospective_action":
                    score = 0.99
                elif name.startswith("same_work_"):
                    score = 0.50
                else:
                    score = 0.50
                answers[name] = {"type": "noul", "noul": score}
            else:
                options = question["criteria"]
                selected = "none" if "none" in options else "uncertain" if "uncertain" in options else next(iter(options))
                remainder = 0.01 / max(1, len(options) - 1)
                answers[name] = {
                    "type": "choice",
                    "choice": selected,
                    "probabilities": {key: 0.99 if key == selected else remainder for key in options},
                    "confidence": 0.99,
                }
        return {"answers": answers}

    return transport


def _pipeline(mode="valid", *, corrupt_after_validation=False):
    config, self_user = _runtime()
    jev_requests = []
    semantic_agent = _CapturingAgent(lambda request: _semantic_response(request, mode))
    semantic = SemanticFallbackResolver(agent_factory=lambda **_kwargs: semantic_agent)
    title_agent = _CapturingAgent(lambda request: {
        "title": "核对库存并更新台账",
        "evidence_spans": ["及时核对库存"],
    })
    title = TitleFieldResolver(agent_factory=lambda **_kwargs: title_agent)
    processor = EventProcessor(
        config,
        self_user,
        mode="shadow",
        llm_resolver=JevV2Resolver(new_policy=True, transport=_typed_jev_transport(jev_requests)),
        semantic_fallback=semantic,
        title_resolver=title,
        existing_actions=[{
            "business_key": "previous-stock-check",
            "external_task_id": "external-previous-stock-check",
            "status": "EXTERNAL_CREATED",
            "source_text": OLD_BUSINESS_TEXT,
            "message_time": "2026-10-05 09:00:00",
            "route_module": "materials",
            "route_owner": "李林骁",
        }],
        business_candidate_query_complete=True,
    )
    if corrupt_after_validation:
        original_resolve = semantic.resolve_context

        def altered_result(*args, **kwargs):
            answer = original_resolve(*args, **kwargs)
            answer["evidence_spans"] = ["mutated after resolver validation"]
            return answer

        semantic.resolve_context = altered_result
    plan = processor.process_batch([{
        "conversationId": "c1",
        "messageId": "CURRENT-MESSAGE-PRIVACY-1201",
        "group": "物资调配室",
        "senderId": "CURRENT-SENDER-ID-1202",
        "sender": "CURRENT-SENDER-1203",
        "createTime": "2026-10-06 10:00:00",
        "text": SOURCE,
        "atUsers": [{"name": "王萌", "userId": "CURRENT-MENTION-ID-1204"}],
        "reference_label": "HIDDEN-CURRENT-REFERENCE-1205",
        "human_confirmation": "HIDDEN-CURRENT-CONFIRMATION-1206",
    }])[0]
    return plan, processor, semantic_agent, title_agent, jev_requests


def test_real_processor_and_typed_jev_reach_validated_business_different_stage_with_redacted_payload():
    plan, processor, semantic_agent, title_agent, jev_requests = _pipeline()

    assert len(jev_requests) == 1
    assert len(semantic_agent.calls) == 1
    encoded = semantic_agent.calls[0][0]
    assert _has_unredacted_jev_content(encoded) is False
    semantic_payload = json.loads(encoded)
    assert semantic_payload["same_work_candidates"] == [{
        "key": "business_0",
        "text": "既有库存核对事项 mediaId=[RESOURCE_ID] [PERSON_MENTION]。",
    }]
    assert semantic_agent.calls[0][1]["conversation_history"] == []
    for secret in (
        "SOURCE-RESOURCE-701", "BUSINESS-RESOURCE-802", "CURRENT-MESSAGE-PRIVACY-1201",
        "CURRENT-SENDER-ID-1202", "CURRENT-SENDER-1203", "CURRENT-MENTION-ID-1204",
        "HIDDEN-CURRENT-REFERENCE-1205", "HIDDEN-CURRENT-CONFIRMATION-1206",
        "王萌", "赵莉",
    ):
        assert secret not in encoded
    assert processor.stats["semantic_fallback_calls"] == 1
    assert processor.stats["semantic_fallback_succeeded"] == 1
    assert processor.stats["semantic_fallback_failed"] == 0
    assert plan["fallback_semantic_evidence"]["same_work"] == {"business_0": "different"}
    assert plan["fallback_semantic_evidence"]["evidence_spans"] == [semantic_payload["title_candidates"][0]["text"][:12]]
    assert plan["decision"] == "CREATE"
    assert plan["reason"] == "jev_v2_new_work_shadow_only"
    if title_agent.calls:
        title_input = title_agent.calls[0][0]
        assert _has_unredacted_jev_content(title_input) is False


@pytest.mark.parametrize(
    ("mode", "expected_error", "expected_stage"),
    [
        ("invalid_quote", "semantic_fallback_evidence_invalid", "validation"),
        ("extra_field", "semantic_fallback_schema_invalid", "schema"),
        ("invalid_candidate_key", "semantic_fallback_match_invalid", "validation"),
    ],
)
def test_invalid_semantic_output_stays_review_with_failure_counters(
    mode, expected_error, expected_stage
):
    plan, processor, semantic_agent, _title_agent, _jev_requests = _pipeline(mode)

    assert len(semantic_agent.calls) == 1
    assert plan["decision"] == "REVIEW"
    assert processor.stats["semantic_fallback_calls"] == 1
    assert processor.stats["semantic_fallback_succeeded"] == 0
    assert processor.stats["semantic_fallback_failed"] == 1
    assert plan["semantic_fallback_error"] == {"code": expected_error, "stage": expected_stage}


def test_processor_revalidates_shared_contract_after_resolver_return():
    plan, processor, semantic_agent, _title_agent, _jev_requests = _pipeline(corrupt_after_validation=True)

    assert len(semantic_agent.calls) == 1
    assert plan["decision"] == "REVIEW"
    assert processor.stats["semantic_fallback_calls"] == 1
    assert processor.stats["semantic_fallback_succeeded"] == 0
    assert processor.stats["semantic_fallback_failed"] == 1
    assert plan["semantic_fallback_error"] == {
        "code": "semantic_fallback_evidence_invalid",
        "stage": "validation",
    }
