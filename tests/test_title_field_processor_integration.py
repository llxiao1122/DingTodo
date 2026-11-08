"""Processor and causal-replay acceptance for the opt-in title field compiler."""
from __future__ import annotations

import json

import pytest

from event_worker import EventProcessor, normalize_event
from jev_v2 import JevV2Resolver
from llm_adapter import TitleFieldResolver
from replay import replay_event_worker_day
from test_jev_v2 import _runtime


DEADLINE_SOURCE = (
    "请各工班在2026年10月02日18:00前完成嘉祥危险废物处置联单信息复核，并将"
    "空容器、废弃滤芯及相关包装物逐项核对补录；同步确认本年度历史处置记录与"
    "系统台账及现场实物一致，确保每项记录完整准确并反馈未完成部分。"
    "资源resource_id=FAKE-RESOURCE-7788 请@王小五跟进。"
)
TITLE = "复核嘉祥危险废物联单并补录容器、滤芯和包装物记录"
TITLE_EVIDENCE = [
    "嘉祥危险废物处置联单信息复核",
    "空容器、废弃滤芯及相关包装物逐项核对补录",
]


def _message(text: str = DEADLINE_SOURCE, message_id: str = "title-source", minute: int = 0):
    return {
        "conversation_id": "c1",
        "message_id": message_id,
        "group": "物资调配室",
        "sender": "王萌",
        "sender_id": "OTHER",
        "create_time": f"2026-09-30 10:{minute:02d}:00",
        "text": text,
        "title_context": "resource_id=UNTRUSTED-CONTEXT @不应外发",
        "_jev_v2_title_verified": True,
    }


def _runtime_with_hazard_owner():
    config, self_user = _runtime()
    config["responsibility_rules"]["hazardous_waste"] = "杨梦卓"
    config["handover_evidence"] = [{
        "period": "2026-09", "area_name": "杂品库", "active_owners": ["杨梦卓"],
        "reviewed": True, "status": "accepted", "hygiene_review": "passed",
        "source": "dws_readback", "evidence_id": "synthetic-title-handover",
    }]
    return config, self_user


def _typed_jev_transport(requests=None):
    """Build valid typed Jev v2 answers while keeping decisions program-owned."""
    requests = requests if requests is not None else []

    def transport(request, timeout):
        requests.append(request)
        answers = {}
        for name, question in request["questions"].items():
            if question["type"] == "noul":
                value = {
                    "applies_to_workgroup": 0.64,
                    "needs_follow_up": 0.93,
                    "prospective_action": 0.99,
                }.get(name, 0.99)
                answers[name] = {"type": "noul", "noul": value}
            elif question["type"] == "choice":
                criteria = question["criteria"]
                if name.startswith("date_role_"):
                    state = json.loads(request["state"])
                    candidate_key = name.removeprefix("date_role_")
                    next(row for row in state["date_role_candidates"] if row["key"] == candidate_key)
                    choice = "unknown"
                else:
                    choice = next((key for key in criteria if key.startswith("date_")), "none")
                remaining = [key for key in criteria if key != choice]
                probabilities = {choice: 0.99}
                probabilities.update({key: 0.01 / len(remaining) for key in remaining})
                answers[name] = {
                    "type": "choice",
                    "choice": choice,
                    "probabilities": probabilities,
                    "confidence": 0.99,
                }
            else:
                raise AssertionError(f"unexpected question type: {question['type']}")
        return {"answers": answers}

    return transport


class _FakeNativeAgent:
    def __init__(self, calls, response=None, error=None):
        self.calls = calls
        self.response = response if response is not None else {
            "title": TITLE,
            "evidence_spans": TITLE_EVIDENCE,
        }
        self.error = error

    def run_conversation(self, user_message, **kwargs):
        self.calls.append((user_message, kwargs))
        if self.error:
            raise self.error
        return {
            "completed": True,
            "final_response": json.dumps(self.response, ensure_ascii=False),
        }


def _title_resolver(calls, *, response=None, error=None, factory_calls=None):
    def factory(**options):
        if factory_calls is not None:
            factory_calls.append(options)
        return _FakeNativeAgent(calls, response=response, error=error)

    return TitleFieldResolver(agent_factory=factory)


def test_normalize_event_discards_untrusted_title_verification_flag_everywhere():
    event = normalize_event(_message())
    assert "_jev_v2_title_verified" not in event
    assert "_jev_v2_title_verified" not in json.dumps(event.get("raw_payload"), ensure_ascii=False)


def test_actual_processor_uses_native_title_resolver_and_preserves_program_fields():
    config, self_user = _runtime_with_hazard_owner()
    jev_requests = []
    agent_calls = []
    factory_options = []
    semantic = JevV2Resolver(new_policy=True, transport=_typed_jev_transport(jev_requests))
    compiler = _title_resolver(agent_calls, factory_calls=factory_options)
    processor = EventProcessor(
        config,
        self_user,
        mode="shadow",
        llm_resolver=semantic,
        title_resolver=compiler,
        existing_actions=[],
        business_candidate_query_complete=True,
    )

    plan = processor.process_batch([_message()])[0]

    assert len(agent_calls) == 1
    assert len(jev_requests) == 1
    user_message, native_options = agent_calls[0]
    prompt = json.loads(user_message)
    assert set(prompt) == {"source_text", "available_context"}
    assert prompt["available_context"] is None
    assert "UNTRUSTED-CONTEXT" not in user_message
    assert "FAKE-RESOURCE-7788" not in user_message
    assert "王小五" not in user_message
    assert "resource_id=[RESOURCE_ID]" in prompt["source_text"]
    assert "[PERSON_MENTION]" in prompt["source_text"]
    assert "确保每项记录完整准确并反馈未完成部分" in prompt["source_text"]
    assert prompt["source_text"].endswith("请[PERSON_MENTION]。")
    assert native_options["conversation_history"] == []
    assert factory_options[0]["enabled_toolsets"] == []
    assert len(factory_options) == 1

    assert plan["decision"] == "CREATE"
    assert plan["reason"] == "jev_v2_new_work_shadow_only"
    assert plan["title"] == TITLE
    assert DEADLINE_SOURCE in plan["detail"]
    assert plan["route_module"] == "hazardous_waste"
    assert plan["route_owner"] == "杨梦卓"
    assert plan["due_date"] == "2026-10-02"
    assert plan["due_iso"] == "2026-10-02T18:00:00+08:00"
    assert plan["semantic_evidence"]["prospective_action"] == 0.99
    assert plan["semantic_evidence"]["applies_to_workgroup"] == 0.64
    assert plan["semantic_evidence"]["needs_follow_up"] == 0.93
    assert plan["semantic_evidence"]["due_date_choice"] == "date_0"
    assert processor.stats["title_field_calls"] == 1
    assert processor.stats["title_field_succeeded"] == 1
    assert processor.stats["title_field_failed"] == 0


@pytest.mark.parametrize(
    "response,error",
    [
        ({"title": TITLE, "evidence_spans": TITLE_EVIDENCE, "route_owner": "someone"}, None),
        ({"title": "联单resource_id=LEAK-123", "evidence_spans": TITLE_EVIDENCE}, None),
        ({"title": TITLE, "evidence_spans": ["not present in source"]}, None),
        ({"title": TITLE, "evidence_spans": []}, None),
        ({"title": None, "evidence_spans": TITLE_EVIDENCE}, None),
        (None, RuntimeError("fake native agent failed")),
    ],
    ids=["extra-key", "raw-identifier", "invalid-evidence", "empty-evidence", "empty-title", "agent-error"],
)
def test_actual_processor_rejects_native_compiler_failures_and_counts_one_attempt(response, error):
    config, self_user = _runtime_with_hazard_owner()
    agent_calls = []
    semantic = JevV2Resolver(new_policy=True, transport=_typed_jev_transport())
    compiler = _title_resolver(agent_calls, response=response, error=error)
    processor = EventProcessor(
        config,
        self_user,
        mode="shadow",
        llm_resolver=semantic,
        title_resolver=compiler,
        existing_actions=[],
        business_candidate_query_complete=True,
    )

    plan = processor.process_batch([_message(message_id="rejected-title")])[0]

    assert len(agent_calls) == 1
    assert plan["decision"] == "REVIEW"
    assert plan["reason"] == "title_compiler_invalid"
    assert plan["title_compiler_error"]
    assert set(plan["title_field_error"]) == {"code", "stage"}
    assert plan["title_field_error"]["stage"] in {"transport", "schema", "validation"}
    if error is not None:
        assert plan["title_field_error"] == {"code": "title_runtime_failed", "stage": "transport"}
    assert processor.stats["title_field_calls"] == 1
    assert processor.stats["title_field_succeeded"] == 0
    assert processor.stats["title_field_failed"] == 1


def test_short_source_missing_reference_identity_or_owner_skips_title_compiler():
    config, self_user = _runtime_with_hazard_owner()

    short_agent_calls = []
    short = EventProcessor(
        config,
        self_user,
        mode="shadow",
        llm_resolver=JevV2Resolver(new_policy=True, transport=_typed_jev_transport()),
        title_resolver=_title_resolver(short_agent_calls),
        existing_actions=[],
        business_candidate_query_complete=True,
    )
    short_plan = short.process_batch([_message("请各工班复核危险废物清单。", "short")])[0]
    assert short_plan["decision"] == "CREATE"
    assert short_agent_calls == []
    assert short.stats["title_field_calls"] == 0

    missing_reference_calls = []
    missing_reference_source = (
        "请各工班按当前危险废物处置安排完成系统记录核对和反馈，"
        "并同步检查相关联单信息，具体内容请查看R列。"
    )
    missing_reference = EventProcessor(
        config,
        self_user,
        mode="shadow",
        llm_resolver=JevV2Resolver(new_policy=True, transport=_typed_jev_transport()),
        title_resolver=_title_resolver(missing_reference_calls),
        existing_actions=[],
        business_candidate_query_complete=True,
    )
    reference_plan = missing_reference.process_batch([
        _message(missing_reference_source, "missing-reference")
    ])[0]
    assert reference_plan["decision"] == "REVIEW"
    assert reference_plan["reason"] == "jev_v2_referenced_context_missing"
    assert missing_reference_calls == []
    assert missing_reference.stats["title_field_calls"] == 0

    missing_identity_calls = []
    missing_identity = EventProcessor(
        config,
        self_user,
        mode="shadow",
        llm_resolver=JevV2Resolver(new_policy=True, transport=_typed_jev_transport()),
        title_resolver=_title_resolver(missing_identity_calls),
        existing_actions=[],
        business_candidate_query_complete=True,
    )
    assert missing_identity.process_batch([{"text": DEADLINE_SOURCE}]) == []
    assert missing_identity.stats["errors"] == 1
    assert missing_identity_calls == []
    assert missing_identity.stats["title_field_calls"] == 0

    missing_owner_config, missing_owner_self = _runtime()
    missing_owner_calls = []
    missing_owner = EventProcessor(
        missing_owner_config,
        missing_owner_self,
        mode="shadow",
        llm_resolver=JevV2Resolver(new_policy=True, transport=_typed_jev_transport()),
        title_resolver=_title_resolver(missing_owner_calls),
        existing_actions=[],
        business_candidate_query_complete=True,
    )
    owner_plan = missing_owner.process_batch([_message(message_id="missing-owner")])[0]
    assert owner_plan["decision"] == "REVIEW"
    assert owner_plan["reason"] == "confirmed_area_handover_unverified"
    assert "current_area_owner" in owner_plan["missing_fields"]
    assert missing_owner_calls == []
    assert missing_owner.stats["title_field_calls"] == 0


def test_oversized_source_fails_closed_without_calling_native_agent():
    config, self_user = _runtime_with_hazard_owner()
    native_calls = []
    semantic = JevV2Resolver(new_policy=True, transport=_typed_jev_transport())

    def factory(**options):
        return _FakeNativeAgent(native_calls)

    compiler = TitleFieldResolver(max_source_chars=80, agent_factory=factory)
    processor = EventProcessor(
        config,
        self_user,
        mode="shadow",
        llm_resolver=semantic,
        title_resolver=compiler,
        existing_actions=[],
        business_candidate_query_complete=True,
    )
    plan = processor.process_batch([_message(message_id="oversized-source")])[0]

    assert plan["decision"] == "REVIEW"
    assert plan["reason"] == "title_compiler_invalid"
    assert native_calls == []
    assert processor.stats["title_field_calls"] == 1
    assert processor.stats["title_field_succeeded"] == 0
    assert processor.stats["title_field_failed"] == 1


def test_causal_replay_counts_real_title_attempts_and_separates_source_and_business_matches():
    config, self_user = _runtime_with_hazard_owner()
    jev_requests = []
    agent_calls = []
    semantic = JevV2Resolver(new_policy=True, transport=_typed_jev_transport(jev_requests))
    compiler = _title_resolver(agent_calls)
    first = _message(DEADLINE_SOURCE, "causal-first", 0)
    same_source_redelivery = dict(first)
    second_source = _message(
        DEADLINE_SOURCE.replace(
            "完成嘉祥危险废物处置联单信息复核",
            "完成嘉祥危险废物处置联单内容检查",
        ).replace("逐项核对补录", "分别检查并补充登记"),
        "causal-related",
        1,
    )
    payload = {
        "source_day": "2026-09-30",
        "config": config,
        "self_user": self_user,
        "business_candidate_query_complete": True,
        "existing_actions": [],
        "messages": [first, same_source_redelivery, second_source],
    }

    result = replay_event_worker_day(payload, resolver=semantic, title_resolver=compiler)

    assert [plan["decision"] for plan in result["plans"]] == ["CREATE", "IGNORE", "IGNORE"]
    assert result["plans"][1]["reason"] == "source_already_processed"
    assert result["plans"][2]["reason"] == "jev_v2_same_work_already_exists"
    assert len(jev_requests) == 2
    second_state = json.loads(jev_requests[1]["state"])
    assert second_state["business_candidates"]
    assert "FAKE-RESOURCE-7788" not in json.dumps(second_state, ensure_ascii=False)
    assert any(action.get("message_id") == "causal-first" for action in result["simulated_ledger"])
    assert len(agent_calls) == 1
    assert result["summary"]["title_field_calls"] == 1
    assert result["summary"]["title_field_succeeded"] == 1
    assert result["summary"]["title_field_failed"] == 0
    assert result["summary"]["writes_performed"] == 0
    assert result["summary"]["external_todos_created"] == 0


def test_causal_replay_aggregates_a_real_title_compiler_failure():
    config, self_user = _runtime_with_hazard_owner()
    agent_calls = []
    semantic = JevV2Resolver(new_policy=True, transport=_typed_jev_transport())
    compiler = _title_resolver(
        agent_calls,
        response={"title": TITLE, "evidence_spans": []},
    )
    result = replay_event_worker_day(
        {
            "source_day": "2026-09-30",
            "config": config,
            "self_user": self_user,
            "business_candidate_query_complete": True,
            "existing_actions": [],
            "messages": [_message(message_id="causal-title-failure")],
        },
        resolver=semantic,
        title_resolver=compiler,
    )

    assert len(agent_calls) == 1
    assert result["plans"][0]["decision"] == "REVIEW"
    assert result["plans"][0]["reason"] == "title_compiler_invalid"
    assert result["summary"]["title_field_calls"] == 1
    assert result["summary"]["title_field_succeeded"] == 0
    assert result["summary"]["title_field_failed"] == 1


def test_title_resolver_remains_opt_in_and_shadow_only():
    config, self_user = _runtime_with_hazard_owner()
    processor = EventProcessor(config, self_user, mode="shadow")
    assert processor.title_resolver is None
    with pytest.raises(ValueError, match="title_fields_require_read_only_shadow"):
        EventProcessor(config, self_user, mode="production", title_resolver=object())
