"""Evidence-limited date-role fallback through the real shadow processor."""
from __future__ import annotations

import json
from datetime import date

import pytest

from event_worker import EventProcessor
from jev_v2 import JevV2Resolver
from llm_adapter import DateRoleFallbackResolver, LlmError, TitleFieldResolver
from replay import replay_event_worker_day
from routing import extract_due_candidates
from test_jev_v2 import _runtime


LONG_HISTORICAL_SOURCE = (
    "2024年3月1日已完成危废转运并归档相关资料。现请各工班及时在系统中补录该批次危险废物转运联单信息，"
    "核对台账字段完整性，确认扫描件与纸质资料一致，并将遗漏字段补充齐全后反馈归档完成情况；"
    "发现缺项时请及时反馈协调处理。"
)


class _NativeHarness:
    def __init__(self, response_kind="valid"):
        self.response_kind = response_kind
        self.requests = []
        self.agent_configs = []
        self.agents = []
        self.date_calls = 0
        self.title_calls = 0

    def factory(self, **kwargs):
        self.agent_configs.append(kwargs)
        harness = self

        class Agent:
            def run_conversation(self, user_message, *, system_message, conversation_history):
                context = json.loads(user_message)
                harness.requests.append((context, user_message, system_message))
                if "unknown_date_candidates" in context:
                    harness.date_calls += 1
                    candidate = context["unknown_date_candidates"][0]
                    span = candidate["source_span"]
                    evidence = next(
                        (item for item in (
                            "已完成危废转运并归档相关资料",
                            "已完成危废转运",
                            "已完成处置",
                        ) if item in span),
                        span[: min(24, len(span))],
                    )
                    role = "historical"
                    row = {"key": candidate["key"], "role": role, "evidence": evidence}
                    if harness.response_kind == "unknown_role":
                        row = {"key": candidate["key"], "role": "unknown", "evidence": evidence}
                        result = {"roles": [row]}
                    elif harness.response_kind == "invented_deadline":
                        row = {"key": candidate["key"], "role": "deadline", "evidence": "2026年10月8日前"}
                        result = {"roles": [row], "due_date": "2026-10-08"}
                    elif harness.response_kind == "raw_ids":
                        result = {"roles": [row], "taskIds": ["RAW_TASK_123"]}
                    elif harness.response_kind == "extra_owner":
                        result = {"roles": [row], "owner": "伪造责任人"}
                    else:
                        result = {"roles": [row]}
                    return {"completed": True, "final_response": json.dumps(result, ensure_ascii=False)}

                harness.title_calls += 1
                source = context["source_text"]
                result = {
                    "title": "危废转运历史资料补录、台账核对及材料归集",
                    "evidence_spans": [source[: min(24, len(source))]],
                }
                return {"completed": True, "final_response": json.dumps(result, ensure_ascii=False)}

        agent = Agent()
        self.agents.append(agent)
        return agent


def _jev_transport(requests, *, role_confidence=0.55, role_choice="deadline"):
    def transport(request, _timeout):
        requests.append(request)
        answers = {}
        for name, question in request["questions"].items():
            if question["type"] == "noul":
                answers[name] = {"type": "noul", "noul": {
                    "applies_to_workgroup": 0.64,
                    "needs_follow_up": 0.93,
                    "prospective_action": 0.99,
                }.get(name, 0.01)}
                continue
            options = question["criteria"]
            if name.startswith("date_role_"):
                selected = role_choice
                confidence = role_confidence
                remainder = (1.0 - confidence) / (len(options) - 1)
                probabilities = {key: confidence if key == selected else remainder for key in options}
            elif "none" in options:
                selected = "none"
                confidence = 0.99
                probabilities = {key: (confidence if key == selected else (1 - confidence) / (len(options) - 1)) for key in options}
            else:
                selected = "unknown"
                confidence = 0.99
                probabilities = {key: (confidence if key == selected else (1 - confidence) / (len(options) - 1)) for key in options}
            answers[name] = {"type": "choice", "choice": selected,
                             "probabilities": probabilities, "confidence": confidence}
        return {"answers": answers}
    return transport


def _make_processor(
    text,
    *,
    native=None,
    title_resolver=None,
    role_confidence=0.55,
    role_choice="deadline",
    config_update=None,
    event_update=None,
):
    config, self_user = _runtime()
    config["responsibility_rules"]["hazardous_waste"] = "苗笑天"
    if config_update:
        config_update(config)
    jev_requests = []
    resolver = JevV2Resolver(
        new_policy=True,
        transport=_jev_transport(
            jev_requests,
            role_confidence=role_confidence,
            role_choice=role_choice,
        ),
    )
    fallback = DateRoleFallbackResolver(agent_factory=native.factory) if native else None
    processor = EventProcessor(
        config,
        self_user,
        mode="shadow",
        llm_resolver=resolver,
        date_role_fallback_resolver=fallback,
        title_resolver=title_resolver,
        existing_actions=[],
        business_candidate_query_complete=True,
    )
    event = {
        "conversation_id": "c1",
        "message_id": "date-role-source",
        "group": "物资调配室",
        "sender": "王萌",
        "sender_id": "OTHER",
        "create_time": "2026-10-07 09:00:00",
        "text": text,
    }
    if event_update:
        event_update(event)
    return processor, event, jev_requests


def test_historical_date_role_fallback_replans_undated_followup_and_compiles_title():
    source = LONG_HISTORICAL_SOURCE
    native = _NativeHarness()
    title = TitleFieldResolver(agent_factory=native.factory)
    processor, event, jev_requests = _make_processor(
        source, native=native, title_resolver=title,
    )

    plan = processor.process_batch([event])[0]

    assert len(jev_requests) == 1
    assert native.date_calls == 1
    assert native.title_calls == 1
    assert len(native.agent_configs) == 2
    assert all(config["enabled_toolsets"] == [] for config in native.agent_configs)
    assert all(config["max_iterations"] == 1 for config in native.agent_configs)
    assert all(config["save_trajectories"] is False for config in native.agent_configs)
    assert all(agent._persist_disabled is True for agent in native.agents)
    assert processor.stats["date_role_fallback_calls"] == 1
    assert processor.stats["date_role_fallback_succeeded"] == 1
    assert processor.stats["date_role_fallback_failed"] == 0
    assert processor.stats["date_role_fallback_latency_ms_total"] >= 0
    assert plan["decision"] == "CREATE"
    assert plan["route_module"] == "hazardous_waste"
    assert plan["route_owner"] == "苗笑天"
    assert plan.get("due_date") is None
    assert plan.get("due_iso") is None
    assert plan["title"] == "危废转运历史资料补录、台账核对及材料归集"

    jev_state = json.loads(jev_requests[0]["state"])
    assert jev_state["date_candidates"][0]["due_date"] == "2024-03-01"
    fallback_context, serialized, _instruction = native.requests[0]
    assert set(fallback_context) == {"source_text", "unknown_date_candidates"}
    assert set(fallback_context["unknown_date_candidates"][0]) == {"key", "source_span"}
    assert "date-role-source" not in serialized
    assert "苗笑天" not in serialized
    assert "decision" not in fallback_context
    assert fallback_context["unknown_date_candidates"] == [{
        "key": "date_0",
        "source_span": fallback_context["unknown_date_candidates"][0]["source_span"],
    }]
    role_evidence = plan["date_role_fallback_evidence"]["roles"][0]
    assert set(plan["date_role_fallback_evidence"]) == {"roles"}
    assert set(role_evidence) == {"key", "role", "evidence"}
    assert role_evidence["role"] == "historical"
    assert role_evidence["evidence"] in fallback_context["unknown_date_candidates"][0]["source_span"]
    assert plan["semantic_evidence"]["date_role_judgments"][0]["confidence"] == 0.55
    assert plan["semantic_evidence"]["date_role_judgments"][0]["model_choice"] == "uncertain"
    assert "confidence" not in role_evidence


def test_date_role_adapter_sanitizes_the_final_serialized_source_request():
    source = LONG_HISTORICAL_SOURCE + " 任务ID: RAW_TASK_778 fileId=RAW_FILE_991"
    candidates = extract_due_candidates(source, date(2026, 10, 7), include_context=True)
    native = _NativeHarness()
    resolver = DateRoleFallbackResolver(agent_factory=native.factory)

    result = resolver.resolve_roles({"text": source}, date_candidates=[candidates[0]])

    context, serialized, _instruction = native.requests[0]
    assert result["roles"][0]["role"] == "historical"
    assert "RAW_TASK_778" not in serialized
    assert "RAW_FILE_991" not in serialized
    assert "[TASK_ID]" in context["source_text"]
    assert "[RESOURCE_ID]" in context["source_text"]
    assert set(context) == {"source_text", "unknown_date_candidates"}


def test_date_role_fallback_rejects_raw_ids_owner_and_invented_deadline_outputs():
    for response_kind in ("raw_ids", "extra_owner", "invented_deadline"):
        native = _NativeHarness(response_kind)
        processor, event, _ = _make_processor(LONG_HISTORICAL_SOURCE, native=native)

        plan = processor.process_batch([event])[0]

        assert plan["decision"] == "REVIEW"
        assert plan["reason"] == "jev_v2_date_role_unverified"
        assert plan.get("due_date") is None
        assert plan.get("due_iso") is None
        assert processor.stats["date_role_fallback_calls"] == 1
        assert processor.stats["date_role_fallback_succeeded"] == 0
        assert processor.stats["date_role_fallback_failed"] == 1


def test_unknown_date_role_response_remains_review_and_source_call_is_bounded_to_one():
    native = _NativeHarness("unknown_role")
    processor, event, _ = _make_processor(LONG_HISTORICAL_SOURCE, native=native)

    first = processor.process_batch([event])[0]
    second = processor.process_batch([event])[0]

    assert first["decision"] == "REVIEW"
    assert first["reason"] == "jev_v2_date_role_unverified"
    assert first.get("due_date") is None
    assert second["decision"] == "IGNORE"
    assert second["reason"] == "source_already_processed"
    assert processor.stats["date_role_fallback_calls"] == 1
    assert processor.stats["date_role_fallback_succeeded"] == 1
    assert processor.stats["date_role_fallback_failed"] == 0
    assert native.date_calls == 1


def test_date_role_resolver_failure_is_recorded_without_replacing_review_context():
    class FailingDateRoleResolver:
        max_source_chars = 4000

        def __init__(self):
            self.calls = 0

        def resolve_roles(self, _event, *, date_candidates=()):
            self.calls += 1
            raise LlmError("date_role_runtime_failed:PRIVATE_RESPONSE @PrivateName")

    processor, event, _jev_requests = _make_processor(LONG_HISTORICAL_SOURCE)
    fallback = FailingDateRoleResolver()
    processor.date_role_fallback_resolver = fallback

    plan = processor.process_batch([event])[0]

    assert fallback.calls == 1
    assert processor.stats["date_role_fallback_calls"] == 1
    assert processor.stats["date_role_fallback_succeeded"] == 0
    assert processor.stats["date_role_fallback_failed"] == 1
    assert plan["decision"] == "REVIEW"
    assert plan["reason"] == "jev_v2_date_role_unverified"
    assert plan.get("due_date") is None
    assert plan.get("due_iso") is None
    assert plan["date_role_fallback_error"] == {
        "code": "date_role_runtime_failed",
        "stage": "transport",
    }
    diagnostic = json.dumps(plan["date_role_fallback_error"], ensure_ascii=False)
    assert "PRIVATE_RESPONSE" not in diagnostic
    assert "PrivateName" not in diagnostic


def test_date_role_fallback_is_disabled_without_explicit_resolver_opt_in():
    processor, event, _ = _make_processor(LONG_HISTORICAL_SOURCE)

    plan = processor.process_batch([event])[0]

    assert plan["decision"] == "REVIEW"
    assert plan["reason"] == "jev_v2_date_role_unverified"
    assert processor.stats["date_role_fallback_calls"] == 0
    assert processor.stats["date_role_fallback_succeeded"] == 0
    assert processor.stats["date_role_fallback_failed"] == 0


def test_confident_jev_unknown_role_does_not_trigger_second_reasoning_call():
    native = _NativeHarness()
    processor, event, _ = _make_processor(
        LONG_HISTORICAL_SOURCE,
        native=native,
        role_confidence=0.99,
        role_choice="unknown",
    )

    plan = processor.process_batch([event])[0]

    assert plan["decision"] == "REVIEW"
    assert plan["reason"] == "jev_v2_date_role_unverified"
    assert native.date_calls == 0
    assert processor.stats["date_role_fallback_calls"] == 0


def test_date_role_fallback_resolver_can_only_be_enabled_in_read_only_shadow():
    config, self_user = _runtime()
    with pytest.raises(ValueError, match="date_role_fallback_requires_read_only_shadow"):
        EventProcessor(
            config,
            self_user,
            mode="test",
            date_role_fallback_resolver=DateRoleFallbackResolver(agent_factory=_NativeHarness().factory),
        )


def test_date_role_fallback_only_receives_unknown_candidates_and_preserves_known_eleven_deadline():
    source = (
        "2026年10月8日18时终止当前危废处置安排。"
        "2024年3月1日已完成危废转运并归档。"
        "请各工班于2026年10月7日11:00前补录该批次联单信息并完成台账核对。"
    )
    native = _NativeHarness()
    title = TitleFieldResolver(agent_factory=native.factory)
    processor, event, _ = _make_processor(source, native=native, title_resolver=title)

    plan = processor.process_batch([event])[0]

    fallback_context = native.requests[0][0]
    assert [row["key"] for row in fallback_context["unknown_date_candidates"]] == ["date_1"]
    assert plan["decision"] == "CREATE"
    assert plan["due_date"] == "2026-10-07"
    assert plan["due_iso"] == "2026-10-07T11:00:00+08:00"
    assert plan["date_role_fallback_evidence"]["roles"][0]["key"] == "date_1"
    jev_roles = {
        row["candidate_key"]: row["resolved_role"]
        for row in plan["semantic_evidence"]["date_role_judgments"]
    }
    assert jev_roles["date_0"] == "effective"
    assert jev_roles["date_2"] == "deadline"


def test_competing_cutoff_and_incomplete_or_identity_context_keep_their_review_gates():
    cutoff = "请各工班今天下班前完成危险废物处置联单信息更新。4点30分仍未更新的工班，将纳入本月考核项。"
    native = _NativeHarness()
    processor, event, _ = _make_processor(cutoff, native=native)
    plan = processor.process_batch([event])[0]
    assert plan["decision"] == "REVIEW"
    assert plan["reason"] == "jev_v2_competing_deadline_context"
    assert native.date_calls == 0
    assert processor.stats["date_role_fallback_calls"] == 0

    referenced = "2024年3月1日已完成危废转运。请各工班完成联单补录，具体要求见R列。"
    native = _NativeHarness()
    processor, event, _ = _make_processor(referenced, native=native)
    plan = processor.process_batch([event])[0]
    assert plan["reason"] == "jev_v2_referenced_context_missing"
    assert native.date_calls == 0

    native = _NativeHarness()
    processor, event, _ = _make_processor(
        LONG_HISTORICAL_SOURCE,
        native=native,
        event_update=lambda msg: msg.update(sender_id="SELF"),
    )
    processor.process_batch([event])
    assert native.date_calls == 0

    native = _NativeHarness()
    processor, event, _ = _make_processor(
        LONG_HISTORICAL_SOURCE,
        native=native,
        event_update=lambda msg: msg.update(create_time="2026-09-30 09:00:00"),
        config_update=lambda cfg: cfg["responsibility_rules"].pop("hazardous_waste", None),
    )
    plan = processor.process_batch([event])[0]
    assert plan["decision"] == "REVIEW"
    assert plan.get("route_owner") is None
    assert "current_area_owner" in plan["missing_fields"]
    assert plan.get("due_date") is None
    assert plan.get("due_iso") is None
    # Without verified current-area assignment, the date fallback is not opened.
    assert native.date_calls == 0

    native = _NativeHarness()
    processor, event, _ = _make_processor(
        "[文件] 危废处置记录.pdf fileId=RAW_FILE_1",
        native=native,
    )
    processor.process_batch([event])
    assert native.date_calls == 0

    native = _NativeHarness()
    processor, event, _ = _make_processor(
        "2024年3月1日已完成危废转运并归档相关资料。请各工班按照[附件]中的要求补录该批次联单信息。",
        native=native,
    )
    processor.process_batch([event])
    assert native.date_calls == 0

    native = _NativeHarness()
    processor, event, _ = _make_processor(
        LONG_HISTORICAL_SOURCE,
        native=native,
        event_update=lambda msg: msg.update(resource_refs=[{
            "resourceId": "RAW_FILE_REF_1", "resourceIdType": "fileId"
        }]),
    )
    processor.process_batch([event])
    assert native.date_calls == 0

    # The Jev-visible prefix is not sufficient if the original source exceeds
    # the independent, full-source bound of the fallback adapter.
    native = _NativeHarness()
    processor, event, _ = _make_processor(
        LONG_HISTORICAL_SOURCE + (" 附加原文" * 900),
        native=native,
    )
    plan = processor.process_batch([event])[0]
    assert plan["decision"] == "REVIEW"
    assert native.date_calls == 0


def test_causal_replay_aggregates_date_role_metrics_and_exposes_created_candidate():
    native = _NativeHarness()
    title = TitleFieldResolver(agent_factory=native.factory)
    jev_requests = []
    resolver = JevV2Resolver(
        new_policy=True,
        transport=_jev_transport(jev_requests),
    )
    config, self_user = _runtime()
    config["responsibility_rules"]["hazardous_waste"] = "苗笑天"
    payload = {
        "source_day": "2026-10-07",
        "config": config,
        "self_user": self_user,
        "business_candidate_query_complete": True,
        "existing_actions": [],
        "messages": [{
            "conversation_id": "c1", "message_id": "replay-source",
            "group": "物资调配室", "sender": "王萌", "sender_id": "OTHER",
            "create_time": "2026-10-07 09:00:00", "text": LONG_HISTORICAL_SOURCE,
        }, {
            "conversation_id": "c1", "message_id": "replay-followup",
            "group": "物资调配室", "sender": "王萌", "sender_id": "OTHER",
            "create_time": "2026-10-07 09:01:00",
            "text": "2024年3月1日已完成危废转运并归档相关资料。请各工班补录该批次联单信息并核对处置记录。",
        }],
    }
    date_fallback = DateRoleFallbackResolver(agent_factory=native.factory)

    result = replay_event_worker_day(
        payload,
        resolver=resolver,
        date_role_fallback_resolver=date_fallback,
        title_resolver=title,
    )

    assert [plan["decision"] for plan in result["plans"]] == ["CREATE", "CREATE"]
    assert result["simulated_ledger"][0]["message_id"] == "replay-source"
    assert result["summary"]["date_role_fallback_calls"] == 2
    assert result["summary"]["date_role_fallback_succeeded"] == 2
    assert result["summary"]["date_role_fallback_failed"] == 0
    assert result["summary"]["semantic_fallback_calls"] == 0
    assert result["summary"]["title_field_calls"] == 1
    assert result["summary"]["date_role_fallback_latency_ms_total"] >= 0
    second_state = json.loads(jev_requests[1]["state"])
    assert len(second_state["business_candidates"]) == 1
    assert LONG_HISTORICAL_SOURCE in second_state["business_candidates"][0]["text"]
