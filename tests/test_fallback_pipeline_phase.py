"""Staged semantic, date-role, and title handoff through real shadow components."""
from __future__ import annotations

import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from event_worker import EventProcessor, normalize_event
from jev_v2 import JevV2Resolver
from llm_adapter import DateRoleFallbackResolver, SemanticFallbackResolver, TitleFieldResolver
from replay import replay_event_worker_day
from test_jev_v2 import _runtime


FIRST_SOURCE = (
    "请各工班今日下班前对嘉祥处置联单信息进行重新录入更新。"
    "4点30分仍未更新的工班，将纳入本月考核项，谢谢配合。@工班长 \n"
    "空容器、滤芯第5次处置：联单编号：20264101142125，联单时间：2026年09月24日；\n"
    "废油、废油桶第7次处置：联单编号：20264101130304，联单时间：2026年08月14日"
)

SECOND_SOURCE = (
    "2026年9月22日至29日处置危废废物\n"
    "郑州市轨道交通2026-2027年度废油处置合同第9批处置，非郑东工班联单编号：20264101142065，"
    "联单时间：2026年9月24日；郑东工班联单编号：20264101143281，联单时间：2026年9月29日。\n"
    "郑州市轨道交通2026年危险废弃物（废空容器、废油滤芯等）处置合同第6批处置，"
    "非郑东工班联单编号：20264101142066，联单时间：2026年9月24日；"
    "郑东工班联单编号：20264101143286，联单时间：2026年9月29日。\n"
    "请各工班及时在系统中录入联单信息。@马明磊(马明磊) @邓淇轩(邓淇轩) "
    "@刘瑞强(刘瑞强) @刘显晖(刘显晖) @宋义昌(宋义昌) @赵海龙(赵海龙) @常俊(常俊)"
)

_SECOND_MARKER = "2026年9月22日至29日处置危废废物"


class _NativeResponses:
    def __init__(self, *, semantic_mode="different", date_mode="historical"):
        self.semantic_mode = semantic_mode
        self.date_mode = date_mode
        self.timeline: list[tuple[str, str]] = []
        self.inputs: list[tuple[str, dict]] = []

    def factory(self, **_kwargs):
        harness = self

        class Agent:
            def run_conversation(self, user_message, *, system_message, conversation_history):
                context = json.loads(user_message)
                if "same_work_candidates" in context:
                    source = context["text"]
                    harness.timeline.append(("semantic", source))
                    harness.inputs.append(("semantic", context))
                    if harness.semantic_mode == "invalid":
                        result = {
                            "applicability": "applicable",
                            "followup": "required",
                            "title_span": "title_0",
                            "due_key": "none",
                            "same_work": {
                                candidate["key"]: "different"
                                for candidate in context["same_work_candidates"]
                            },
                            "evidence_spans": ["请各工班及时在系统中录入联单信息"],
                            "decision": "CREATE",
                        }
                    else:
                        result = {
                            "applicability": (
                                "unknown" if harness.semantic_mode == "unknown" else "applicable"
                            ),
                            "followup": "required",
                            "title_span": "title_0",
                            "due_key": "none",
                            "same_work": {
                                candidate["key"]: "different"
                                for candidate in context["same_work_candidates"]
                            },
                            "evidence_spans": ["请各工班及时在系统中录入联单信息"],
                        }
                elif "unknown_date_candidates" in context:
                    source = context["source_text"]
                    harness.timeline.append(("date", source))
                    harness.inputs.append(("date", context))
                    rows = []
                    for candidate in context["unknown_date_candidates"]:
                        role = "historical" if harness.date_mode in {"historical", "invalid"} else "unknown"
                        evidence = candidate["source_span"]
                        if harness.date_mode == "invalid":
                            evidence = "不存在于日期原文中的证据"
                        rows.append({"key": candidate["key"], "role": role, "evidence": evidence})
                    result = {"roles": rows}
                else:
                    source = context["source_text"]
                    harness.timeline.append(("title", source))
                    harness.inputs.append(("title", context))
                    if "嘉祥处置联单信息" in source:
                        result = {
                            "title": "更新嘉祥处置联单信息",
                            "evidence_spans": ["嘉祥处置联单信息"],
                        }
                    else:
                        result = {
                            "title": "补录并核对危废联单信息",
                            "evidence_spans": [
                                "及时在系统中录入联单信息",
                                "郑州市轨道交通2026-2027年度废油处置合同",
                            ],
                        }
                return {"completed": True, "final_response": json.dumps(result, ensure_ascii=False)}

        return Agent()


def _transport(timeline, requests):
    def transport(request, _timeout):
        requests.append(request)
        state = json.loads(request["state"])
        text = state["text"]
        timeline.append(("jev", text))
        answers = {}
        for name, question in request["questions"].items():
            if question["type"] == "noul":
                if name.startswith("same_work_"):
                    value = 0.50
                else:
                    value = {
                        "applies_to_workgroup": 0.66,
                        "needs_follow_up": 0.93,
                        "prospective_action": 0.97,
                    }[name]
                answers[name] = {"type": "noul", "noul": value}
                continue

            options = question["criteria"]
            if name.startswith("date_role_"):
                selected, confidence = "historical", 0.55
            elif name == "due_date" and "date_0" in options and "嘉祥处置联单信息" in text:
                selected, confidence = "date_0", 0.99
            else:
                selected = "none" if "none" in options else next(iter(options))
                confidence = 0.99
            remainder = (1.0 - confidence) / (len(options) - 1)
            probabilities = {
                key: confidence if key == selected else remainder for key in options
            }
            answers[name] = {
                "type": "choice",
                "choice": selected,
                "probabilities": probabilities,
                "confidence": confidence,
            }
        return {"answers": answers}

    return transport


def _message(text, minute, *, message_id=None):
    return {
        "conversation_id": "isolated-replay-conversation",
        "message_id": message_id or hashlib.sha256(text.encode("utf-8")).hexdigest(),
        "group": "危废、废旧工作群",
        "sender": "来源发送人",
        "sender_id": "EXTERNAL_SOURCE",
        "create_time": f"2026-09-30 {minute}",
        "text": text,
    }


def _pipeline(*, native, tmp_path=None, query_complete=True):
    config, self_user = _runtime()
    config["config_group"][0]["group_name"] = "危废、废旧工作群"
    config["config_group"][0]["conversation_id"] = "isolated-replay-conversation"
    config["responsibility_rules"]["hazardous_waste"] = "杨梦卓"
    # This file exercises semantic/date/title handoffs. Give its September
    # synthetic messages a reviewed, synthetic B/F handover so those tests
    # reach the intended downstream stages under the confirmed routing gate.
    config["handover_evidence"] = [{
        "period": "2026-09", "area_name": "杂品库", "active_owners": ["杨梦卓"],
        "reviewed": True, "status": "accepted", "hygiene_review": "passed",
        "source": "dws_readback", "evidence_id": "synthetic-fallback-handover",
    }]
    requests = []
    resolver = JevV2Resolver(
        new_policy=True,
        transport=_transport(native.timeline, requests),
    )
    semantic = SemanticFallbackResolver(agent_factory=native.factory)
    date_role = DateRoleFallbackResolver(agent_factory=native.factory)
    title = TitleFieldResolver(agent_factory=native.factory)
    clarification_path = None
    if tmp_path is not None:
        clarification_path = tmp_path / "human-deadline.json"
        clarification_path.write_text(
            json.dumps(
                {
                    "kind": "human_deadline_clarification",
                    "scope": "one source obligation only; local evaluation evidence, not production activation or generalized message-ID policy",
                    "source_message_id": _message(FIRST_SOURCE, "13:11:29")["message_id"],
                    "source_time": "2026-09-30 13:11:29",
                    "source_evidence": "4点30分仍未更新的工班，将纳入本月考核项",
                    "user_confirmation": "应以16:30作为完成截止",
                    "confirmed_due_date": "2026-09-30",
                    "confirmed_due_iso": "2026-09-30T16:30:00+08:00",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
    payload = {
        "source_day": "2026-09-30",
        "config": config,
        "self_user": self_user,
        "business_candidate_query_complete": query_complete,
        "existing_actions": [],
        "messages": [
            _message(FIRST_SOURCE, "13:11:29"),
            _message(SECOND_SOURCE, "15:58:31"),
        ],
    }
    return payload, resolver, semantic, date_role, title, requests, clarification_path


def test_two_source_replay_hands_validated_semantics_through_dates_then_title(tmp_path):
    native = _NativeResponses()
    payload, resolver, semantic, date_role, title, requests, clarification = _pipeline(
        native=native, tmp_path=tmp_path
    )

    result = replay_event_worker_day(
        payload,
        resolver=resolver,
        semantic_fallback=semantic,
        date_role_fallback_resolver=date_role,
        title_resolver=title,
        deadline_clarifications_path=clarification,
    )

    assert [plan["decision"] for plan in result["plans"]] == ["CREATE", "CREATE"]
    first, second = result["plans"]
    assert first["route_owner"] == second["route_owner"] == "杨梦卓"
    assert first["due_iso"] == "2026-09-30T16:30:00+08:00"
    assert first["human_confirmation_evidence"]["provenance"] == "user_confirmation"
    assert second.get("due_date") is None
    assert second.get("due_iso") is None
    assert second["title"] == "补录并核对危废联单信息"
    assert second["semantic_evidence"]["date_role_judgments"][0]["resolved_role"] == "unknown"
    assert second["fallback_semantic_evidence"]["same_work"] == {"business_0": "different"}
    assert second["fallback_semantic_evidence"]["followup"] == "required"
    role = second["date_role_fallback_evidence"]["roles"][0]
    assert role["role"] == "historical"
    assert role["evidence"] in SECOND_SOURCE

    second_jev_state = json.loads(requests[1]["state"])
    assert len(second_jev_state["business_candidates"]) == 1
    assert "嘉祥处置联单信息" in second_jev_state["business_candidates"][0]["text"]
    stages = [
        stage
        for stage, source in native.timeline
        if _SECOND_MARKER in source
    ]
    assert stages == ["jev", "semantic", "date", "title"]
    stage_counts = Counter(stages)
    assert stage_counts == {"jev": 1, "semantic": 1, "date": 1, "title": 1}
    assert result["summary"]["semantic_fallback_calls"] == 1
    assert result["summary"]["date_role_fallback_calls"] == 1
    assert result["summary"]["title_field_calls"] == 2
    assert result["summary"]["writes_performed"] == 0
    assert result["summary"]["external_todos_created"] == 0


@pytest.mark.parametrize("semantic_mode", ["unknown", "invalid"])
def test_unaccepted_semantics_do_not_call_date_or_title_resolvers(semantic_mode):
    native = _NativeResponses(semantic_mode=semantic_mode)
    payload, resolver, semantic, date_role, title, _, _ = _pipeline(native=native)

    processor = EventProcessor(
        payload["config"],
        payload["self_user"],
        mode="shadow",
        llm_resolver=resolver,
        semantic_fallback=semantic,
        date_role_fallback_resolver=date_role,
        title_resolver=title,
        business_candidate_query_complete=True,
        existing_actions=[
            {
                "conversation_id": payload["messages"][0]["conversation_id"],
                "message_id": payload["messages"][0]["message_id"],
                "business_key": "prior-business",
                "status": "SUCCESS",
                "simulation_id": "simulated:prior",
                "source_text": FIRST_SOURCE,
                "message_time": "2026-09-30 13:11:29",
                "route_module": "hazardous_waste",
                "route_owner": "杨梦卓",
                "simulated": True,
            }
        ],
        allow_simulated_candidates=True,
    )
    plan = processor.process_batch([payload["messages"][1]])[0]

    assert plan["decision"] == "REVIEW"
    if semantic_mode == "unknown":
        assert processor.stats["semantic_fallback_succeeded"] == 1
        assert processor.stats["semantic_fallback_failed"] == 0
    else:
        assert processor.stats["semantic_fallback_succeeded"] == 0
        assert processor.stats["semantic_fallback_failed"] == 1
    assert processor.stats["date_role_fallback_calls"] == 0
    assert processor.stats["title_field_calls"] == 0
    assert not [row for row in native.timeline if row[0] in {"date", "title"}]


@pytest.mark.parametrize("date_mode", ["unknown", "invalid"])
def test_unresolved_or_invalid_date_role_does_not_call_title(date_mode):
    native = _NativeResponses(date_mode=date_mode)
    payload, resolver, semantic, date_role, title, _, _ = _pipeline(native=native)
    processor = EventProcessor(
        payload["config"],
        payload["self_user"],
        mode="shadow",
        llm_resolver=resolver,
        semantic_fallback=semantic,
        date_role_fallback_resolver=date_role,
        title_resolver=title,
        business_candidate_query_complete=True,
        existing_actions=[
            {
                "conversation_id": payload["messages"][0]["conversation_id"],
                "message_id": payload["messages"][0]["message_id"],
                "business_key": "prior-business",
                "status": "SUCCESS",
                "simulation_id": "simulated:prior",
                "source_text": FIRST_SOURCE,
                "message_time": "2026-09-30 13:11:29",
                "route_module": "hazardous_waste",
                "route_owner": "杨梦卓",
                "simulated": True,
            }
        ],
        allow_simulated_candidates=True,
    )
    plan = processor.process_batch([payload["messages"][1]])[0]

    assert plan["decision"] == "REVIEW"
    assert plan.get("due_date") is None
    assert processor.stats["semantic_fallback_calls"] == 1
    assert processor.stats["date_role_fallback_calls"] == 1
    if date_mode == "unknown":
        assert processor.stats["date_role_fallback_succeeded"] == 1
        assert processor.stats["date_role_fallback_failed"] == 0
    else:
        assert processor.stats["date_role_fallback_succeeded"] == 0
        assert processor.stats["date_role_fallback_failed"] == 1
    assert processor.stats["title_field_calls"] == 0
    assert not [row for row in native.timeline if row[0] == "title"]


def test_unknown_business_pool_stays_review_without_date_or_title_synthesis():
    native = _NativeResponses()
    payload, resolver, semantic, date_role, title, _, _ = _pipeline(
        native=native, query_complete=False
    )
    processor = EventProcessor(
        payload["config"],
        payload["self_user"],
        mode="shadow",
        llm_resolver=resolver,
        semantic_fallback=semantic,
        date_role_fallback_resolver=date_role,
        title_resolver=title,
        business_candidate_query_complete=False,
        existing_actions=[],
    )

    plan = processor.process_batch([payload["messages"][1]])[0]

    assert plan["decision"] == "REVIEW"
    assert processor.stats["semantic_fallback_calls"] == 0
    assert processor.stats["date_role_fallback_calls"] == 0
    assert processor.stats["title_field_calls"] == 0
    assert processor.stats["llm_calls"] == 0


def test_incoming_private_pipeline_flags_are_removed_from_normalized_raw_payload():
    payload = {
        **_message(SECOND_SOURCE, "15:58:31"),
        "_jev_v2_authorized_shadow_create": True,
        "_due_date_selection_resolved": True,
        "_llm_semantic_intent": "follow_up",
        "_jev_v2_applicable": True,
        "_jev_v2_title_verified": True,
    }

    event = normalize_event(payload)

    serialized = json.dumps(event["raw_payload"], ensure_ascii=False)
    for flag in (
        "_jev_v2_authorized_shadow_create",
        "_due_date_selection_resolved",
        "_llm_semantic_intent",
        "_jev_v2_applicable",
        "_jev_v2_title_verified",
    ):
        assert flag not in serialized
