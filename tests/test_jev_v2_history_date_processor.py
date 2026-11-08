"""Processor acceptance for labeled history dates and competing cutoff signals."""
from __future__ import annotations

import json

from event_worker import EventProcessor
from jev_v2 import JevV2Resolver
from llm_adapter import TitleFieldResolver
from routing import has_competing_assessment_cutoff
from test_jev_v2 import _runtime


HISTORY_SOURCE = (
    "请各工班及时将本次危险废物处置联单信息录入系统，并逐项核对每类包装物、容器、"
    "废弃滤芯等物项的规格、数量、去向和现场记录，确保新增记录与本次实际处置过程一致，"
    "所有必填内容完整、准确、可查询，完成后向工作群反馈核对情况。"
    "历史字段：联单时间：2024年06月03日；处置日期：2025年04月22日；"
    "另一条历史字段：联单时间：2027年02月09日。"
)
TITLE = "本次危险废物处置联单信息录入系统"
TITLE_EVIDENCE = "将本次危险废物处置联单信息录入系统"


def _processor(
    source,
    *,
    choice="date_0",
    role_judgment=None,
    role_confidence=0.99,
    omit_role_judgment=False,
    create_time="2026-10-07 09:00:00",
    title_resolver=None,
    configure_hazard_owner=True,
    event_overrides=None,
):
    config, self_user = _runtime()
    configured_owner = config["config_staff"][1][1]
    if configure_hazard_owner:
        config["responsibility_rules"]["hazardous_waste"] = configured_owner
    else:
        config["responsibility_rules"].pop("hazardous_waste", None)
    requests = []

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
                state = json.loads(request["state"])
                is_role = name.startswith("date_role_")
                if is_role:
                    if omit_role_judgment:
                        continue
                    key = name.removeprefix("date_role_")
                    candidate = next(row for row in state["date_role_candidates"] if row["key"] == key)
                    selected = role_judgment(candidate, state) if callable(role_judgment) else (role_judgment or "unknown")
                else:
                    selected = choice(state) if callable(choice) else choice
                assert selected in question["criteria"]
                rest = [key for key in question["criteria"] if key != selected]
                probabilities = {selected: 0.99}
                probabilities.update({key: 0.01 / len(rest) for key in rest})
                answers[name] = {
                    "type": "choice",
                    "choice": selected,
                    "probabilities": probabilities,
                    "confidence": role_confidence if is_role else 0.99,
                }
            else:
                raise AssertionError(f"unexpected typed question: {question['type']}")
        return {"answers": answers}

    processor = EventProcessor(
        config,
        self_user,
        mode="shadow",
        llm_resolver=JevV2Resolver(new_policy=True, transport=transport),
        title_resolver=title_resolver,
        existing_actions=[],
        business_candidate_query_complete=True,
    )
    event = {
        "conversation_id": "c1",
        "message_id": "history-date-acceptance",
        "group": "物资调配室",
        "sender": "sender fixture",
        "sender_id": "fixture-sender",
        "create_time": create_time,
        "text": source,
        **(event_overrides or {}),
    }
    return processor, processor.process_batch([event])[0], requests, configured_owner


def _title_resolver(calls):
    class FakeNativeAgent:
        def run_conversation(self, user_message, **kwargs):
            calls.append((user_message, kwargs))
            return {
                "completed": True,
                "final_response": json.dumps(
                    {"title": TITLE, "evidence_spans": [TITLE_EVIDENCE]},
                    ensure_ascii=False,
                ),
            }

    return TitleFieldResolver(agent_factory=lambda **options: FakeNativeAgent())


def test_labeled_history_dates_do_not_block_undated_record_entry_or_title_compilation():
    native_calls = []
    processor, plan, requests, configured_owner = _processor(
        HISTORY_SOURCE,
        title_resolver=_title_resolver(native_calls),
    )

    assert len(requests) == 1
    date_options = requests[0]["questions"]["due_date"]["criteria"]
    assert set(date_options) == {"date_0", "date_1", "date_2", "none", "uncertain"}
    assert all("历史" in date_options[key] for key in ("date_0", "date_1", "date_2"))

    assert len(native_calls) == 1
    prompt = json.loads(native_calls[0][0])
    assert prompt["source_text"].endswith("另一条历史字段：联单时间：2027年02月09日。")
    assert TITLE_EVIDENCE in prompt["source_text"]
    assert plan["decision"] == "CREATE"
    assert plan["route_module"] == "hazardous_waste"
    assert plan["route_owner"] == configured_owner
    assert plan.get("due_date") is None
    assert plan.get("due_iso") is None
    assert plan["title"] == TITLE
    assert plan["semantic_evidence"]["due_date_choice"] == "date_0"
    assert plan["semantic_evidence"]["due_date_basis"] == "historical_dates_not_deadlines"
    assert HISTORY_SOURCE in plan["detail"]
    assert processor.stats["title_field_calls"] == 1
    assert processor.stats["title_field_succeeded"] == 1
    assert processor.stats["title_field_failed"] == 0


def test_competing_assessment_time_reviews_cutoff_and_keeps_configured_hazard_owner():
    source = (
        "请各工班今天下班前完成危险废物处置联单信息更新。"
        "4点30分仍未更新的工班，将纳入本月考核。"
        "历史字段：联单时间：2026年09月28日；处置日期：2026年09月29日。"
    )
    _, plan, requests, configured_owner = _processor(source, choice="date_0")

    criteria = requests[0]["questions"]["due_date"]["criteria"]
    assert "下班前" in criteria["date_0"] or "截止日期" in criteria["date_0"]
    assert "历史" in criteria["date_1"]
    assert "历史" in criteria["date_2"]
    assert plan["decision"] == "REVIEW"
    assert plan["reason"] == "jev_v2_competing_deadline_context"
    assert plan["route_module"] == "hazardous_waste"
    assert plan["route_owner"] == configured_owner
    assert plan.get("due_date") is None
    assert plan.get("due_iso") is None


def test_unlabeled_candidate_stays_review_while_labeled_history_does_not_become_due():
    source = (
        "请各工班按本次危险废物处置情况及时将新增联单资料录入系统，核对关键信息并完成完整记录。"
        "相关处置事项发生于2026年09月28日。"
        "历史字段：联单时间：2026年09月21日；处置日期：2026年09月22日。"
    )
    _, plan, requests, configured_owner = _processor(source, choice="date_0")

    criteria = requests[0]["questions"]["due_date"]["criteria"]
    assert "用途" in criteria["date_0"]
    assert "历史" in criteria["date_1"]
    assert "历史" in criteria["date_2"]
    assert plan["decision"] == "REVIEW"
    assert plan["reason"] == "jev_v2_date_role_unverified"
    assert plan["route_module"] == "hazardous_waste"
    assert plan["route_owner"] == configured_owner
    assert plan.get("due_date") is None
    assert plan.get("due_iso") is None


def test_unknown_past_disposal_interval_is_judged_historical_and_creates_undated_hazard_task():
    source = (
        "请各工班及时将本次危险废物处置联单信息录入系统，并逐项核对每类包装物、容器、废弃滤芯等物项的规格、数量、去向和现场记录，"
        "确保新增记录与本次实际处置过程一致，所有必填内容完整、准确、可查询，完成后向工作群反馈核对情况。"
        "相关处置区间为2026年09月22日至29日处置危废废物，mediaId=@SECRET_MEDIA_8831，请@王某确认。"
    )
    native_calls = []
    processor, plan, requests, configured_owner = _processor(
        source,
        role_judgment=lambda candidate, state: "historical" if "处置区间" in candidate["source_span"] else "unknown",
        title_resolver=_title_resolver(native_calls),
    )

    assert len(requests) == 1
    request = requests[0]
    state = json.loads(request["state"])
    assert "date_role_date_0" in request["questions"]
    assert set(request["questions"]["date_role_date_0"]["criteria"]) == {
        "deadline", "effective", "historical", "event", "unknown"
    }
    role_candidates = state["date_role_candidates"]
    assert len(role_candidates) == 1
    assert set(role_candidates[0]) == {"key", "source_span"}
    assert role_candidates[0]["key"] == "date_0"
    assert "2026年09月22日至29日处置危废废物" in role_candidates[0]["source_span"]
    assert "相关处置区间" in state["text"]
    serialized = json.dumps(request, ensure_ascii=False)
    assert "SECRET_MEDIA_8831" not in serialized
    assert "王某" not in serialized
    assert "[RESOURCE_ID]" in serialized
    assert "[PERSON_MENTION]" in serialized
    assert "history-date-acceptance" not in serialized

    assert len(native_calls) == 1
    assert plan["decision"] == "CREATE"
    assert plan["route_module"] == "hazardous_waste"
    assert plan["route_owner"] == configured_owner
    assert plan.get("due_date") is None
    assert plan.get("due_iso") is None
    assert plan["title"] == TITLE
    assert plan["semantic_evidence"]["date_role_judgments"][0]["parser_role"] == "unknown"
    assert plan["semantic_evidence"]["date_role_judgments"][0]["resolved_role"] == "historical"
    assert plan["semantic_evidence"]["date_role_judgments"][0]["role_origin"] == "jev_v2"
    assert source in plan["detail"]
    assert processor.stats["llm_calls"] == 1
    assert processor.stats["title_field_calls"] == 1
    assert processor.stats["title_field_succeeded"] == 1


def test_unknown_date_role_missing_low_confidence_or_unknown_stays_review():
    source = (
        "请各工班按本次危险废物处置情况及时将新增联单资料录入系统。"
        "相关处置事项发生于2026年09月28日。"
    )
    missing = _processor(source, omit_role_judgment=True)
    assert missing[1]["decision"] == "REVIEW"
    assert missing[1]["reason"] == "jev_v2_resolution_failed"

    low_confidence = _processor(source, role_judgment="historical", role_confidence=0.60)
    assert low_confidence[1]["decision"] == "REVIEW"
    assert low_confidence[1]["reason"] == "jev_v2_date_role_unverified"

    unknown = _processor(source, role_judgment="unknown")
    assert unknown[1]["decision"] == "REVIEW"
    assert unknown[1]["reason"] == "jev_v2_date_role_unverified"
    for _, plan, requests, configured_owner in (missing, low_confidence, unknown):
        assert plan.get("due_date") is None
        assert plan.get("due_iso") is None
        assert len(requests) == 1
    assert missing[1]["missing_fields"] and "due_date" in missing[1]["missing_fields"]
    for _, plan, _, configured_owner in (low_confidence, unknown):
        assert plan["route_owner"] == configured_owner


def test_hostile_history_role_cannot_replace_known_eleven_am_feedback_deadline():
    source = (
        "请各工班于2026年10月07日11点前完成反馈。"
        "另有记录日期2026年10月06日，需核对历史资料。"
    )
    _, plan, requests, _ = _processor(
        source,
        choice="none",
        role_judgment="historical",
    )
    questions = requests[0]["questions"]
    assert "date_role_date_0" not in questions
    assert "date_role_date_1" in questions
    assert plan["decision"] == "CREATE"
    assert plan["due_date"] == "2026-10-07"
    assert plan["due_iso"] == "2026-10-07T11:00:00+08:00"
    evidence = plan["semantic_evidence"]["date_role_judgments"]
    assert next(item for item in evidence if item["candidate_key"] == "date_1")["resolved_role"] == "historical"


def test_overlong_source_is_reviewed_without_provider_truncation():
    source = (
        "请各工班及时录入危险废物处置联单信息。"
        + "补充记录" * 1100
        + "相关处置事项发生于2026年09月28日。"
    )
    _, plan, requests, _ = _processor(source, choice="date_0")

    assert requests == []
    assert plan["decision"] == "REVIEW"
    assert plan["reason"] == "jev_v2_source_incomplete"
    assert plan["jev_error_code"] == "jev_v2_source_incomplete"
    assert plan.get("due_date") is None
    assert plan.get("due_iso") is None


def test_date_review_uses_confirmed_month_anchor_when_legacy_config_has_no_hazard_owner():
    source = (
        "请各工班今天下班前完成危险废物处置联单信息更新。"
        "4点30分仍未更新的工班，将纳入本月考核。"
    )
    _, plan, _, _ = _processor(source, configure_hazard_owner=False)

    assert plan["decision"] == "REVIEW"
    assert plan["route_module"] == "hazardous_waste"
    assert plan.get("route_owner") == "苗笑天"
    assert plan.get("matrix_route_owner") == "杨梦卓"
    assert plan["assignment_audit"]["handover_source"] == "user_confirmed_month_anchor"
    assert "route_owner" not in plan["missing_fields"]
    assert plan["assignment_audit"]["handover_source"] == "user_confirmed_month_anchor"


def test_competing_cutoff_detection_requires_unfinished_work_and_assessment_link():
    assert has_competing_assessment_cutoff(
        "4点30分仍未更新的工班，将纳入本月考核。"
    )
    assert not has_competing_assessment_cutoff(
        "4点30分召开月度考核会议，今天下班前完成联单更新。"
    )
