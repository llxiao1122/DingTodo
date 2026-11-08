"""Date-role acceptance through the real Jev v2 EventProcessor path."""
from __future__ import annotations

import json

from event_worker import EventProcessor
from jev_v2 import JevV2Resolver
from test_jev_v2 import _runtime


def _processor(text, *, choice, role_choice="unknown", create_time="2026-10-07 09:00:00", followup=0.93, fallback=None):
    config, self_user = _runtime()
    requests = []

    def transport(request, timeout):
        requests.append(request)
        answers = {}
        for name, question in request["questions"].items():
            if question["type"] == "noul":
                value = {
                    "applies_to_workgroup": 0.64,
                    "needs_follow_up": followup,
                    "prospective_action": 0.99,
                }.get(name, 0.99)
                answers[name] = {"type": "noul", "noul": value}
            elif question["type"] == "choice":
                options = question["criteria"]
                state = json.loads(request["state"])
                if name.startswith("date_role_"):
                    key = name.removeprefix("date_role_")
                    candidate = next(row for row in state["date_role_candidates"] if row["key"] == key)
                    selected = role_choice(candidate, state) if callable(role_choice) else role_choice
                else:
                    selected = choice(state) if callable(choice) else choice
                assert selected in options
                rest = [key for key in options if key != selected]
                probabilities = {selected: 0.99}
                probabilities.update({key: 0.01 / len(rest) for key in rest})
                answers[name] = {
                    "type": "choice",
                    "choice": selected,
                    "probabilities": probabilities,
                    "confidence": 0.99,
                }
            else:
                raise AssertionError(f"unexpected typed question: {question['type']}")
        return {"answers": answers}

    processor = EventProcessor(
        config,
        self_user,
        mode="shadow",
        llm_resolver=JevV2Resolver(new_policy=True, transport=transport),
        semantic_fallback=fallback,
        existing_actions=[],
        business_candidate_query_complete=True,
    )
    event = {
        "conversation_id": "c1",
        "message_id": "date-role",
        "group": "物资调配室",
        "sender": "王萌",
        "sender_id": "OTHER",
        "create_time": create_time,
        "text": text,
    }
    plans = processor.process_batch([event])
    return processor, plans[0], requests


class _Fallback:
    def __init__(self, due_key):
        self.due_key = due_key
        self.calls = []
        self.last_title_candidates = []

    def resolve_context(self, event, plan, *, date_candidates=(), business_candidates=()):
        self.calls.append((event, plan, list(date_candidates), list(business_candidates)))
        self.last_title_candidates = [{"key": "title_0", "text": plan.get("title") or event["text"]}]
        return {
            "applicability": "applicable",
            "followup": "required",
            "title_span": "title_0",
            "due_key": self.due_key,
            "same_work": {},
            "evidence_spans": [event["text"]],
        }


def test_direct_jev_effective_only_date_keeps_followup_without_a_deadline():
    source = "根据市局通知，2026年10月07日18时终止当前防汛值守。请各工班按新安排完成值守衔接和人员确认。"
    processor, plan, requests = _processor(source, choice="date_0")

    assert json.loads(requests[0]["state"])["date_candidates"] == [{
        "key": "date_0", "due_date": "2026-10-07", "due_iso": "2026-10-07T18:00:00+08:00",
    }]
    date_question = requests[0]["questions"]["due_date"]
    assert "date_0" in date_question["criteria"]
    assert "生效" in date_question["criteria"]["date_0"]
    assert "不是任务截止日期" in date_question["criteria"]["date_0"]
    assert plan["decision"] == "CREATE"
    assert plan["semantic_evidence"]["needs_follow_up"] == 0.93
    assert plan.get("due_date") is None
    assert plan.get("due_iso") is None
    assert processor.stats["create"] == 1


def test_jev_none_cannot_drop_a_single_explicit_feedback_deadline():
    source = "请各工班于2026年10月07日11:00前完成反馈。"
    _, plan, requests = _processor(source, choice="none")

    assert json.loads(requests[0]["state"])["date_candidates"][0]["due_date"] == "2026-10-07"
    assert plan["decision"] == "CREATE"
    assert plan["due_date"] == "2026-10-07"
    assert plan["due_iso"] == "2026-10-07T11:00:00+08:00"


def test_mixed_jev_effective_selection_uses_only_the_unambiguous_feedback_deadline():
    source = (
        "根据市局通知，2026年10月08日18时起暂停当前防汛值守安排。"
        "请各工班于2026年10月07日17时前反馈调整人员清单并完成新安排衔接。"
    )
    _, plan, requests = _processor(
        source,
        choice=lambda state: "date_0" if state["date_candidates"][0]["due_date"] == "2026-10-08" else "none",
        create_time="2026-10-01 09:00:00",
    )

    candidates = json.loads(requests[0]["state"])["date_candidates"]
    assert [candidate["due_date"] for candidate in candidates] == ["2026-10-08", "2026-10-07"]
    assert plan["decision"] == "CREATE"
    assert plan["due_date"] == "2026-10-07"
    assert plan["due_iso"] == "2026-10-07T17:00:00+08:00"
    assert plan["semantic_evidence"]["due_date_choice"] == "date_0"


def test_jev_none_with_multiple_source_deadlines_stays_review():
    source = (
        "请各工班于2026年10月07日17时前反馈名单。"
        "请各工班于2026年10月08日18时前完成调整。"
    )
    _, plan, requests = _processor(
        source,
        choice="none",
        create_time="2026-10-01 09:00:00",
    )

    candidates = json.loads(requests[0]["state"])["date_candidates"]
    assert [candidate["due_date"] for candidate in candidates] == ["2026-10-07", "2026-10-08"]
    assert plan["decision"] == "REVIEW"
    assert plan["reason"] == "jev_v2_multiple_explicit_deadlines"
    assert plan.get("due_date") is None


def test_jev_cannot_choose_between_multiple_source_deadlines():
    source = (
        "请各工班于2026年10月07日17时前反馈名单。"
        "请各工班于2026年10月08日18时前完成调整。"
    )
    _, plan, requests = _processor(
        source,
        choice="date_0",
        create_time="2026-10-01 09:00:00",
    )

    assert len(json.loads(requests[0]["state"])["date_candidates"]) == 2
    assert plan["decision"] == "REVIEW"
    assert plan["reason"] == "jev_v2_multiple_explicit_deadlines"
    assert plan.get("due_date") is None


def test_direct_jev_historical_unknown_date_stays_review_without_due_date():
    source = "请各工班核对历史防汛记录（2026年09月28日），并完成后续清单复核。"
    _, plan, requests = _processor(source, choice="date_0")

    candidate = json.loads(requests[0]["state"])["date_candidates"][0]
    assert candidate["due_date"] == "2026-09-28"
    assert plan["decision"] == "REVIEW"
    assert plan["reason"] == "jev_v2_date_role_unverified"
    assert plan.get("due_date") is None
    assert "due_date" in plan["missing_fields"]


def test_direct_jev_today_eleven_before_feedback_keeps_the_source_deadline():
    source = "请各工班今天上午11点前完成安全巡查反馈。"
    _, plan, requests = _processor(source, choice="date_0")

    candidate = json.loads(requests[0]["state"])["date_candidates"][0]
    assert candidate["due_date"] == "2026-10-07"
    assert candidate["due_iso"] == "2026-10-07T11:00:00+08:00"
    assert plan["decision"] == "CREATE"
    assert plan["due_date"] == "2026-10-07"
    assert plan["due_iso"] == "2026-10-07T11:00:00+08:00"


def test_confident_semantic_deadline_resolves_only_the_program_owned_date_candidate():
    source = "请各工班于2026年10月08日完成库存核对方案整理。"
    _, plan, requests = _processor(
        source,
        choice="date_0",
        role_choice="deadline",
        create_time="2026-10-01 09:00:00",
    )

    state = json.loads(requests[0]["state"])
    assert state["date_candidates"] == [{
        "key": "date_0",
        "due_date": "2026-10-08",
        "due_iso": "2026-10-08T18:00:00+08:00",
    }]
    assert requests[0]["questions"]["date_role_date_0"]["criteria"].keys() == {
        "deadline", "effective", "historical", "event", "unknown"
    }
    assert plan["decision"] == "CREATE"
    assert plan["due_date"] == "2026-10-08"
    assert plan["due_iso"] == "2026-10-08T18:00:00+08:00"
    role_evidence = plan["semantic_evidence"]["date_role_judgments"][0]
    assert role_evidence["parser_role"] == "unknown"
    assert role_evidence["resolved_role"] == "deadline"
    assert role_evidence["role_origin"] == "jev_v2"


def test_semantic_fallback_none_does_not_silence_unknown_historical_date():
    source = "请各工班核对历史防汛记录（2026年09月28日），并完成后续清单复核。"
    fallback = _Fallback("none")
    processor, plan, _ = _processor(
        source,
        choice="uncertain",
        followup=0.5,
        fallback=fallback,
    )

    assert len(fallback.calls) == 1
    assert fallback.calls[0][2][0]["role"] == "unknown"
    assert plan["decision"] == "REVIEW"
    assert plan["reason"] in {"deadline_selection_unresolved", "jev_v2_date_role_unverified"}
    assert plan.get("due_date") is None
    assert processor.stats["semantic_fallback_calls"] == 1


def test_semantic_fallback_none_keeps_single_explicit_feedback_deadline():
    source = "请各工班于2026年10月07日11:00前完成反馈。"
    fallback = _Fallback("none")
    _, plan, _ = _processor(
        source,
        choice="uncertain",
        followup=0.5,
        fallback=fallback,
    )

    assert len(fallback.calls) == 1
    assert plan["decision"] == "CREATE"
    assert plan["due_date"] == "2026-10-07"
    assert plan["due_iso"] == "2026-10-07T11:00:00+08:00"


def test_semantic_fallback_effective_time_agrees_with_direct_jev_no_due_date():
    source = (
        "根据市局通知，2026年10月07日18时终止当前防汛值守。"
        "请各工班按新安排完成值守衔接和人员确认。"
    )
    fallback = _Fallback("unknown")
    processor, plan, _ = _processor(
        source,
        choice="uncertain",
        followup=0.5,
        fallback=fallback,
    )

    assert len(fallback.calls) == 1
    assert fallback.calls[0][2][0]["role"] == "effective"
    assert plan["decision"] == "CREATE"
    assert plan.get("due_date") is None
    assert plan.get("due_iso") is None
    assert processor.stats["create"] == 1


def test_semantic_fallback_cannot_select_unknown_historical_date_as_deadline():
    source = "请各工班核对历史防汛记录（2026年09月28日），并完成后续清单复核。"
    fallback = _Fallback("date_0")
    _, plan, _ = _processor(
        source,
        choice="uncertain",
        followup=0.5,
        fallback=fallback,
    )

    assert len(fallback.calls) == 1
    assert plan["decision"] == "REVIEW"
    assert plan["reason"] in {"deadline_selection_unresolved", "jev_v2_date_role_unverified"}
    assert plan.get("due_date") is None
