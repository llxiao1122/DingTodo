"""Review proposal fields stay tied to program and source evidence."""
from __future__ import annotations

import json

import pytest

from event_worker import EventProcessor
from jev_v2 import JevV2Resolver
from llm_adapter import LlmError
from test_jev_v2 import _runtime


HISTORICAL_SOURCE = (
    "2026年9月22日至29日处置危废废物。请各工班及时在系统中录入联单信息。"
)
PRIOR_TEXT = "请各工班更新危险废物联单信息并完成系统录入。"
CONFIGURED_OWNER = "杨梦卓"


def _typed_transport(*, same_work=0.5, scope=0.99, followup=0.99, invalid_due_choice=False):
    calls = []

    def transport(request, _timeout):
        calls.append(request)
        answers = {}
        for name, question in request["questions"].items():
            if question["type"] == "noul":
                value = {
                    "applies_to_workgroup": scope,
                    "needs_follow_up": followup,
                    "prospective_action": 0.99,
                }.get(name, same_work if name.startswith("same_work_") else 0.99)
                answers[name] = {"type": "noul", "noul": value}
                continue
            options = question["criteria"]
            if name.startswith("date_role_"):
                choice = "unknown"
            elif name == "due_date" and invalid_due_choice:
                choice = "date_outside_candidates"
            else:
                choice = "none" if "none" in options else next(iter(options))
            remaining = [key for key in options if key != choice]
            probabilities = {choice: 0.99}
            if remaining:
                probabilities.update({key: 0.01 / len(remaining) for key in remaining})
            answers[name] = {
                "type": "choice",
                "choice": choice,
                "probabilities": probabilities,
                "confidence": 0.99,
            }
        return {"answers": answers}

    return transport, calls


def _prior_action():
    return {
        "business_key": "prior-hazard-work",
        "route_module": "hazardous_waste",
        "route_owner": CONFIGURED_OWNER,
        "status": "SUCCESS",
        "simulated": True,
        "simulation_id": "simulated:prior-hazard-work",
        "message_time": "2026-09-30 13:11:29",
        "source_text": PRIOR_TEXT,
    }


def _event(text=HISTORICAL_SOURCE, *, message_id="review-source", create_time="2026-09-30 15:58:31"):
    return {
        "conversation_id": "c1",
        "message_id": message_id,
        "group": "物资调配室",
        "sender": "sender fixture",
        "sender_id": "OTHER",
        "create_time": create_time,
        "text": text,
    }


class _FailingFallback:
    max_source_chars = 4000

    def __init__(self, error="codex_runtime_failed:PRIVATE_RESPONSE @PrivateName"):
        self.error = error
        self.calls = []

    def resolve_context(self, event, plan, *, date_candidates=(), business_candidates=()):
        self.calls.append((event, plan, list(date_candidates), list(business_candidates)))
        raise LlmError(self.error)


def _processor(*, transport=None, fallback=None, existing_actions=None, configure_owner=True, **options):
    config, self_user = _runtime()
    if configure_owner:
        config["responsibility_rules"]["hazardous_waste"] = CONFIGURED_OWNER
        config["handover_evidence"] = [{
            "period": "2026-09", "area_name": "杂品库", "active_owners": [CONFIGURED_OWNER],
            "reviewed": True, "status": "accepted", "hygiene_review": "passed",
            "source": "dws_readback", "evidence_id": "synthetic-review-handover",
        }]
    else:
        config["responsibility_rules"].pop("hazardous_waste", None)
    if transport is None:
        transport, _ = _typed_transport()
    processor = EventProcessor(
        config,
        self_user,
        mode="shadow",
        llm_resolver=JevV2Resolver(new_policy=True, transport=transport),
        semantic_fallback=fallback,
        existing_actions=list(existing_actions or []),
        business_candidate_query_complete=True,
        allow_simulated_candidates=bool(existing_actions),
        **options,
    )
    return processor, config, self_user


def test_business_uncertain_fallback_failure_keeps_review_reason_owner_and_no_historical_due():
    transport, jev_calls = _typed_transport()
    fallback = _FailingFallback()
    processor, _config, _self_user = _processor(
        transport=transport,
        fallback=fallback,
        existing_actions=[_prior_action()],
    )

    plan = processor.process_batch([_event()])[0]

    assert len(jev_calls) == 1
    assert processor.stats["llm_calls"] == 1
    assert processor.stats["semantic_fallback_calls"] == 1
    assert processor.stats["semantic_fallback_succeeded"] == 0
    assert processor.stats["semantic_fallback_failed"] == 1
    assert plan["decision"] == "REVIEW"
    assert plan["reason"] == "jev_v2_business_match_uncertain"
    assert plan["route_module"] == "hazardous_waste"
    assert plan["route_owner"] == CONFIGURED_OWNER
    assert plan["matrix_route_owner"] == CONFIGURED_OWNER
    assert plan["executor_ids"] == []
    assert plan["executor_names"] == []
    assert plan.get("due_date") is None
    assert plan.get("due_iso") is None
    assert plan["due_role_unresolved"] is True
    assert "due_date" in plan["missing_fields"]
    assert HISTORICAL_SOURCE in plan["detail"]
    assert len(fallback.calls[0][3]) == 1
    assert fallback.calls[0][3][0]["text"] == PRIOR_TEXT

    assert plan["semantic_fallback_error"] == {
        "code": "codex_runtime_failed",
        "stage": "transport",
    }
    diagnostic = json.dumps(plan["semantic_fallback_error"], ensure_ascii=False)
    assert "PRIVATE_RESPONSE" not in diagnostic
    assert "PrivateName" not in diagnostic


def test_valid_different_fallback_keeps_only_the_unresolved_date_gate():
    transport, jev_calls = _typed_transport()

    class ValidDifferentFallback:
        max_source_chars = 4000

        def __init__(self):
            self.last_title_candidates = []

        def resolve_context(self, event, plan, *, date_candidates=(), business_candidates=()):
            self.last_title_candidates = [{"key": "title_0", "text": plan.get("title") or event["text"]}]
            return {
                "applicability": "applicable",
                "followup": "required",
                "title_span": "title_0",
                "due_key": "unknown",
                "same_work": {candidate["key"]: "different" for candidate in business_candidates},
                "evidence_spans": ["请各工班及时在系统中录入联单信息"],
            }

    fallback = ValidDifferentFallback()
    processor, _config, _self_user = _processor(
        transport=transport,
        fallback=fallback,
        existing_actions=[_prior_action()],
    )

    plan = processor.process_batch([_event()])[0]

    assert len(jev_calls) == 1
    assert processor.stats["semantic_fallback_calls"] == 1
    assert processor.stats["semantic_fallback_succeeded"] == 1
    assert processor.stats["semantic_fallback_failed"] == 0
    assert plan["decision"] == "REVIEW"
    assert plan["reason"] == "jev_v2_date_role_unverified"
    assert plan["due_selection_error"] == "jev_v2_date_role_unverified"
    assert plan["due_role_unresolved"] is True
    assert plan.get("due_date") is None
    assert plan["route_owner"] == CONFIGURED_OWNER
    assert plan["fallback_semantic_evidence"]["same_work"] == {"business_0": "different"}


@pytest.mark.parametrize(
    "response_update,expected_error",
    [
        ({"route_owner": "伪造责任人"}, {"code": "semantic_fallback_schema_invalid", "stage": "schema"}),
        ({"due_key": "date_outside_candidates"}, {"code": "semantic_fallback_due_key_invalid", "stage": "validation"}),
    ],
)
def test_semantic_fallback_cannot_choose_owner_or_due_outside_supplied_candidates(response_update, expected_error):
    transport, _jev_calls = _typed_transport()

    class InvalidFallback:
        max_source_chars = 4000

        def __init__(self):
            self.last_title_candidates = []

        def resolve_context(self, event, plan, *, date_candidates=(), business_candidates=()):
            self.last_title_candidates = [{"key": "title_0", "text": event["text"]}]
            response = {
                "applicability": "applicable",
                "followup": "required",
                "title_span": "title_0",
                "due_key": "unknown",
                "same_work": {candidate["key"]: "different" for candidate in business_candidates},
                "evidence_spans": ["请各工班及时在系统中录入联单信息"],
            }
            return {**response, **response_update}

    processor, _config, _self_user = _processor(
        transport=transport,
        fallback=InvalidFallback(),
        existing_actions=[_prior_action()],
    )

    plan = processor.process_batch([_event()])[0]

    assert plan["decision"] == "REVIEW"
    assert plan["reason"] == "jev_v2_business_match_uncertain"
    assert plan["route_owner"] == CONFIGURED_OWNER
    assert plan.get("due_date") is None
    assert plan.get("due_iso") is None
    assert plan["semantic_fallback_error"] == expected_error
    assert processor.stats["semantic_fallback_calls"] == 1
    assert processor.stats["semantic_fallback_succeeded"] == 0
    assert processor.stats["semantic_fallback_failed"] == 1


def test_source_validated_eleven_am_deadline_survives_a_scope_review():
    transport, _jev_calls = _typed_transport(followup=0.5)
    processor, _config, _self_user = _processor(transport=transport)
    event = _event(
        "请各工班于2026年10月07日11点前完成危险废物联单信息反馈。",
        create_time="2026-10-07 09:00:00",
    )

    plan = processor.process_batch([event])[0]

    assert plan["decision"] == "REVIEW"
    assert plan["route_owner"] == "苗笑天"
    assert plan["assignment_audit"]["handover_source"] == "user_confirmed_month_anchor"
    assert plan["due_date"] == "2026-10-07"
    assert plan["due_iso"] == "2026-10-07T11:00:00+08:00"
    assert plan["due_role_unresolved"] is False


def test_missing_hazard_owner_clears_owner_and_executor_ids_on_review():
    transport, _jev_calls = _typed_transport(followup=0.5)
    processor, _config, _self_user = _processor(transport=transport, configure_owner=False)

    plan = processor.process_batch([_event("请各工班落实危险废物联单核对工作。")])[0]

    assert plan["decision"] == "REVIEW"
    assert plan["route_module"] == "hazardous_waste"
    assert plan.get("route_owner") is None
    assert plan.get("matrix_route_owner") == "杨梦卓"
    assert plan["executor_ids"] == []
    assert plan["executor_names"] == []
    assert "confirmed_area_handover_unverified" in plan["warnings"]
    assert plan["assignment_audit"]["primary_owner"] is None
    assert plan["assignment_audit"]["handover_evidence_ids"] == []
    assert "current_area_owner" in plan["missing_fields"]


def test_exact_human_confirmed_deadline_survives_a_later_title_review(tmp_path):
    transport, _jev_calls = _typed_transport()

    class FailingTitleResolver:
        def resolve(self, _event, _plan):
            raise LlmError("title_runtime_failed:PRIVATE_RESPONSE @PrivateName")

    evidence = {
        "kind": "human_deadline_clarification",
        "scope": "one source obligation only; local evaluation evidence, not production activation or generalized message-ID policy",
        "source_message_id": "human-review-source",
        "source_time": "2026-09-30 13:11:29",
        "source_evidence": "4点30分仍未更新的工班，将纳入本月考核项",
        "user_confirmation": "应以16:30作为完成截止",
        "confirmed_due_date": "2026-09-30",
        "confirmed_due_iso": "2026-09-30T16:30:00+08:00",
        "responsibility": "program-selected owner remains unchanged",
    }
    clarification_path = tmp_path / "local-review-clarification.json"
    clarification_path.write_text(json.dumps(evidence, ensure_ascii=False), encoding="utf-8")
    source = (
        "请各工班今日下班前对嘉祥危险废物处置联单信息进行重新录入更新。"
        "4点30分仍未更新的工班，将纳入本月考核项。"
        "同时逐项核对录入字段、处置去向和系统台账，确保记录完整准确后反馈。"
    )
    processor, _config, _self_user = _processor(
        transport=transport,
        title_resolver=FailingTitleResolver(),
        deadline_clarifications_path=clarification_path,
    )

    plan = processor.process_batch([
        _event(source, message_id="human-review-source", create_time="2026-09-30 13:11:29")
    ])[0]

    assert plan["decision"] == "REVIEW"
    assert plan["reason"] == "title_compiler_invalid"
    assert plan["route_owner"] == CONFIGURED_OWNER
    assert plan["due_date"] == "2026-09-30"
    assert plan["due_iso"] == "2026-09-30T16:30:00+08:00"
    assert plan["human_confirmation_evidence"]["provenance"] == "user_confirmation"
    assert plan["human_confirmation_evidence"]["confirmed_due_iso"] == evidence["confirmed_due_iso"]
    assert plan["title_field_error"] == {"code": "title_runtime_failed", "stage": "transport"}
    assert "human_confirmation_evidence" not in plan["semantic_evidence"]
    assert processor.stats["human_clarified_sources"] == 1


def test_invalid_jev_date_choice_does_not_replace_source_validated_due():
    transport, calls = _typed_transport(invalid_due_choice=True)
    processor, _config, _self_user = _processor(transport=transport)
    event = _event("请各工班于2026年10月07日11点前完成危险废物联单信息反馈。", create_time="2026-10-07 09:00:00")

    plan = processor.process_batch([event])[0]

    assert len(calls) == 1
    assert processor.stats["llm_calls"] == 1
    assert plan["decision"] == "REVIEW"
    assert plan["route_owner"] == "苗笑天"
    assert plan["assignment_audit"]["handover_source"] == "user_confirmed_month_anchor"
    assert plan["due_date"] == "2026-10-07"
    assert plan["due_iso"] == "2026-10-07T11:00:00+08:00"
    assert plan["jev_error_code"] == "jev_invalid_response"
    assert plan["jev_error_stage"] == "schema"
