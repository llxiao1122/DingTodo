"""Behavioral coverage for deterministic DWS attachment context in the real poller."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import json
from unittest.mock import patch

import event_worker
from jev_v2 import JevV2Resolver


NOW = datetime.fromisoformat("2037-04-12T10:00:00+08:00")
CONFIG = {
    "config_calendar": [{"dt": "2037-04-12", "day_type": "workday"}],
    "config_group": [
        {"group_name": "Operational Chat", "conversation_id": "conversation-a", "purpose": "todo_scan"}
    ],
    "config_staff": [[1, "SyntheticSelf", "synthetic-self-contact", "lead"]],
    "active_routing_loaded": True,
    "responsibility_rules": {"materials": "SyntheticSelf", "asset": "SyntheticSelf"},
}


def _message(message_id: str, moment: str, text: str, *, conversation: str = "conversation-a",
             sender_id: str = "sender-a", resources=None, at_users=None) -> dict:
    return {
        "conversation_id": conversation,
        "message_id": message_id,
        "create_time": moment,
        "sender_id": sender_id,
        "sender": "SyntheticSender",
        "text": text,
        "resource_refs": deepcopy(resources or []),
        "at_users": deepcopy(at_users or []),
    }


def _file(message_id: str = "file-source", moment: str = "2037-04-12 09:01:00",
          filename: str = "inventory-checklist.xlsx", with_text_resource_id: bool = False, **kwargs) -> dict:
    metadata = " fileId=opaque-text-file-id" if with_text_resource_id else ""
    return _message(
        message_id,
        moment,
        f"[文件]{filename}{metadata}",
        resources=[{"name": filename, "resourceId": "opaque-resource-value", "resourceIdType": "file"}],
        **kwargs,
    )


def _policy_answer(request: dict, _timeout: float) -> dict:
    answers = {
        "applies_to_workgroup": {"type": "noul", "noul": 0.99},
        "needs_follow_up": {"type": "noul", "noul": 0.99},
        "prospective_action": {"type": "noul", "noul": 0.99},
    }
    for key, question in request["questions"].items():
        if question["type"] != "choice":
            continue
        options = question["criteria"]
        choice = "none" if "none" in options else options[0]
        probabilities = {option: (0.99 if option == choice else 0.01 / max(1, len(options) - 1)) for option in options}
        answers[key] = {
            "type": "choice",
            "choice": choice,
            "probabilities": probabilities,
            "confidence": 0.99,
        }
    return {"answers": answers}


def _run_poller(messages: list[dict], *, config: dict | None = None, actions: list[dict] | None = None):
    config = deepcopy(config or CONFIG)
    raw_by_conversation: dict[str, list[dict]] = {}
    for row in messages:
        raw_by_conversation.setdefault(row["conversation_id"], []).append(row)

    class Adapter:
        def query(self, request):
            if request["view"] == "runtime_config":
                return {"config": config}
            if request["view"] == "scan_state":
                return {"rows": [], "has_more": False}
            if request["view"] == "actions":
                return {"rows": deepcopy(actions or []), "has_more": False}
            if request["view"] == "messages":
                return {"rows": [], "has_more": False}
            raise AssertionError(request)

    class Reader:
        def read_group(self, *, conversation_id, **_kwargs):
            return {
                "complete": True,
                "source_order_provenance": event_worker.COMPLETE_DWS_PAGE_ORDER,
                "messages": raw_by_conversation.get(conversation_id, []),
            }

    class Dws:
        def get_self(self):
            return {"name": "SyntheticSelf", "user_id": "synthetic-self-contact"}

    calls: list[dict] = []
    resolver = JevV2Resolver(
        new_policy=True,
        transport=lambda request, timeout: calls.append(deepcopy(request)) or _policy_answer(request, timeout),
    )
    poller = event_worker.OneShotHistoryPoller(
        adapter=Adapter(),
        reader=Reader(),
        dws=Dws(),
        resolver=resolver,
        now_factory=lambda: NOW,
    )
    with patch("routing.SELF_NAME", "SyntheticSelf"):
        result = poller.run_once()
    return result, calls, raw_by_conversation


def _plan_for(result: dict, message_id: str) -> dict:
    return next(plan for plan in result["plans"] if plan["message_id"] == message_id)


def test_department_only_notice_and_matching_file_do_not_create_or_call_semantics():
    notice = _message(
        "department-notice",
        "2037-04-12 09:00:00",
        "请各部门于2037年4月16日前核对 inventory-checklist.xlsx 并完成材料台账复核。",
    )
    attachment = _file()

    result, calls, _raw = _run_poller([notice, attachment])

    assert result["stats"]["create"] == 0
    assert calls == []
    file_plan = _plan_for(result, "file-source")
    assert file_plan["decision"] == "IGNORE"
    assert file_plan["due_date"] is None
    assert file_plan["due_iso"] is None


def test_standalone_file_is_reviewed_without_semantic_resolution_or_filename_title():
    attachment = _file(filename="危废台账核对清单.xlsx", with_text_resource_id=True)

    result, calls, _raw = _run_poller([attachment])

    plan = _plan_for(result, "file-source")
    assert plan["decision"] == "REVIEW"
    assert plan["title"] != "危废台账核对清单.xlsx"
    assert result["stats"]["create"] == 0
    assert calls == []

    marker_only = _message("marker-only-file", "2037-04-12 09:01:00", "[文件]")
    marker_result, marker_calls, _marker_raw = _run_poller([marker_only])
    assert _plan_for(marker_result, "marker-only-file")["decision"] == "REVIEW"
    assert marker_calls == []


def test_direct_self_and_explicit_workgroup_notice_pairs_keep_file_source_identity():
    cases = [
        _message(
            "direct-notice",
            "2037-04-12 09:00:00",
            "请@SyntheticSelf 核对 inventory-checklist.xlsx 并于2037年4月16日前反馈。",
            at_users=[{"name": "SyntheticSelf", "userId": "synthetic-self-contact"}],
        ),
        _message(
            "workgroup-notice",
            "2037-04-12 09:00:00",
            "请各工班核对 inventory-checklist.xlsx 并反馈。",
        ),
    ]

    for notice in cases:
        attachment = _file(
            message_id=f"{notice['message_id']}-file",
            with_text_resource_id=True,
        )
        prior_action = {
            "conversation_id": notice["conversation_id"],
            "message_id": notice["message_id"],
            "source_key": f"conversation-a:{notice['message_id']}:todo",
            "action_key": "todo",
            "status": "IGNORED",
        }
        result, calls, raw = _run_poller([notice, attachment], actions=[prior_action])
        file_plan = _plan_for(result, attachment["message_id"])

        assert file_plan["decision"] == "CREATE"
        assert file_plan["source_key"] == f"conversation-a:{attachment['message_id']}:todo"
        assert file_plan["title"] not in {"", "inventory-checklist", "inventory-checklist.xlsx"}
        assert "核对" in file_plan["title"]
        assert file_plan["route_owner"] == "SyntheticSelf"
        assert "opaque-resource-value" not in file_plan["title"]
        if notice["message_id"] == "direct-notice":
            assert file_plan["due_date"] == "2037-04-16"
        assert calls, "the bounded semantic policy resolution should remain for eligible work"
        assert "opaque-text-file-id" not in json.dumps(calls, ensure_ascii=False)
        assert raw["conversation-a"][1]["text"] == "[文件]inventory-checklist.xlsx fileId=opaque-text-file-id"
        assert raw["conversation-a"][1]["resource_refs"] == attachment["resource_refs"]


def test_ambiguous_or_noncausal_notice_context_never_merges_into_a_file():
    notice = lambda mid, stamp, sender="sender-a", conversation="conversation-a": _message(
        mid, stamp, "请各部门核对 inventory-checklist.xlsx 并完成台账复核。",
        sender_id=sender, conversation=conversation,
    )
    cases = [
        [notice("notice-1", "2037-04-12 09:00:00"), notice("notice-2", "2037-04-12 09:00:20"), _file()],
        [notice("notice-other-sender", "2037-04-12 09:00:00", sender="sender-b"), _file()],
        [notice("notice-old", "2037-04-12 08:58:59"), _file()],
        [_message("notice-no-filename-reference", "2037-04-12 09:00:00", "请各部门核对另一份清单并反馈。"), _file()],
        [_file(moment="2037-04-12 09:00:00"), notice("notice-future", "2037-04-12 09:00:01")],
    ]

    for messages in cases:
        result, calls, _raw = _run_poller(messages)
        file_plan = _plan_for(result, "file-source")
        assert file_plan["decision"] == "REVIEW"
        assert calls == []


def test_notice_from_another_configured_conversation_cannot_scope_the_file():
    config = deepcopy(CONFIG)
    config["config_group"].append(
        {"group_name": "Other Operational Chat", "conversation_id": "conversation-b", "purpose": "todo_scan"}
    )
    notice = _message(
        "notice-other-conversation",
        "2037-04-12 09:00:00",
        "请各部门核对 inventory-checklist.xlsx 并完成台账复核。",
        conversation="conversation-b",
    )

    result, calls, _raw = _run_poller([notice, _file()], config=config)

    assert _plan_for(result, "file-source")["decision"] == "REVIEW"
    assert calls == []


def test_direct_other_person_assignment_with_file_is_outside_self_scope():
    notice = _message(
        "other-person-notice",
        "2037-04-12 09:00:00",
        "请@SyntheticOther 核对 inventory-checklist.xlsx 并反馈。",
        at_users=[{"name": "SyntheticOther", "userId": "synthetic-other-contact"}],
    )

    result, calls, _raw = _run_poller([notice, _file()])

    assert result["stats"]["create"] == 0
    assert _plan_for(result, "file-source")["decision"] == "IGNORE"
    assert calls == []


def test_background_workgroup_invitation_does_not_scope_other_department_report_or_file():
    notice = _message(
        "other-department-report",
        "2037-04-12 09:00:00",
        "拟邀请各工班负责人参会，每个工班安排一位负责人参会。"
        "请某科室@SyntheticOther 于2037年4月13日14:00前报送工班负责人信息，"
        "填写《各工班负责人信息统计表》，收到请回复。",
        at_users=[{"name": "SyntheticOther", "userId": "synthetic-other-contact"}],
    )
    attachment = _file(
        message_id="report-form-file",
        moment="2037-04-12 09:00:31",
        filename="各工班负责人信息统计表.xlsx",
    )

    result, calls, _raw = _run_poller([notice, attachment])

    notice_plan = _plan_for(result, notice["message_id"])
    file_plan = _plan_for(result, attachment["message_id"])
    assert notice_plan["decision"] == "IGNORE"
    assert file_plan["decision"] == "IGNORE"
    assert result["stats"]["create"] == 0
    assert calls == []
    for plan in (notice_plan, file_plan):
        assert plan["route_owner"] is None
        assert plan["due_date"] is None
        assert plan["due_iso"] is None


def test_independent_workgroup_duty_with_other_collector_remains_eligible():
    notice = _message(
        "workgroup-report-duty",
        "2037-04-12 09:00:00",
        "请各工班填报 SyntheticX 信息并交给@SyntheticOther汇总。",
        at_users=[{"name": "SyntheticOther", "userId": "synthetic-other-contact"}],
    )

    result, calls, _raw = _run_poller([notice])

    plan = _plan_for(result, notice["message_id"])
    assert plan["decision"] == "CREATE"
    assert plan["route_owner"] == "SyntheticSelf"
    assert calls


def test_mixed_or_conflicting_assignment_clauses_are_reviewed():
    cases = [
        _message(
            "mixed-independent-duties",
            "2037-04-12 09:00:00",
            "请各工班核对 SyntheticX，并请@SyntheticOther报送 SyntheticY。",
            at_users=[{"name": "SyntheticOther", "userId": "synthetic-other-contact"}],
        ),
        _message(
            "conflicting-joint-recipient",
            "2037-04-12 09:02:00",
            "请@SyntheticOther和各工班共同报送 SyntheticX。",
            at_users=[{"name": "SyntheticOther", "userId": "synthetic-other-contact"}],
        ),
    ]

    for notice in cases:
        result, calls, _raw = _run_poller([notice])
        plan = _plan_for(result, notice["message_id"])
        assert plan["decision"] == "REVIEW"
        assert plan["route_owner"] is None
        assert plan["due_date"] is None
        assert plan["due_iso"] is None
        assert calls == []


def test_already_linked_file_source_stays_ignored_without_recreation():
    attachment = _file()
    actions = [{
        "conversation_id": "conversation-a",
        "message_id": attachment["message_id"],
        "source_key": "conversation-a:file-source:todo",
        "action_key": "todo",
        "status": "EXTERNAL_CREATED",
        "external_task_id": "existing-task",
    }]

    result, calls, _raw = _run_poller([attachment], actions=actions)

    plan = _plan_for(result, "file-source")
    assert plan["decision"] == "IGNORE"
    assert plan["reason"] == "source_already_processed"
    assert result["stats"]["external_todos_created"] == 0
    assert calls == []
