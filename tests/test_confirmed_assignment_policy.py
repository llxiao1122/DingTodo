from __future__ import annotations

import json
import subprocess
import sys
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dws_adapter import DwsClient, DwsError, verify_todo  # noqa: E402
from runner import plan_batch  # noqa: E402


STAFF = [
    [1, "李林骁", "STALE-LI", "leader"],
    [2, "陈红洁", "STALE-CHEN", "executor"],
    [3, "张志斌", "STALE-ZHANG", "executor"],
    [4, "杨梦卓", "STALE-YANG", "executor"],
    [5, "苗笑天", "STALE-MIAO", "executor"],
    [6, "谭继衡", "STALE-TAN", "executor"],
]


def _handover(area: str, owner: str, evidence_id: str) -> dict:
    return {
        "period": "2026-10",
        "area_name": area,
        "active_owners": [owner],
        "reviewed": True,
        "status": "accepted",
        "source": "user_confirmed_month_anchor",
        "hygiene_review": "anchor",
        "evidence_id": evidence_id,
    }


def _payload(messages: list[dict], *, handover_evidence: list[dict] | None = None) -> dict:
    return {
        "config": {
            "config_staff": STAFF,
            "active_routing_loaded": True,
            "responsibility_rules": {
                "hazardous_waste": "杨梦卓",
                "scrap": "谭继衡",
                "materials": "李林骁",
            },
            "handover_evidence": list(handover_evidence or []),
        },
        "self_user": {"name": "李林骁", "user_id": "LIVE-LI"},
        "messages": messages,
    }


def _message(message_id: str, text: str, *, group: str = "铁炉西工班群") -> dict:
    return {
        "conversation_id": "synthetic-conversation",
        "message_id": message_id,
        "group": group,
        "sender": "合成发件人",
        "create_time": "2026-10-18 09:00:00",
        "text": text,
    }


def _plan(message: dict, *, evidence: list[dict] | None = None, run_date: date = date(2026, 10, 18)) -> dict:
    result = plan_batch(
        _payload([message], handover_evidence=evidence),
        mode="test",
        run_date=run_date,
    )
    return result["plans"][0]


def test_confirmed_a_and_c_rules_override_generic_leader_and_materials_defaults():
    messages = [
        _message("synthetic-a", "宣传简报：请各工班于10月20日前完成消防活动组织反馈。"),
        _message("synthetic-c", "请各工班于10月20日前完成需求计划相关数据核对。"),
    ]

    plans = plan_batch(_payload(messages), mode="test", run_date=date(2026, 10, 18))["plans"]

    assert [plan["decision"] for plan in plans] == ["CREATE", "CREATE"]
    assert [plan["route_owner"] for plan in plans] == ["陈红洁", "苗笑天"]
    assert plans[0]["assignment_audit"]["collaborators"] == ["李林骁"]
    assert plans[1]["assignment_audit"]["collaborators"] == ["李林骁"]
    # Test mode is deliberately isolated to the current user's DWS identity.
    assert all(plan["executor_ids"] == ["LIVE-LI"] for plan in plans)


def test_a_and_c_record_legacy_matrix_conflict_and_explicit_current_assignment_wins():
    payload = _payload([
        _message("synthetic-a-current", "宣传简报请安排苗笑天负责消防活动组织反馈，10月20日前完成。"),
        _message("synthetic-c-current", "请安排陈红洁负责需求计划相关数据核对，10月20日前完成。"),
    ])
    payload["config"]["responsibility_rules"].update({"drill": "李林骁", "materials": "李林骁"})

    plans = plan_batch(payload, mode="test", run_date=date(2026, 10, 18))["plans"]

    assert [plan["route_owner"] for plan in plans] == ["苗笑天", "陈红洁"]
    assert plans[0]["assignment_audit"]["assignment_basis"] == "explicit_named_assignment_in_source"
    assert plans[1]["assignment_audit"]["assignment_basis"] == "explicit_named_assignment_in_source"
    for plan in plans:
        assert plan["assignment_audit"]["database_conflict"] == {
            "configured_owner": "李林骁", "selected_owner": plan["route_owner"]
        }
        assert plan["assignment_audit"]["conflict_resolution"] == "explicit_current_assignment_has_priority_over_confirmed_static_rule_and_legacy_matrix"
        assert plan["assignment_audit"]["legacy_configuration_recheck_required"] is True


def test_d_notice_is_awareness_only_and_covers_the_six_confirmed_members():
    plan = _plan(
        _message("synthetic-d", "通知：请铁炉西工班全员知晓本次检修安排。")
    )

    assert plan["decision"] == "CREATE"
    assert plan["assignment_audit"]["awareness_only"] is True
    assert plan["assignment_audit"]["awareness_names"] == [
        "李林骁", "陈红洁", "张志斌", "杨梦卓", "苗笑天", "谭继衡"
    ]
    assert plan["due_iso"] is None
    assert "反馈" not in plan["title"]
    assert plan["executor_ids"] == ["LIVE-LI"]


@pytest.mark.parametrize(
    ("message_id", "text", "area", "owner", "old_owner", "module"),
    [
        (
            "synthetic-b",
            "请各工班于10月20日前完成废旧物资称重和拍照。",
            "材料棚及办公用房",
            "张志斌",
            "谭继衡",
            "scrap",
        ),
        (
            "synthetic-f",
            "请各工班于10月20日前完成危废处置数据核对。",
            "杂品库",
            "苗笑天",
            "杨梦卓",
            "hazardous_waste",
        ),
    ],
)
def test_b_and_f_use_verified_current_month_handover_evidence_and_record_conflicts(
    message_id: str, text: str, area: str, owner: str, old_owner: str, module: str
):
    plan = _plan(
        _message(message_id, text),
        evidence=[_handover(area, owner, f"synthetic-{module}-handover")],
    )

    assert plan["decision"] == "CREATE"
    assert plan["route_module"] == module
    assert plan["route_owner"] == owner
    audit = plan["assignment_audit"]
    assert audit["rule_version"]
    assert audit["source"] == "rule-driven-task-routing/references/confirmed-assignment-habits.md"
    assert audit["handover_evidence_ids"] == [f"synthetic-{module}-handover"]
    assert audit["database_conflict"] == {"configured_owner": old_owner, "selected_owner": owner}


def test_dynamic_area_rule_reviews_without_current_month_handover_evidence():
    plan = _plan(
        _message("synthetic-b-missing", "请各工班于11月20日前完成废旧物资称重和拍照。"),
        run_date=date(2026, 11, 1),
    )

    assert plan["decision"] == "REVIEW"
    assert plan["reason"] == "confirmed_area_handover_unverified"
    assert plan["executor_ids"] == []


def test_month_end_handover_does_not_treat_month_anchor_as_hygiene_release():
    plan = _plan(
        _message("synthetic-month-end-unreviewed", "请于10月31日前完成危废处置数据核对。"),
        run_date=date(2026, 10, 31),
    )
    assert plan["decision"] == "REVIEW"
    assert plan["reason"] == "confirmed_area_handover_unverified"
    assert plan["executor_ids"] == []


def test_month_end_overlap_keeps_both_area_owners_until_hygiene_passes():
    evidence = [{
        "period": "2026-10", "area_name": "杂品库", "active_owners": ["苗笑天", "杨梦卓"],
        "outgoing_owner": "杨梦卓", "incoming_owner": "苗笑天", "reviewed": True,
        "status": "overlap", "hygiene_review": "pending", "source": "dws_readback",
        "evidence_id": "synthetic-overlap-review-pending",
    }]
    plan = _plan(
        _message("synthetic-month-end-overlap", "请于10月31日前完成危废处置数据核对。"),
        evidence=evidence, run_date=date(2026, 10, 31),
    )
    assert plan["decision"] == "CREATE"
    assert plan["route_owner"] == "苗笑天"
    assert plan["routed_executor_names"] == ["苗笑天", "杨梦卓", "李林骁"]


def test_e_and_g_require_an_explicit_per_task_assignment():
    e_plan = _plan(
        _message("synthetic-e", "请各工班开展跨库区专项现场整理，李林骁逐次分配责任人。")
    )
    g_plan = _plan(
        _message("synthetic-g", "请各工班按要求完成每月盘点。")
    )

    assert e_plan["decision"] == "REVIEW"
    assert g_plan["decision"] == "REVIEW"
    assert e_plan["reason"] == "manual_assignment_required"
    assert g_plan["reason"] == "manual_assignment_required"


def test_exact_contact_resolution_supports_v1_return_shape_and_rejects_reused_ids():
    def completed(name: str, user_id: str) -> subprocess.CompletedProcess:
        payload = {
            "success": True,
            "result": [{"orgEmployeeModel": {"orgUserName": name, "userId": user_id}}],
        }
        return subprocess.CompletedProcess([], 0, json.dumps(payload), "")

    with patch(
        "dws_adapter.subprocess.run",
        side_effect=[completed("陈红洁", "DWS-CHEN"), completed("李林骁", "DWS-LI")],
    ):
        resolved = DwsClient().resolve_exact(["陈红洁", "李林骁"])

    assert resolved == {"陈红洁": "DWS-CHEN", "李林骁": "DWS-LI"}

    with patch(
        "dws_adapter.subprocess.run",
        side_effect=[completed("陈红洁", "DUPLICATE-ID"), completed("李林骁", "DUPLICATE-ID")],
    ):
        with pytest.raises(DwsError, match="unique"):
            DwsClient().resolve_exact(["陈红洁", "李林骁"])


def test_readback_uses_exact_name_fallback_only_after_live_ids_were_resolved():
    plan = {
        "title": "合成读回任务",
        "executor_names": ["陈红洁", "李林骁"],
        "executor_ids": ["DWS-CHEN", "DWS-LI"],
        "allow_executor_namespace_fallback": True,
        "priority": 20,
    }
    detail = {
        "subject": "合成读回任务",
        "isDone": False,
        "executorInfos": [
            {"name": "陈红洁", "userId": "DETAIL-CHEN"},
            {"orgEmployeeModel": {"orgUserName": "李林骁", "userId": "DETAIL-LI"}},
        ],
        "priority": 20,
    }

    assert verify_todo(detail, plan) == []
    assert plan["identity_fallback_evidence"]["method"] == "exact_unique_name_after_id_namespace_mismatch"

    duplicate_names = {
        **detail,
        "executorInfos": [
            {"name": "陈红洁", "userId": "DETAIL-CHEN"},
            {"name": "陈红洁", "userId": "OTHER-CHEN"},
        ],
    }
    assert "executor_mismatch" in verify_todo(duplicate_names, plan)

    missing_ids = {
        **detail,
        "executorInfos": [{"name": "陈红洁"}, {"name": "李林骁"}],
    }
    assert "executor_mismatch" in verify_todo(missing_ids, plan)
