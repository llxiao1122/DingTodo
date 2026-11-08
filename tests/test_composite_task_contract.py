from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from composite_tasks import (  # noqa: E402
    detect_composite_candidate, prepare_child_components, propose_child_actions, run_durable_child_actions,
)
from event_worker import EventProcessor  # noqa: E402


COMPOSITE_TEXT = (
    "资产盘点权限核对于10月20日前完成；"
    "资产标签核验于10月22日前完成；"
    "线上流程测试于10月25日前完成。"
)


def test_three_independent_deadlines_are_detected_as_a_composite_review_candidate():
    candidate = detect_composite_candidate(COMPOSITE_TEXT)

    assert candidate is not None
    assert [row["scope_key"] for row in candidate["components"]] == [
        "asset_inventory_permissions", "asset_tags", "online_process",
    ]
    assert [row["deadline_text"] for row in candidate["components"]] == [
        "10月20日前", "10月22日前", "10月25日前",
    ]


def test_child_action_key_uses_source_and_business_scope_not_title_or_array_position():
    candidate = detect_composite_candidate(COMPOSITE_TEXT)
    changed_titles = [
        {**row, "title": f"修改后的标题{index}"}
        for index, row in enumerate(reversed(candidate["components"]))
    ]
    first = propose_child_actions(
        parent_task_id="synthetic-parent-task",
        conversation_id="synthetic-conversation",
        message_id="synthetic-message",
        business_scope="synthetic-asset-cycle-2026-10",
        components=candidate["components"],
    )
    second = propose_child_actions(
        parent_task_id="synthetic-parent-task",
        conversation_id="synthetic-conversation",
        message_id="synthetic-message",
        business_scope="synthetic-asset-cycle-2026-10",
        components=changed_titles,
    )

    assert {row["scope_key"]: row["action_key"] for row in first} == {
        row["scope_key"]: row["action_key"] for row in second
    }
    assert all(row["operation"] == "claim_child_action" for row in first)
    assert all(row["parent_task_id"] == "synthetic-parent-task" for row in first)


def test_proposed_child_actions_require_explicit_parent_identity_and_deadline():
    candidate = detect_composite_candidate(COMPOSITE_TEXT)
    with pytest.raises(ValueError, match="parent_task_id_required"):
        propose_child_actions(
            parent_task_id="", conversation_id="c", message_id="m", business_scope="asset-cycle",
            components=candidate["components"],
        )
    missing_due = [{**row, "deadline_text": ""} for row in candidate["components"]]
    with pytest.raises(ValueError, match="explicit_deadline_required"):
        propose_child_actions(
            parent_task_id="parent", conversation_id="c", message_id="m", business_scope="asset-cycle",
            components=missing_due,
        )


def test_composite_text_without_separate_explicit_deadlines_is_not_split():
    assert detect_composite_candidate("请完成资产盘点权限核对、标签核验及线上流程测试。") is None


def test_production_pipeline_reviews_composite_without_creating_parent_or_children():
    class Writer:
        def __init__(self):
            self.requests = []
        def execute(self, request):
            self.requests.append(dict(request))
            if request["operation"] == "claim_action":
                return {"created": True, "action_id": 12}
            return {"ok": True}

    class Dws:
        def __init__(self):
            self.search_calls = 0
            self.create_calls = 0
        def search_todos(self, _query):
            self.search_calls += 1
            return []
        def resolve_exact(self, _names):
            return {}
        def create_todo(self, *_args, **_kwargs):
            self.create_calls += 1
            return {"task_id": "unexpected"}

    writer, dws = Writer(), Dws()
    processor = EventProcessor(
        {
            "config_group": [{
                "conversation_id": "synthetic-conversation", "group_name": "铁炉西工班群", "purpose": "todo_scan",
            }],
            "config_staff": [[1, "李林骁", "STALE", "leader"]],
            "active_routing_loaded": True,
        },
        {"name": "李林骁", "user_id": "SELF"},
        mode="production", write_ledger=True, allow_external_writes=True,
        dws=dws, writer=writer,
    )

    plan = processor.process_batch([{
        "conversation_id": "synthetic-conversation", "message_id": "synthetic-composite-source",
        "group": "铁炉西工班群", "sender_id": "synthetic-sender",
        "create_time": "2026-10-18 10:00:00", "text": COMPOSITE_TEXT,
    }])[0]

    assert plan["decision"] == "REVIEW"
    assert plan["reason"] == "external_executor_identity_lookup_failed"
    assert len(plan["subtask_decomposition"]["components"]) == 3
    assert dws.search_calls == dws.create_calls == 0
    assert "claim_action" in [row["operation"] for row in writer.requests]
    assert "save_assignment_audit" in [row["operation"] for row in writer.requests]


def test_durable_child_resume_reuses_linked_ids_and_creates_only_missing_children():
    candidate = detect_composite_candidate(COMPOSITE_TEXT)
    components = prepare_child_components(candidate, date(2026, 10, 18))

    class DurableWriter:
        def __init__(self):
            self.actions = {}
            self.requests = []
        def execute(self, request):
            self.requests.append(dict(request))
            operation = request["operation"]
            if operation == "claim_child_action":
                key = request["action_key"]
                if key in self.actions:
                    return {"created": False, **self.actions[key]}
                self.actions[key] = {
                    "action_id": len(self.actions) + 1, "external_task_id": None,
                    "last_error": None,
                }
                return {"created": True, **self.actions[key]}
            if operation == "link_external_task":
                row = next(value for value in self.actions.values() if value["action_id"] == request["action_id"])
                row["external_task_id"] = request["external_task_id"]
                return {"linked": True}
            if operation == "save_action_result":
                row = next(value for value in self.actions.values() if value["action_id"] == request["action_id"])
                row["last_error"] = request.get("last_error")
                return {"saved": True}
            raise AssertionError(operation)

    class Dws:
        def __init__(self):
            self.created = []
            self.comments = {}
            self.fail_comment_once = True
        def create_sub_todo(self, parent_task_id, title, executor_ids, due_iso, priority=20):
            task_id = f"synthetic-child-{len(self.created) + 1}"
            self.created.append((parent_task_id, title, list(executor_ids), due_iso, priority, task_id))
            return {"task_id": task_id}
        def get_todo(self, task_id):
            parent_task_id, title, executor_ids, due_iso, priority, _ = next(
                row for row in self.created if row[-1] == task_id
            )
            from datetime import datetime
            due_ms = int(datetime.fromisoformat(due_iso).timestamp() * 1000)
            return {
                "taskId": task_id, "subject": title, "parentId": parent_task_id,
                "dueTime": due_ms, "priority": priority, "isDone": False,
                "executorInfos": [{"name": "李林骁", "userId": executor_ids[0]}],
            }
        def list_task_comments(self, task_id):
            return list(self.comments.get(task_id, []))
        def add_source_comment(self, task_id, comment):
            self.comments.setdefault(task_id, []).append({"content": comment})
            if self.fail_comment_once:
                self.fail_comment_once = False
                raise RuntimeError("synthetic_comment_failure")
        def verify_source_comment(self, task_id, comment):
            assert any(row["content"] == comment for row in self.comments.get(task_id, []))

    writer, dws = DurableWriter(), Dws()
    arguments = {
        "parent_action_id": 900, "parent_task_id": "synthetic-parent-task",
        "conversation_id": "synthetic-conversation", "message_id": "synthetic-message",
        "business_scope": "synthetic-asset-cycle-2026-10", "executor_ids": ["synthetic-live-executor"],
        "executor_names": ["李林骁"],
        "components": components, "writer": writer, "dws": dws, "allow_external_writes": True,
    }

    first = run_durable_child_actions(**arguments)
    second = run_durable_child_actions(**arguments)

    assert first["status"] == "REVIEW"
    assert second["status"] == "COMPLETED"
    assert len(dws.created) == 3
    assert first["errors"] == ["child_readback_or_comment_incomplete"]
    assert all(row[0] == "synthetic-parent-task" for row in dws.created)
    assert all(row[2] == ["synthetic-live-executor"] for row in dws.created)
    assert sum(1 for request in writer.requests if request["operation"] == "claim_child_action") == 4
    assert writer.actions[next(iter(writer.actions))]["external_task_id"]
    assert all(action["external_task_id"] for action in writer.actions.values())


def test_child_create_uncertainty_never_reissues_non_idempotent_create():
    candidate = detect_composite_candidate(COMPOSITE_TEXT)
    component = prepare_child_components(candidate, date(2026, 10, 18))[:1]

    class Writer:
        def __init__(self):
            self.claimed = False
        def execute(self, request):
            if request["operation"] == "claim_child_action":
                if self.claimed:
                    return {"created": False, "action_id": 1, "external_task_id": None,
                            "last_error": "child_create_outcome_unknown"}
                self.claimed = True
                return {"created": True, "action_id": 1, "external_task_id": None}
            if request["operation"] == "link_external_task":
                raise RuntimeError("synthetic_durable_link_failure")
            return {"ok": True}

    class Dws:
        def __init__(self):
            self.create_calls = 0
        def create_sub_todo(self, *_args, **_kwargs):
            self.create_calls += 1
            return {"task_id": "synthetic-unknown-child"}

    writer, dws = Writer(), Dws()
    arguments = {
        "parent_action_id": 901, "parent_task_id": "synthetic-parent-task",
        "conversation_id": "synthetic-conversation", "message_id": "synthetic-message-2",
        "business_scope": "synthetic-asset-cycle-2026-10", "executor_ids": ["synthetic-live-executor"],
        "executor_names": ["李林骁"],
        "components": component, "writer": writer, "dws": dws, "allow_external_writes": True,
    }
    assert run_durable_child_actions(**arguments)["status"] == "REVIEW"
    assert run_durable_child_actions(**arguments)["status"] == "REVIEW"
    assert dws.create_calls == 1


import pytest
