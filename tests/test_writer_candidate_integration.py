from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from pathlib import Path
import sys
from unittest.mock import patch

import pytest


ROOT = Path(__file__).resolve().parents[1]
TESTS = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(TESTS))

from candidate_mysql_fake import FakeMySQLDatabase, load_candidate_writer  # noqa: E402
from composite_tasks import detect_composite_candidate  # noqa: E402
import event_worker  # noqa: E402
from event_worker import EventProcessor  # noqa: E402
from routing import build_source_comment  # noqa: E402


COMPOSITE_TEXT = (
    "资产盘点权限核对于10月20日前完成；"
    "资产标签核验于10月22日前完成；"
    "线上流程测试于10月25日前完成。"
)
SYNTHETIC_AUDIT = {
    "rule_version": "confirmed-assignment/2026-10-v1",
    "source": "event_worker/explicit-assignment-fallback",
    "rule_id": None,
    "primary_owner": "李林骁",
    "collaborators": [],
    "awareness_only": False,
    "awareness_names": [],
    "manual_assignment_required": False,
    "assignment_basis": "processor_resolved_names",
    "handover_evidence_ids": [],
    "handover_source": None,
}


class SyntheticDws:
    def __init__(self):
        self.tasks = {}
        self.comments = {}
        self.calls = []
        self.subtask_failure_at: int | None = None
        self.subtask_call_count = 0

    def search_todos(self, _query):
        self.calls.append(("search",))
        return []

    def get_self(self):
        self.calls.append(("get_self",))
        return {"name": "李林骁", "user_id": "SYNTHETIC-SELF"}

    def resolve_exact(self, names):
        self.calls.append(("resolve_exact", tuple(names)))
        return {name: "SYNTHETIC-SELF" for name in names if name == "李林骁"}

    def create_todo(self, plan, priority=20):
        task_id = "synthetic-parent-task"
        self.tasks[task_id] = {
            "taskId": task_id,
            "subject": plan["title"],
            "due_iso": plan.get("due_iso"),
            "priority": priority,
            "executor_ids": list(plan["executor_ids"]),
            "executor_names": list(plan["executor_names"]),
            "parent_id": None,
        }
        self.calls.append(("create_parent", task_id))
        return {"task_id": task_id}

    def create_sub_todo(self, parent_task_id, title, executor_ids, due_iso, priority=20):
        self.subtask_call_count += 1
        self.calls.append(("create_child", parent_task_id, title))
        if self.subtask_failure_at == self.subtask_call_count:
            raise TimeoutError("synthetic_create_sub_timeout")
        task_id = f"synthetic-child-{self.subtask_call_count}"
        parent = self.tasks.get(parent_task_id)
        names = list(parent["executor_names"]) if parent else ["李林骁"]
        self.tasks[task_id] = {
            "taskId": task_id,
            "subject": title,
            "due_iso": due_iso,
            "priority": priority,
            "executor_ids": list(executor_ids),
            "executor_names": names,
            "parent_id": parent_task_id,
        }
        return {"task_id": task_id}

    def get_todo(self, task_id):
        self.calls.append(("get_todo", task_id))
        task = self.tasks.get(str(task_id))
        if not task:
            raise AssertionError("synthetic task missing")
        due_time = None
        if task["due_iso"]:
            due_time = int(datetime.fromisoformat(task["due_iso"]).timestamp() * 1000)
        return {
            "taskId": task["taskId"],
            "subject": task["subject"],
            "parentId": task["parent_id"],
            "dueTime": due_time,
            "priority": task["priority"],
            "isDone": False,
            "executorInfos": [
                {"name": name, "userId": user_id}
                for name, user_id in zip(task["executor_names"], task["executor_ids"])
            ],
        }

    def list_task_comments(self, task_id):
        self.calls.append(("list_comments", task_id))
        return list(self.comments.get(str(task_id), []))

    def add_source_comment(self, task_id, content):
        self.calls.append(("add_comment", task_id))
        self.comments.setdefault(str(task_id), []).append({"content": content})
        return {"task_id": task_id, "verified": True}

    def verify_source_comment(self, task_id, content):
        self.calls.append(("verify_comment", task_id))
        assert any(row.get("content") == content for row in self.comments.get(str(task_id), []))


def _writer_and_db():
    writer_class, _policy, pymysql = load_candidate_writer()
    db = FakeMySQLDatabase(pymysql.IntegrityError)
    requests = []

    class TrackingWriter(writer_class):
        def execute(self, request):
            requests.append(dict(request))
            return super().execute(request)

    return TrackingWriter(connect_factory=db.connect), db, requests


def _runtime():
    return {
        "config_group": [{
            "conversation_id": "synthetic-conversation",
            "group_name": "铁炉西工班群",
            "purpose": "todo_scan",
        }],
        "config_staff": [[1, "李林骁", "SYNTHETIC-STALE", "leader"]],
        "active_routing_loaded": True,
    }


def _message(message_id="synthetic-composite-source"):
    return {
        "conversation_id": "synthetic-conversation",
        "message_id": message_id,
        "group": "铁炉西工班群",
        "sender_id": "synthetic-sender",
        "sender": "合成发件人",
        "create_time": "2026-10-18 10:00:00",
        "text": COMPOSITE_TEXT,
    }


def _plan_batch(payload, *, mode, live_ids=None, **_kwargs):
    message = payload["messages"][0]
    already = any(
        row.get("conversation_id") == message.get("conversation_id")
        and row.get("message_id") == message.get("message_id")
        and row.get("action_key", "todo") == "todo"
        for row in payload.get("existing_actions", [])
    )
    plan = {
        "decision": "IGNORE" if already else "CREATE",
        "reason": "source_already_processed" if already else "synthetic_confirmed_work",
        "title": "资产综合核验",
        "due_date": "2026-10-20",
        "due_iso": "2026-10-20T18:00:00+08:00",
        "business_key": "synthetic-asset-cycle-2026-10",
        "route_owner": "李林骁",
        "route_module": "asset",
        "executor_names": ["李林骁"],
        "assignment_audit": dict(SYNTHETIC_AUDIT),
        "comment": build_source_comment(message),
    }
    if mode == "production":
        plan["executor_ids"] = [live_ids["李林骁"]] if live_ids else []
    return {"plans": [plan]}


def _processor(writer, dws, existing_actions=None):
    return EventProcessor(
        _runtime(), {"name": "李林骁", "user_id": "SYNTHETIC-SELF"},
        mode="production", write_ledger=True, allow_external_writes=True,
        writer=writer, dws=dws, existing_actions=existing_actions,
    )


def _database_actions(db):
    return [
        {**row, "action_id": row["id"]}
        for row in sorted(db.actions.values(), key=lambda value: value["id"])
    ]


class SyntheticHistoryAdapter:
    def __init__(self, db):
        self.db = db

    def query(self, request):
        view = request["view"]
        if view == "runtime_config":
            return {"config": {
                **_runtime(),
                "config_calendar": [{"dt": "2026-10-18", "day_type": "workday"}],
            }}
        if view == "scan_state":
            return {"rows": [], "has_more": False}
        if view == "actions":
            rows = [
                row for row in _database_actions(self.db)
                if not request.get("conversation_id") or row["conversation_id"] == request["conversation_id"]
            ]
            return {"rows": rows, "has_more": False}
        raise AssertionError(request)


class SyntheticHistoryReader:
    def __init__(self, message):
        self.message = message
        self.calls = []

    def read_group(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "complete": True,
            "source_order_provenance": "complete_dws_chronological_page",
            "messages": [dict(self.message)],
        }


def _production_entry(db, writer, dws, message):
    return event_worker.OneShotHistoryPoller(
        adapter=SyntheticHistoryAdapter(db),
        reader=SyntheticHistoryReader(message),
        dws=dws,
        writer=writer,
        mode="production",
        write_ledger=True,
        allow_external_writes=True,
        not_before=datetime.fromisoformat("2026-10-18T00:00:00+08:00"),
        now_factory=lambda: datetime.fromisoformat("2026-10-18T12:00:00+08:00"),
    )


def test_production_processor_uses_candidate_writer_and_recovers_only_unclaimed_children():
    writer, db, requests = _writer_and_db()
    dws = SyntheticDws()
    original_runner = event_worker.run_durable_child_actions

    def interrupt_after_two_children(**kwargs):
        result = original_runner(**{**kwargs, "components": kwargs["components"][:2]})
        assert result["status"] == "COMPLETED"
        return {
            "status": "REVIEW", "created": result["created"], "resumed": result["resumed"],
            "errors": ["synthetic_process_interruption_after_two_children"],
        }

    with patch("event_worker.plan_batch", side_effect=_plan_batch), patch(
        "event_worker.run_durable_child_actions", side_effect=interrupt_after_two_children
    ):
        first_run = _production_entry(db, writer, dws, _message()).run_once()

    first = first_run["plans"][0]
    assert first_run["status"] == "REVIEW"
    assert first["decision"] == "REVIEW"
    assert first["reason"] == "composite_child_actions_incomplete"
    assert len([row for row in dws.tasks if row.startswith("synthetic-child-")]) == 2
    parent = next(row for row in db.actions.values() if row["action_key"] == "todo")
    children = [row for row in db.actions.values() if row.get("parent_action_id") == parent["id"]]
    assert len(children) == 2
    assert all(row["external_task_id"] for row in children)
    audit = json.loads(parent["rule_audit"])
    assert audit["primary_owner"] == "李林骁"
    assert audit["source"] == "event_worker/explicit-assignment-fallback"
    assert "text" not in audit and "raw_message" not in audit

    before_parent_creates = len([call for call in dws.calls if call[0] == "create_parent"])
    before_child_ids = {row["external_task_id"] for row in children}
    with patch("event_worker.plan_batch", side_effect=_plan_batch):
        resumed_run = _production_entry(db, writer, dws, _message()).run_once()

    assert resumed_run["status"] == "COMPLETED"
    resumed = resumed_run["plans"][0]
    assert resumed["subtask_execution"]["status"] == "COMPLETED"
    assert len([call for call in dws.calls if call[0] == "create_parent"]) == before_parent_creates == 1
    assert len([row for row in dws.tasks if row.startswith("synthetic-child-")]) == 3
    assert len(before_child_ids & {row.get("external_task_id") for row in db.actions.values()}) == 2
    assert len([request for request in requests if request["operation"] == "claim_child_action"]) == 5
    assert all(row.get("parent_action_id") == parent["id"] for row in db.actions.values() if row.get("parent_action_id"))
    task_ids = [row.get("external_task_id") for row in db.actions.values() if row.get("external_task_id")]
    assert len(task_ids) == len(set(task_ids))
    assert sum(1 for call in dws.calls if call[0] == "get_todo") >= 5


def test_unknown_create_sub_outcome_is_durable_and_never_reissued():
    writer, db, requests = _writer_and_db()
    dws = SyntheticDws()
    dws.subtask_failure_at = 1
    components = detect_composite_candidate(COMPOSITE_TEXT)["components"]
    from composite_tasks import prepare_child_components, run_durable_child_actions
    prepared = prepare_child_components(detect_composite_candidate(COMPOSITE_TEXT), datetime(2026, 10, 18).date())
    parent = writer.execute({
        "operation": "claim_action", "conversation_id": "synthetic-conversation",
        "message_id": "synthetic-unknown-source", "action_key": "todo",
        "business_key": "synthetic-asset-cycle-2026-10", "status": "CLAIMED",
    })
    writer.execute({
        "operation": "link_external_task", "action_id": parent["action_id"],
        "external_task_id": "synthetic-parent-task", "status": "EXTERNAL_CREATED",
    })
    call_args = {
        "parent_action_id": parent["action_id"], "parent_task_id": "synthetic-parent-task",
        "conversation_id": "synthetic-conversation", "message_id": "synthetic-unknown-source",
        "business_scope": "synthetic-asset-cycle-2026-10", "executor_ids": ["SYNTHETIC-SELF"],
        "executor_names": ["李林骁"], "components": prepared,
        "writer": writer, "dws": dws, "allow_external_writes": True,
    }
    first = run_durable_child_actions(**call_args)
    second = run_durable_child_actions(**call_args)

    assert first["status"] == second["status"] == "REVIEW"
    assert "child_create_state_unknown" in second["errors"]
    assert dws.subtask_call_count == 1
    unknown = next(row for row in db.actions.values() if row.get("parent_action_id"))
    assert unknown["external_task_id"] is None
    assert unknown["last_error"] == "child_create_outcome_unknown"
    assert sum(1 for row in requests if row["operation"] == "claim_child_action") == 2


def test_business_collision_keeps_a_source_row_and_reads_canonical_effective_task_id():
    writer, db, _requests = _writer_and_db()
    canonical = writer.execute({
        "operation": "claim_action", "conversation_id": "synthetic-conversation",
        "message_id": "synthetic-source-1", "action_key": "todo",
        "business_key": "synthetic-business-key", "status": "CLAIMED",
    })
    writer.execute({
        "operation": "link_external_task", "action_id": canonical["action_id"],
        "external_task_id": "SYNTHETIC-TASK-1", "status": "EXTERNAL_CREATED",
    })
    duplicate = writer.execute({
        "operation": "claim_action", "conversation_id": "synthetic-conversation",
        "message_id": "synthetic-source-2", "action_key": "todo",
        "business_key": "synthetic-business-key", "status": "CLAIMED",
    })
    replay = writer.execute({
        "operation": "claim_action", "conversation_id": "synthetic-conversation",
        "message_id": "synthetic-source-2", "action_key": "todo",
        "business_key": "synthetic-business-key", "status": "CLAIMED",
    })

    assert duplicate["created"] is True
    assert duplicate["action_id"] != canonical["action_id"]
    assert duplicate["duplicate_of_action_id"] == canonical["action_id"]
    assert duplicate["canonical_external_task_id"] == "SYNTHETIC-TASK-1"
    assert duplicate["effective_external_task_id"] == "SYNTHETIC-TASK-1"
    assert duplicate["row"]["external_task_id"] is None
    assert replay["action_id"] == duplicate["action_id"]
    assert len([row for row in db.actions.values() if row.get("external_task_id") == "SYNTHETIC-TASK-1"]) == 1


def test_child_claim_is_bound_to_the_persisted_parent_source_and_task():
    writer, db, _requests = _writer_and_db()
    parent = writer.execute({
        "operation": "claim_action", "conversation_id": "synthetic-conversation",
        "message_id": "synthetic-child-source", "action_key": "todo",
        "business_key": "synthetic-business", "status": "CLAIMED",
    })
    writer.execute({
        "operation": "link_external_task", "action_id": parent["action_id"],
        "external_task_id": "SYNTHETIC-PARENT", "status": "EXTERNAL_CREATED",
    })
    candidate = detect_composite_candidate(COMPOSITE_TEXT)
    from composite_tasks import propose_child_actions
    child_request = propose_child_actions(
        parent_task_id="SYNTHETIC-PARENT", conversation_id="synthetic-conversation",
        message_id="synthetic-child-source", business_scope="synthetic-business",
        components=[candidate["components"][0]],
    )[0]
    request = {
        **{key: child_request[key] for key in (
            "operation", "contract_version", "parent_task_id", "conversation_id", "message_id",
            "business_scope", "scope_key", "action_key",
        )},
        "parent_action_id": parent["action_id"], "status": "CLAIMED",
    }
    child = writer.execute(request)
    replay = writer.execute(request)

    assert child["created"] is True
    assert replay["created"] is False
    assert replay["action_id"] == child["action_id"]
    assert db.actions[child["action_id"]]["parent_action_id"] == parent["action_id"]
    with pytest.raises(Exception, match="child_parent_link_or_source_mismatch"):
        writer.execute({**request, "parent_task_id": "SYNTHETIC-OTHER-PARENT"})
    with pytest.raises(Exception, match="child_parent_link_or_source_mismatch"):
        writer.execute({**request, "parent_action_id": child["action_id"]})


def test_candidate_database_fake_enforces_external_task_unique_key():
    writer, db, _requests = _writer_and_db()
    actions = []
    for index in range(2):
        actions.append(writer.execute({
            "operation": "claim_action", "conversation_id": "synthetic-conversation",
            "message_id": f"synthetic-unique-{index}", "action_key": "todo",
            "business_key": f"synthetic-business-{index}", "status": "CLAIMED",
        }))
    writer.execute({
        "operation": "link_external_task", "action_id": actions[0]["action_id"],
        "external_task_id": "SYNTHETIC-UNIQUE-TASK", "status": "EXTERNAL_CREATED",
    })
    with pytest.raises(Exception, match="constraint_violation"):
        writer.execute({
            "operation": "link_external_task", "action_id": actions[1]["action_id"],
            "external_task_id": "SYNTHETIC-UNIQUE-TASK", "status": "EXTERNAL_CREATED",
        })
    assert [row["external_task_id"] for row in db.actions.values()].count("SYNTHETIC-UNIQUE-TASK") == 1


def test_concurrent_business_claims_keep_two_sources_and_one_canonical_owner():
    writer_class, _policy, pymysql = load_candidate_writer()
    db = FakeMySQLDatabase(pymysql.IntegrityError)
    db.business_select_barrier = __import__("threading").Barrier(2)
    db.business_barrier_remaining = 2

    def claim(index):
        return writer_class(connect_factory=db.connect).execute({
            "operation": "claim_action", "conversation_id": "synthetic-conversation",
            "message_id": f"synthetic-concurrent-{index}", "action_key": "todo",
            "business_key": "synthetic-concurrent-business", "status": "CLAIMED",
        })

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(claim, (1, 2)))

    assert len({row["action_id"] for row in results}) == 2
    assert sum(1 for row in db.actions.values() if row.get("business_key") == "synthetic-concurrent-business") == 1
    assert sum(1 for row in db.actions.values() if row.get("duplicate_of_action_id")) == 1
    assert len({(row["conversation_id"], row["message_id"]) for row in db.actions.values()}) == 2


def test_candidate_writer_fails_closed_before_any_write_when_extension_schema_is_missing():
    writer_class, _policy, pymysql = load_candidate_writer()
    db = FakeMySQLDatabase(pymysql.IntegrityError)
    db.schema_columns.discard("rule_audit")
    writer = writer_class(connect_factory=db.connect)

    with pytest.raises(Exception, match="writer_schema_contract_unsupported"):
        writer.execute({
            "operation": "record_message", "conversation_id": "synthetic-conversation",
            "message_id": "synthetic-no-schema", "group_name": "合成群",
            "message_time": "2026-10-18 10:00:00", "text": "合成消息",
        })
    assert not db.messages


def test_candidate_writer_fails_closed_when_parent_column_type_is_not_unsigned_nullable_bigint():
    writer_class, _policy, pymysql = load_candidate_writer()
    db = FakeMySQLDatabase(pymysql.IntegrityError)
    db.schema_column_types["parent_action_id"] = ("bigint", "bigint", "NO")
    writer = writer_class(connect_factory=db.connect)

    with pytest.raises(Exception, match="writer_schema_contract_unsupported"):
        writer.execute({
            "operation": "record_message", "conversation_id": "synthetic-conversation",
            "message_id": "synthetic-wrong-column-contract", "group_name": "合成群",
            "message_time": "2026-10-18 10:00:00", "text": "合成消息",
        })
    assert not db.messages


def test_event_processor_fails_closed_for_unupgraded_local_writer_without_unknown_operations():
    operations = []

    class LegacyRuntimeWriter(event_worker.WorkWriter):
        def execute(self, request):
            operations.append(dict(request))
            return {"ok": True}

    dws = SyntheticDws()
    processor = _processor(LegacyRuntimeWriter(), dws)
    with patch("event_worker.plan_batch", side_effect=_plan_batch):
        plan = processor.process_batch([_message("synthetic-old-writer")])[0]

    assert plan["decision"] == "REVIEW"
    assert plan["reason"] == "writer_extension_contract_unsupported"
    assert operations == []
    assert not dws.calls
