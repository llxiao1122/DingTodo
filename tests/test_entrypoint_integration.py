from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import types

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import event_worker  # noqa: E402
import listener_engine  # noqa: E402
import polling_resident  # noqa: E402
from event_worker import OneShotHistoryPoller  # noqa: E402


TZ = timezone(timedelta(hours=8))
RUN_NOW = datetime(2026, 10, 9, 10, 0, tzinfo=TZ)
FLOOR = "2026-10-08T00:00:00+08:00"


class SyntheticAdapter:
    def __init__(self):
        self.config = {
            "config_calendar": [{"dt": "2026-10-09", "day_type": "workday"}],
            "config_group": [{
                "group_name": "铁炉西工班群", "conversation_id": "synthetic-conversation", "purpose": "todo_scan",
            }],
            "config_staff": [[1, "李林骁", "STALE-LI", "leader"], [2, "陈红洁", "STALE-CHEN", "executor"]],
            "active_routing_loaded": True,
            "responsibility_rules": {"drill": "李林骁"},
            "handover_evidence": [],
        }
        self.requests = []

    def query(self, request):
        self.requests.append(dict(request))
        if request["view"] == "runtime_config":
            return {"config": self.config}
        return {"rows": [], "has_more": False}


class SyntheticReader:
    def __init__(self):
        self.calls = []

    def read_group(self, **kwargs):
        self.calls.append(dict(kwargs))
        return {
            "complete": True,
            "source_order_provenance": "complete_dws_chronological_page",
            "context_status": "complete",
            "messages": [{
                "conversation_id": "synthetic-conversation", "message_id": "synthetic-source-message",
                "group": "铁炉西工班群", "sender_id": "synthetic-sender", "sender": "合成发件人",
                "create_time": "2026-10-09 09:30:00",
                "text": "宣传简报：请铁炉西工班于10月20日前完成消防活动组织反馈。",
                "resource_refs": [],
            }],
        }


class SyntheticWriter:
    def __init__(self):
        self.requests = []

    def execute(self, request):
        self.requests.append(dict(request))
        if request["operation"] == "claim_action":
            return {"created": True, "action_id": 41}
        return {"ok": True, "committed": True, "action_id": request.get("action_id"),
                "row": {"status": request.get("status"), "external_task_id": request.get("external_task_id")}, "created": True, "changed": True}


class SyntheticDws:
    def __init__(self):
        self.calls = []
        self.plan = None

    def get_self(self):
        return {"name": "李林骁", "user_id": "LIVE-LI"}

    def resolve_exact(self, names):
        self.calls.append(("resolve_exact", list(names)))
        return {name: "LIVE-" + name for name in names}

    def search_todos(self, query):
        self.calls.append(("search_todos", query))
        return []

    def create_todo(self, plan, priority=20):
        self.calls.append(("create_todo", plan["title"], priority))
        self.plan = dict(plan)
        return {"task_id": "synthetic-task-id"}

    def add_source_comment(self, task_id, comment):
        self.calls.append(("add_source_comment", task_id))

    def verify_source_comment(self, task_id, comment):
        self.calls.append(("verify_source_comment", task_id))

    def get_todo(self, task_id):
        self.calls.append(("get_todo", task_id))
        return {
            "taskId": task_id,
            "subject": self.plan["title"],
            "isDone": False,
            "dueTime": self.plan["due_iso"],
            "priority": self.plan["priority"],
            "executorInfos": [
                {"name": name, "userId": user_id}
                for name, user_id in zip(self.plan["executor_names"], self.plan["executor_ids"])
            ],
        }


def _poller(adapter, reader, writer, dws):
    return OneShotHistoryPoller(
        adapter=adapter,
        reader=reader,
        dws=dws,
        writer=writer,
        resolver=None,
        mode="production",
        write_ledger=True,
        allow_external_writes=True,
        not_before=FLOOR,
        now_factory=lambda: RUN_NOW,
    )


def _assert_pipeline(poller, writer, dws):
    assert len(poller.reader.calls) == 1
    operations = [row["operation"] for row in writer.requests]
    assert "record_message" in operations
    assert "claim_action" in operations
    assert "link_external_task" in operations
    assert "advance_scan_state" in operations
    search_index = next(index for index, call in enumerate(dws.calls) if call[0] == "search_todos")
    create_index = next(index for index, call in enumerate(dws.calls) if call[0] == "create_todo")
    assert search_index < create_index
    assert poller.dws.plan["route_owner"] == "陈红洁"
    assert poller.dws.plan["executor_names"] == ["陈红洁", "李林骁"]
    assert poller.dws.plan["executor_ids"] == ["LIVE-陈红洁", "LIVE-LI"]
    assert poller.dws.plan["assignment_audit"]["conflict_resolution"] == "confirmed_static_rule_has_priority_over_legacy_matrix"


def test_resident_main_runs_current_production_planner_writer_and_business_search(tmp_path, monkeypatch, capsys):
    adapter, reader, writer, dws = SyntheticAdapter(), SyntheticReader(), SyntheticWriter(), SyntheticDws()
    poller = _poller(adapter, reader, writer, dws)
    captured_args = {}

    def build(args):
        captured_args.update(vars(args))
        return poller

    monkeypatch.setattr(event_worker, "build_history_poller", build)
    state_dir = tmp_path / "resident-state"
    result = polling_resident.main([
        "--state-dir", str(state_dir), "--interval-seconds", "900", "--max-cycles", "1",
        "--mode", "production", "--write-ledger", "--allow-external-writes",
        "--not-before", FLOOR, "--resource-dir", str(tmp_path / "resources"),
    ])

    assert result == 0
    assert captured_args["interval_seconds"] == 900
    assert captured_args["mode"] == "production"
    assert captured_args["write_ledger"] is True
    assert captured_args["allow_external_writes"] is True
    _assert_pipeline(poller, writer, dws)
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["status"] == "COMPLETED"


def test_daily_recovery_main_keeps_worker_argv_and_enters_same_writer_pipeline(tmp_path, monkeypatch, capsys):
    adapter, reader, writer, dws = SyntheticAdapter(), SyntheticReader(), SyntheticWriter(), SyntheticDws()
    poller = _poller(adapter, reader, writer, dws)
    workflows = types.ModuleType("workflows")
    workflows.__path__ = []
    adapter_package = types.ModuleType("workflows.polardb_work_adapter")
    adapter_package.__path__ = []
    query_module = types.ModuleType("workflows.polardb_work_adapter.query")
    query_module.WorkQueryAdapter = lambda: adapter
    monkeypatch.setitem(sys.modules, "workflows", workflows)
    monkeypatch.setitem(sys.modules, "workflows.polardb_work_adapter", adapter_package)
    monkeypatch.setitem(sys.modules, "workflows.polardb_work_adapter.query", query_module)
    worker_argv = []

    def run_worker(argv, **_kwargs):
        worker_argv.extend(argv)
        return poller.run_once()

    monkeypatch.setattr(listener_engine, "run_worker_subprocess", run_worker)
    state_dir = tmp_path / "recovery-state"
    result = listener_engine.main([
        "--state-dir", str(state_dir), "--recover-once", "--mode", "production",
        "--write-ledger", "--allow-external-writes", "--jev-v2-policy",
        "--semantic-fallback", "--title-fields", "--date-role-fallback", "--supervised-rollout",
        "--not-before", FLOOR, "--resource-dir", str(tmp_path / "resources"),
    ])

    assert result == 0
    assert "--daily-recovery" in worker_argv
    for flag in (
        "--mode", "production", "--write-ledger", "--allow-external-writes", "--jev-v2-policy",
        "--semantic-fallback", "--title-fields", "--date-role-fallback", "--supervised-rollout",
        "--not-before", FLOOR,
    ):
        assert flag in worker_argv
    _assert_pipeline(poller, writer, dws)
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["status"] == "COMPLETED"
