from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from event_lifecycle import normalize_todo_lifecycle_event, sync_todo_lifecycle_events  # noqa: E402
from event_worker import OneShotHistoryPoller  # noqa: E402
from listener_engine import (  # noqa: E402
    AtomicPendingState,
    CatchupCoordinator,
    DwsEventTransport,
    ListenerSupervisor,
    TODO_EVENT_KEYS,
    TodoEventRouter,
    _argument_parser,
    todo_consume_argv,
)


TZ = timezone(timedelta(hours=8))


def test_todo_event_contract_keeps_only_stable_ids_and_exact_keys():
    event = normalize_todo_lifecycle_event({
        "type": "user_todo_task_update", "event_id": "synthetic-event-1",
        "task_id": "synthetic-task-1", "subject": "must not be persisted",
    })

    assert event == {
        "event_key": "user_todo_task_update",
        "event_id": "synthetic-event-1",
        "task_id": "synthetic-task-1",
    }
    assert todo_consume_argv("dws-test") == [
        "dws-test", "event", "consume",
        "user_todo_task_create", "user_todo_task_update", "user_todo_task_delete",
        "--role-types", "creator,executor,participant", "--flatten", "-f", "ndjson",
    ]
    with pytest.raises(ValueError, match="unsupported_todo_event"):
        normalize_todo_lifecycle_event({
            "type": "user_im_message_read_group", "event_id": "e", "task_id": "t",
        })


def test_todo_event_router_queues_durably_and_deduplicates_after_restart(tmp_path):
    state = AtomicPendingState(tmp_path / "state")
    notices = []
    router = TodoEventRouter(state, notify=lambda: notices.append("wake"))
    payload = {
        "type": "user_todo_task_update", "event_id": "synthetic-event-1",
        "task_id": "synthetic-task-1", "subject": "not stored",
    }

    assert router.handle(payload) is True
    assert router.handle(payload) is False
    restored = AtomicPendingState(tmp_path / "state")
    assert restored.task_event_snapshot() == {
        "synthetic-event-1": {
            "event_key": "user_todo_task_update", "event_id": "synthetic-event-1",
            "task_id": "synthetic-task-1",
        }
    }
    file_text = (tmp_path / "state" / "pending.json").read_text()
    assert "not stored" not in file_text
    assert notices == ["wake"]
    snapshot = restored.task_event_snapshot()
    restored.acknowledge_task_events(snapshot)
    assert restored.task_event_snapshot() == {}
    assert router.handle(payload) is False


def test_coordinator_runs_todo_only_wakeup_under_the_existing_work_lock(tmp_path):
    state = AtomicPendingState(tmp_path / "state")
    state.enqueue_todo_event({
        "event_key": "user_todo_task_create", "event_id": "synthetic-event-2",
        "task_id": "synthetic-task-2",
    })
    calls = []

    def run_worker():
        calls.append((tmp_path / "state" / "work.lock").exists())
        return {"status": "COMPLETED"}

    coordinator = CatchupCoordinator(state, run_worker, tmp_path / "state" / "work.lock")
    outcome = coordinator.run_once()

    assert outcome["status"] == "COMPLETED"
    assert calls == [True]
    assert state.task_event_snapshot() == {}
    assert state.completed_task_event("synthetic-event-2") is True


def test_supervisor_routes_event_to_shared_queue_and_runs_one_locked_worker(tmp_path):
    state = AtomicPendingState(tmp_path / "state")
    observed = []

    def runner():
        observed.append(state.task_event_snapshot())
        return {"status": "COMPLETED"}

    coordinator = CatchupCoordinator(state, runner, tmp_path / "state" / "work.lock")
    supervisor = ListenerSupervisor(
        scope_loader=lambda: ("synthetic-conversation",), state=state,
        coordinator=coordinator, transport=object(), coalesce_seconds=0,
        enable_todo_events=True,
    )
    payload = {
        "type": "user_todo_task_create", "event_id": "synthetic-event-supervisor",
        "task_id": "synthetic-task-supervisor", "subject": "not queued",
    }

    assert supervisor._todo_event_callback()(payload) is True
    notice = supervisor.notices.get_nowait()
    supervisor._process_notice_batch(notice)

    assert observed[0]["synthetic-event-supervisor"]["task_id"] == "synthetic-task-supervisor"
    assert state.task_event_snapshot() == {}
    assert state.completed_task_event("synthetic-event-supervisor") is True


def test_todo_transport_subscribes_one_role_scoped_set_and_requires_all_receipts(tmp_path):
    calls = []

    class FakeConsumer:
        def __init__(self, argv, consumer_id, on_event, on_ready):
            calls.append(argv)
            self.consumer_id = consumer_id
        def start(self):
            pass
        def wait_ready(self, _timeout, _cancel):
            return True
        def wait(self, _timeout=0):
            return None
        def subscription_receipts(self):
            return [
                {"subscribe_id": f"synthetic-sub-{index}", "ownership": "new"}
                for index, _key in enumerate(TODO_EVENT_KEYS)
            ]
        def receipts_complete(self):
            return True
        def failure_metadata(self):
            return {}
        def close(self, **_kwargs):
            return {"status": "graceful"}

    transport = DwsEventTransport(
        tmp_path / "state", executable="dws-test", consumer_factory=FakeConsumer,
    )
    consumer = transport.start_todo_events(lambda _event: None, lambda *_args: None)

    assert consumer is not None
    assert calls == [todo_consume_argv("dws-test")]
    assert consumer.consumer_id == "todo-lifecycle"


def test_task_lifecycle_sync_exact_reads_back_before_existing_writer_update():
    events = [{
        "event_key": "user_todo_task_update", "event_id": "synthetic-event-3",
        "task_id": "synthetic-task-3",
    }]
    actions = [{
        "action_id": 31, "external_task_id": "synthetic-task-3", "status": "EXTERNAL_CREATED",
        "attempt_count": 2,
    }]

    class Dws:
        def __init__(self):
            self.calls = []
        def get_todo(self, task_id):
            self.calls.append(task_id)
            return {"taskId": task_id, "isDone": True, "subject": "private title"}

    class Writer:
        def __init__(self):
            self.requests = []
        def execute(self, request):
            self.requests.append(dict(request))
            return {"saved": True}

    dws, writer = Dws(), Writer()
    result = sync_todo_lifecycle_events(events, actions, dws=dws, writer=writer)

    assert result == {"processed": 1, "ignored": 0, "errors": []}
    assert dws.calls == ["synthetic-task-3"]
    assert writer.requests == [{
        "operation": "save_action_result", "action_id": 31, "status": "SUCCESS",
        "attempt_count": 2, "last_error": None,
    }]


def test_task_lifecycle_readback_mismatch_and_delete_without_tombstone_fail_closed():
    events = [
        {"event_key": "user_todo_task_update", "event_id": "e1", "task_id": "t1"},
        {"event_key": "user_todo_task_delete", "event_id": "e2", "task_id": "t2"},
    ]
    actions = [
        {"action_id": 1, "external_task_id": "t1", "status": "EXTERNAL_CREATED"},
        {"action_id": 2, "external_task_id": "t2", "status": "EXTERNAL_CREATED"},
    ]

    class Dws:
        def get_todo(self, task_id):
            if task_id == "t1":
                return {"taskId": "different-task", "isDone": False}
            return {"taskId": task_id, "isDone": False}

    class Writer:
        requests = []
        def execute(self, request):
            self.requests.append(dict(request))
            return {"saved": True}

    writer = Writer()
    result = sync_todo_lifecycle_events(events, actions, dws=Dws(), writer=writer)
    assert result["processed"] == 0
    assert result["errors"] == ["todo_readback_mismatch", "todo_delete_tombstone_unverified"]
    assert writer.requests == []


def test_one_shot_pipeline_syncs_queued_todo_events_through_the_existing_writer():
    class Adapter:
        def query(self, request):
            if request["view"] == "runtime_config":
                return {"config": {
                    "config_calendar": [{"dt": "2026-10-18", "day_type": "workday"}],
                    "config_group": [{
                        "group_name": "合成工班群", "conversation_id": "synthetic-conversation",
                        "purpose": "todo_scan",
                    }],
                }}
            if request["view"] == "actions":
                return {"rows": [{
                    "action_id": 55, "conversation_id": "synthetic-conversation",
                    "message_id": "synthetic-source", "external_task_id": "synthetic-task-5",
                    "status": "EXTERNAL_CREATED", "attempt_count": 1,
                }], "has_more": False}
            return {"rows": [], "has_more": False}

    class Reader:
        def read_group(self, **_kwargs):
            return {
                "complete": True, "source_order_provenance": "complete_dws_chronological_page",
                "messages": [],
            }

    class Dws:
        def __init__(self):
            self.reads = []
        def get_self(self):
            return {"name": "合成执行人", "user_id": "synthetic-self"}
        def get_todo(self, task_id):
            self.reads.append(task_id)
            return {"taskId": task_id, "isDone": False}

    class Writer:
        def __init__(self):
            self.requests = []
        def execute(self, request):
            self.requests.append(dict(request))
            return {"saved": True}

    dws, writer = Dws(), Writer()
    poller = OneShotHistoryPoller(
        adapter=Adapter(), reader=Reader(), dws=dws, writer=writer,
        mode="production", write_ledger=True, allow_external_writes=True,
        now_factory=lambda: datetime(2026, 10, 18, 10, 0, tzinfo=TZ),
        todo_lifecycle_events=[{
            "event_key": "user_todo_task_update", "event_id": "synthetic-event-5",
            "task_id": "synthetic-task-5",
        }],
    )

    result = poller.run_once()
    assert result["status"] == "COMPLETED"
    assert dws.reads == ["synthetic-task-5"]
    assert writer.requests == [{
        "operation": "save_action_result", "action_id": 55, "status": "EXTERNAL_CREATED",
        "attempt_count": 1, "last_error": None,
    }]


def test_todo_subscriptions_are_opt_in_and_service_candidate_is_closed_by_default():
    args = _argument_parser().parse_args(["--state-dir", "/tmp/synthetic-state"])
    assert args.enable_todo_events is False
    candidate = (ROOT / "deploy" / "dws-unified-event-consumer.service.candidate").read_text()
    assert "ConditionPathExists=" in candidate
    assert "--enable-todo-events" in candidate
    assert "--state-dir %h/.local/state/dws-history-listener" in candidate
    assert "--jev-v2-policy --semantic-fallback --title-fields --date-role-fallback --supervised-rollout" in candidate
    assert "--not-before 2026-10-08T00:00:00+08:00" in candidate
    assert "@CURRENT_MODEL_FLAGS@" not in candidate
    assert "@CURRENT_NOT_BEFORE_ARGS@" not in candidate
