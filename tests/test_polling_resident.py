from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import subprocess
import sys
import threading
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import event_worker
from event_worker import OneShotHistoryPoller
from jev_v2 import JevV2Resolver
from listener_engine import AtomicPendingState, WorkLock


TZ = timezone(timedelta(hours=8))
NOW = datetime(2026, 10, 9, 10, 0, tzinfo=TZ)
FLOOR = datetime(2026, 10, 8, 0, 0, tzinfo=TZ)


class FakeAdapter:
    def __init__(self, *, day_type="workday", scan_rows=None, actions=None):
        self.day_type = day_type
        self.scan_rows = list(scan_rows or [])
        self.actions = list(actions or [])
        self.requests = []

    def query(self, request):
        self.requests.append(dict(request))
        view = request["view"]
        if view == "runtime_config":
            return {"config": {
                "config_calendar": [{"dt": NOW.date().isoformat(), "day_type": self.day_type}],
                "config_group": [{
                    "group_name": "synthetic-group",
                    "conversation_id": "synthetic-conversation",
                    "purpose": "todo_scan",
                }],
                "config_staff": [],
                "active_routing_loaded": True,
            }}
        rows = self.scan_rows if view == "scan_state" else self.actions
        return {"rows": rows, "has_more": False}


class FakeReader:
    def __init__(self, *, complete=True):
        self.complete = complete
        self.calls = []

    def read_group(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "complete": self.complete,
            "source_order_provenance": "complete_dws_chronological_page",
            "messages": [],
        }


class FakeDws:
    def get_self(self):
        return {"name": "synthetic-self", "user_id": "synthetic-self-id"}


class FakeWriter:
    def __init__(self):
        self.requests = []

    def execute(self, request):
        self.requests.append(dict(request))
        return {"created": False, "changed": False}


def make_poller(*, day_type="workday", complete=True, model_calls=None, now=NOW, scan_rows=None):
    adapter = FakeAdapter(day_type=day_type, scan_rows=scan_rows)
    reader = FakeReader(complete=complete)
    writer = FakeWriter()
    model_calls = model_calls if model_calls is not None else []
    resolver = JevV2Resolver(
        new_policy=True,
        transport=lambda request, _timeout: model_calls.append(request) or {"answers": {}},
    )
    poller = OneShotHistoryPoller(
        adapter=adapter,
        reader=reader,
        dws=FakeDws(),
        writer=writer,
        resolver=resolver,
        mode="production",
        write_ledger=True,
        allow_external_writes=True,
        supervised_rollout=True,
        not_before=FLOOR,
        now_factory=lambda: now,
    )
    return poller, adapter, reader, writer, model_calls


def make_state(tmp_path):
    return AtomicPendingState(tmp_path / "state")


def test_resident_cli_defaults_to_fifteen_minutes_and_approved_floor():
    from polling_resident import build_argument_parser

    args = build_argument_parser().parse_args(["--state-dir", "/tmp/synthetic-state"])

    assert args.interval_seconds == 900
    assert args.max_cycles is None
    assert args.not_before == FLOOR
    assert "--max-cycles" in build_argument_parser().format_help()


def test_unit_example_selects_the_resident_and_shared_production_configuration():
    unit = (
        Path(__file__).resolve().parents[1]
        / "deploy"
        / "dws-history-polling-resident.service.example"
    ).read_text()

    assert "polling_resident.py" in unit
    assert "--interval-seconds 900" in unit
    assert "--state-dir %h/.local/state/dws-history-listener" in unit
    assert "--mode production --write-ledger --allow-external-writes" in unit
    for flag in (
        "--jev-v2-policy", "--semantic-fallback", "--title-fields",
        "--date-role-fallback", "--supervised-rollout",
        "--not-before 2026-10-08T00:00:00+08:00",
    ):
        assert flag in unit
    assert "OnCalendar=" not in unit
    assert "20min" not in unit


def test_isolated_runpy_help_works_from_an_unrelated_directory():
    entry = Path(__file__).resolve().parents[1] / "polling_resident.py"
    wrapper = (
        "import runpy, sys; entry=sys.argv[1]; "
        "sys.argv=[entry, '--help']; runpy.run_path(entry, run_name='__main__')"
    )
    result = subprocess.run(
        [sys.executable, "-I", "-c", wrapper, str(entry)],
        cwd="/tmp",
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )

    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    assert "usage:" in result.stdout.lower()
    assert "--interval-seconds" in result.stdout


def test_resident_refuses_to_lower_the_approved_rollout_floor():
    from polling_resident import _validate_resident_options, build_argument_parser

    parser = build_argument_parser()
    args = parser.parse_args([
        "--state-dir", "/tmp/synthetic-state",
        "--not-before", "2026-10-07T23:59:59+08:00",
    ])

    with pytest.raises(SystemExit) as error:
        _validate_resident_options(args, parser)
    assert error.value.code == 2


def test_scheduler_skips_missed_ticks_after_a_long_cycle_without_spinning(tmp_path):
    from polling_resident import run_resident

    state = make_state(tmp_path)
    lock_path = tmp_path / "state" / "work.lock"
    stop = threading.Event()
    clock = [100.0]
    waits = []
    starts = []

    class SlowPoller:
        def run_once(self):
            starts.append(clock[0])
            if len(starts) == 1:
                clock[0] += 2000.0
            return {"status": "COMPLETED", "messages": 0, "stats": {"llm_calls": 0}}

    def wait(seconds):
        waits.append(seconds)
        clock[0] += seconds
        return stop.is_set()

    result = run_resident(
        SlowPoller(), state=state, scope_loader=lambda: ["synthetic-conversation"],
        lock_path=lock_path, interval_seconds=900, max_cycles=2,
        stop_event=stop, monotonic=lambda: clock[0], wait=wait,
        output=lambda _record: None,
    )

    assert result == 0
    assert starts == [100.0, 2800.0]
    assert waits == [700.0]


def test_resident_wait_and_retry_backoff_are_interruptible_and_bounded(tmp_path):
    from polling_resident import run_resident

    state = make_state(tmp_path)
    stop = threading.Event()
    clock = [0.0]
    statuses = iter(("REVIEW", "REVIEW", "COMPLETED"))
    attempts = []
    waits = []

    class RetryPoller:
        def run_once(self):
            attempts.append(clock[0])
            return {"status": next(statuses), "messages": 0}

    def wait(seconds):
        waits.append(seconds)
        clock[0] += seconds
        return stop.is_set()

    result = run_resident(
        RetryPoller(), state=state, scope_loader=lambda: ["synthetic-conversation"],
        lock_path=tmp_path / "state" / "work.lock", interval_seconds=900,
        max_cycles=3, stop_event=stop, monotonic=lambda: clock[0], wait=wait,
        retry_limit=3, retry_base_seconds=30, output=lambda _record: None,
    )

    assert result == 0
    assert attempts == [0.0, 30.0, 90.0]
    assert waits == [30.0, 60.0]
    assert state.snapshot() == {}

    canceled = threading.Event()
    canceled_clock = [0.0]
    calls = []

    class OnePoller:
        def run_once(self):
            calls.append("run")
            return {"status": "COMPLETED"}

    def cancel_wait(seconds):
        canceled_clock[0] += seconds
        canceled.set()
        return True

    run_resident(
        OnePoller(), state=make_state(tmp_path / "cancel"),
        scope_loader=lambda: ["synthetic-conversation"],
        lock_path=tmp_path / "cancel" / "state" / "work.lock",
        interval_seconds=900, max_cycles=3, stop_event=canceled,
        monotonic=lambda: canceled_clock[0], wait=cancel_wait,
        output=lambda _record: None,
    )
    assert calls == ["run"]


def test_resident_and_daily_recovery_share_work_lock_without_overlapping(tmp_path):
    from polling_resident import run_resident

    state = make_state(tmp_path)
    lock_path = tmp_path / "state" / "work.lock"
    called = threading.Event()
    stop = threading.Event()

    class Poller:
        def run_once(self):
            called.set()
            return {"status": "COMPLETED"}

    result = []
    with WorkLock(lock_path):
        worker = threading.Thread(target=lambda: result.append(run_resident(
            Poller(), state=state, scope_loader=lambda: ["synthetic-conversation"],
            lock_path=lock_path, interval_seconds=900, max_cycles=1,
            stop_event=stop, output=lambda _record: None,
        )))
        worker.start()
        assert not called.wait(0.1)
    worker.join(timeout=2)

    assert not worker.is_alive()
    assert called.is_set()
    assert result == [0]
    assert state.snapshot() == {}


def test_sigterm_stop_interrupts_wait_for_shared_daily_recovery_lock(tmp_path):
    from polling_resident import run_resident

    state = make_state(tmp_path)
    lock_path = tmp_path / "state" / "work.lock"
    stop = threading.Event()
    called = threading.Event()
    result = []

    class Poller:
        def run_once(self):
            called.set()
            return {"status": "COMPLETED"}

    with WorkLock(lock_path):
        worker = threading.Thread(target=lambda: result.append(run_resident(
            Poller(), state=state, scope_loader=lambda: ["synthetic-conversation"],
            lock_path=lock_path, interval_seconds=900, max_cycles=1,
            stop_event=stop, output=lambda _record: None,
        )))
        worker.start()
        assert not called.wait(0.1)
        stop.set()
    worker.join(timeout=2)

    assert not worker.is_alive()
    assert result == [0]
    assert not called.is_set()


def test_factory_constructs_providers_once_and_resident_reuses_them(tmp_path):
    from polling_resident import run_resident

    adapter = FakeAdapter()
    reader = FakeReader()
    dws = FakeDws()
    writer = FakeWriter()
    args = SimpleNamespace(
        mode="production", write_ledger=True, allow_external_writes=True,
        llm_jev=False, jev_v2_policy=False, semantic_fallback=False,
        title_fields=False, date_role_fallback=False, supervised_rollout=False,
        not_before=FLOOR, resource_dir="/tmp/synthetic-resource-dir",
    )

    with (
        patch.object(event_worker, "WorkQueryAdapter", return_value=adapter) as adapter_ctor,
        patch.object(event_worker, "DingTalkReader", return_value=reader) as reader_ctor,
        patch.object(event_worker, "DwsClient", return_value=dws) as dws_ctor,
        patch.object(event_worker, "WorkWriter", return_value=writer) as writer_ctor,
        patch.object(event_worker, "configure_writer_environment") as configure,
    ):
        poller = event_worker.build_history_poller(args)
        poller.now_factory = lambda: NOW
        assert poller.adapter is adapter
        assert poller.reader is reader
        assert poller.dws is dws
        assert poller.writer is writer
        assert run_resident(
            poller, state=make_state(tmp_path),
            scope_loader=lambda: ["synthetic-conversation"],
            lock_path=tmp_path / "state" / "work.lock", interval_seconds=900,
            max_cycles=2, stop_event=threading.Event(),
            monotonic=lambda: 0.0, wait=lambda _seconds: False,
            output=lambda _record: None,
        ) == 0

    adapter_ctor.assert_called_once_with()
    reader_ctor.assert_called_once_with()
    dws_ctor.assert_called_once_with(confirmed_official_writes=True)
    writer_ctor.assert_called_once_with()
    configure.assert_called_once_with()
    assert len(reader.calls) == 2


def test_real_poller_zero_new_sources_makes_no_model_calls_across_cycles(tmp_path):
    from polling_resident import run_resident

    poller, adapter, reader, writer, model_calls = make_poller()
    state = make_state(tmp_path)

    result = run_resident(
        poller, state=state, scope_loader=lambda: ["synthetic-conversation"],
        lock_path=tmp_path / "state" / "work.lock", interval_seconds=900,
        max_cycles=2, stop_event=threading.Event(), monotonic=lambda: 0.0,
        wait=lambda _seconds: False, output=lambda _record: None,
    )

    assert result == 0
    assert len(reader.calls) == 2
    assert len([row for row in adapter.requests if row["view"] == "runtime_config"]) == 2
    assert model_calls == []
    assert writer.requests == []
    assert state.snapshot() == {}


def test_calendar_gate_preserves_pending_and_does_not_read_on_off_day(tmp_path):
    from polling_resident import run_resident

    poller, _adapter, reader, _writer, model_calls = make_poller(day_type="weekend")
    state = make_state(tmp_path)

    run_resident(
        poller, state=state, scope_loader=lambda: ["synthetic-conversation"],
        lock_path=tmp_path / "state" / "work.lock", interval_seconds=900,
        max_cycles=1, stop_event=threading.Event(), output=lambda _record: None,
    )

    assert reader.calls == []
    assert model_calls == []
    assert set(state.snapshot()) == {"synthetic-conversation"}


@pytest.mark.parametrize("moment", [
    datetime(2026, 10, 9, 8, 59, tzinfo=TZ),
    datetime(2026, 10, 9, 17, 30, tzinfo=TZ),
])
def test_resident_keeps_the_existing_ordinary_execution_window(tmp_path, moment):
    from polling_resident import run_resident

    poller, _adapter, reader, _writer, model_calls = make_poller(now=moment)
    state = make_state(tmp_path)
    run_resident(
        poller, state=state, scope_loader=lambda: ["synthetic-conversation"],
        lock_path=tmp_path / "state" / "work.lock", interval_seconds=900,
        max_cycles=1, stop_event=threading.Event(), output=lambda _record: None,
    )

    assert reader.calls == []
    assert model_calls == []
    assert set(state.snapshot()) == {"synthetic-conversation"}


def test_resident_passes_authoritative_scan_watermark_without_resetting_it(tmp_path):
    from polling_resident import run_resident

    previous = {
        "conversation_id": "synthetic-conversation",
        "last_msg_time": "2026-10-08 12:34:56",
        "last_msg_id": "synthetic-watermark-id",
    }
    poller, adapter, reader, _writer, _model_calls = make_poller(scan_rows=[previous])
    state = make_state(tmp_path)
    run_resident(
        poller, state=state, scope_loader=lambda: ["synthetic-conversation"],
        lock_path=tmp_path / "state" / "work.lock", interval_seconds=900,
        max_cycles=1, stop_event=threading.Event(), output=lambda _record: None,
    )

    assert len(reader.calls) == 1
    assert reader.calls[0]["last_msg_time"] == previous["last_msg_time"]
    assert adapter.scan_rows == [previous]


def test_incomplete_cycle_keeps_pending_then_restart_clears_after_complete(tmp_path):
    from polling_resident import run_resident

    state = make_state(tmp_path)
    lock_path = tmp_path / "state" / "work.lock"
    poller, _adapter, reader, writer, model_calls = make_poller(complete=False)
    result = run_resident(
        poller, state=state, scope_loader=lambda: ["synthetic-conversation"],
        lock_path=lock_path, interval_seconds=900, max_cycles=1,
        stop_event=threading.Event(), output=lambda _record: None,
        retry_limit=0,
    )
    assert result == 2
    assert len(reader.calls) == 1
    assert set(state.snapshot()) == {"synthetic-conversation"}
    assert writer.requests == []
    assert model_calls == []

    restarted, _adapter2, reader2, writer2, model_calls2 = make_poller(complete=True)
    run_resident(
        restarted, state=state, scope_loader=lambda: ["synthetic-conversation"],
        lock_path=lock_path, interval_seconds=900, max_cycles=1,
        stop_event=threading.Event(), output=lambda _record: None,
    )
    assert len(reader2.calls) == 1
    assert state.snapshot() == {}
    assert writer2.requests == []
    assert model_calls2 == []


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__]))
