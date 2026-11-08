import json
import io
import os
import subprocess
import sys
import threading
import time
import types
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from listener_engine import (  # noqa: E402
    AtomicPendingState,
    CatchupCoordinator,
    DwsConsumer,
    ScopeError,
    StateError,
    configured_group_ids,
    consume_argv,
    is_ready_marker,
    normalize_wakeup,
    next_opening_wake,
    SubscriptionRetryStore,
    subscription_retry_limit,
    WakeupRouter,
    run_daily_recovery,
    _worker_argv,
)


def test_official_flat_and_enveloped_aliases_keep_exact_conversation_identity():
    assert normalize_wakeup({"conversation_id": "conv-A"}) == "conv-A"
    assert normalize_wakeup({"data": {"openConversationId": "conv+B", "content": "private"}}) == "conv+B"
    assert normalize_wakeup({"payload": {"conversationId": " conv-C "}}) == " conv-C "


def test_scope_is_only_authoritative_todo_scan_and_fails_closed():
    runtime = {"config": {"config_group": [
        {"conversation_id": "todo", "purpose": "todo_scan"},
        {"conversation_id": "other", "purpose": "reference"},
    ]}}
    assert configured_group_ids(runtime) == ("todo",)
    exact = {"config": {"config_group": [{"conversation_id": " opaque ", "purpose": "todo_scan"}]}}
    assert configured_group_ids(exact) == (" opaque ",)
    with pytest.raises(ScopeError):
        configured_group_ids({"config": {"config_group": []}})
    with pytest.raises(ScopeError):
        configured_group_ids({"config": {"config_group": [
            {"purpose": "todo_scan", "group_name": "missing-id"},
        ]}})


def test_consumer_argv_uses_official_group_scoped_listener_and_ndjson_flags():
    assert consume_argv("opaque-conversation", executable="dws-test") == [
        "dws-test", "event", "+listen-im", "--kind", "group",
        "--chat-id", "opaque-conversation", "-f", "ndjson",
    ]


def test_ready_requires_official_marker_not_subscription_line_or_free_text():
    assert is_ready_marker("[event] ready event_key=user_im_message_receive_group bus_pid=42 subscribe_id=s")
    assert is_ready_marker("[event] ready event_count=2 bus_pid=42")
    assert not is_ready_marker("[event] subscription event_key=user_im_message_receive_group subscribe_id=s")
    assert not is_ready_marker("provider note: [event] ready maybe")


def test_consumer_drains_both_pipes_and_continues_after_invalid_ndjson():
    valid = {"conversation_id": "conv-1", "content": "must stay in process"}
    ready_gate = threading.Event()

    class DelayedLines(io.StringIO):
        def readline(self, size=-1):
            ready_gate.wait(1)
            return super().readline(size)

    class FakeProcess:
        def __init__(self, *_args, **_kwargs):
            self.stdout = DelayedLines("invalid-json\n" + json.dumps(valid) + "\n")
            self.stderr = io.StringIO("[event] ready event_key=user_im_message_receive_group bus_pid=1 subscribe_id=s\n")
            self.exited = False

        def poll(self):
            return 0 if self.exited else None

        def wait(self, timeout=None):
            self.exited = True
            return 0

        def terminate(self):
            self.exited = True

    received = []
    got_event = threading.Event()
    consumer = DwsConsumer(
        consume_argv("conv-1"), "conv-1",
        lambda payload: (received.append(payload), got_event.set()),
        lambda _group, _reconnected: ready_gate.set(),
        popen_factory=FakeProcess,
    )
    consumer.start()
    assert consumer.wait_ready(1)
    assert got_event.wait(1)
    consumer.close()
    assert received == [valid]
    assert consumer.invalid_lines == 1


def test_consumer_drains_and_parses_pre_ready_pretty_startup_errors_from_stdout():
    startup_error = {
        "error": {
            "code": "subscription_blocked",
            "details": {
                "retryable": True,
                "reason": "cooldown",
                "state": "cooldown",
                "retry_after_seconds": 12,
                "trace_id": "trace-123",
                "message": "private token must not escape",
            },
        }
    }
    output_drained = threading.Event()

    class TrackingPipe(io.StringIO):
        def __init__(self, value, on_eof=None):
            super().__init__(value)
            self.on_eof = on_eof

        def readline(self, size=-1):
            line = super().readline(size)
            if not line and self.on_eof:
                self.on_eof.set()
            return line

    class FakeProcess:
        def __init__(self, *_args, **_kwargs):
            self.stdout = TrackingPipe(json.dumps(startup_error, indent=2) + "\n", output_drained)
            self.stderr = TrackingPipe("")

        def poll(self):
            return 0 if output_drained.is_set() else None

        def wait(self, timeout=None):
            return 0

        def terminate(self):
            output_drained.set()

    consumer = DwsConsumer(
        consume_argv("conv-1"), "conv-1", lambda _payload: pytest.fail("pre-ready output is not an event"),
        lambda *_args: None, popen_factory=FakeProcess,
    )
    consumer.start()
    assert len(consumer._threads) == 2
    assert not consumer.wait_ready(1)
    consumer.drain_exit_metadata()
    failure = consumer.failure_metadata()
    assert failure["retryable"] is True
    assert failure["reason"] == "cooldown"
    assert failure["state"] == "cooldown"
    assert failure["retry_after_seconds"] == 12
    assert failure["trace_id"] == "trace-123"
    assert "private token" not in repr(failure)
    consumer.close()


def test_consumer_parses_pre_ready_pretty_error_from_stderr_with_same_whitelist():
    failure = {
        "error": {
            "details": {
                "retryable": False,
                "reason": "invalid_target",
                "state": "terminal_hold",
                "retry_after_seconds": 99_999,
                "request_id": "request-9",
                "log": "private token text",
            }
        }
    }
    drained = threading.Event()

    class Pipe(io.StringIO):
        def __init__(self, value, on_eof=None):
            super().__init__(value)
            self.on_eof = on_eof

        def readline(self, size=-1):
            line = super().readline(size)
            if not line and self.on_eof:
                self.on_eof.set()
            return line

    class FakeProcess:
        def __init__(self, *_args, **_kwargs):
            self.stdout = Pipe("")
            self.stderr = Pipe(json.dumps(failure, indent=2) + "\n", drained)

        def poll(self):
            return 1 if drained.is_set() else None

        def wait(self, timeout=None):
            return 1

        def terminate(self):
            drained.set()

    consumer = DwsConsumer(
        consume_argv("conv-1"), "conv-1", lambda _payload: pytest.fail("failure envelope is not an event"),
        lambda *_args: None, popen_factory=FakeProcess,
    )
    consumer.start()
    assert not consumer.wait_ready(1)
    consumer.drain_exit_metadata()
    assert consumer.failure_metadata() == {
        "retryable": False,
        "reason": "invalid_target",
        "state": "terminal_hold",
        "retry_after_seconds": 99_999,
        "request_id": "request-9",
    }
    assert "private token" not in repr(consumer.failure_metadata())
    consumer.close()


def test_consumer_graceful_close_uses_eof_and_needs_no_remote_stop(tmp_path):
    ready_line = "[event] ready event_key=user_im_message_receive_group bus_pid=42 subscribe_id=sub-owned\n"
    stop_calls = []

    class FakeStdin:
        def close(self):
            pass

    class FakeProcess:
        def __init__(self, *_args, **_kwargs):
            self.stdin = FakeStdin()
            self.stdout = io.StringIO("")
            self.stderr = io.StringIO(ready_line)
            self.exited = False

        def poll(self):
            return 0 if self.exited else None

        def wait(self, timeout=None):
            self.exited = True
            return 0

        def terminate(self):
            raise AssertionError("clean EOF should not need terminate")

    consumer = DwsConsumer(
        consume_argv("conv-1", executable="dws-fake"), "conv-1", lambda _payload: None,
        lambda *_args: None, popen_factory=FakeProcess, cleanup_state_dir=tmp_path / "state",
        stop_runner=lambda argv, **kwargs: (
            stop_calls.append(argv) or subprocess.CompletedProcess(argv, 0, "", "")
        ),
    )
    consumer.start()
    assert consumer.wait_ready(1)
    result = consumer.close()
    assert result["status"] == "graceful"
    assert result["owned_subscriptions"] == ["sub-owned"]
    assert stop_calls == []
    assert consumer.cleanup_ledger.snapshot() == []


def test_consumer_holds_explicit_stdin_pipe_until_close():
    process_rows = []

    class FakeProcess:
        def __init__(self, *_args, **kwargs):
            self.stdin = io.StringIO()
            self.stdout = io.StringIO("")
            self.stderr = io.StringIO("")
            self.exited = False
            process_rows.append((kwargs, self))

        def poll(self):
            return 0 if self.exited else None

        def wait(self, timeout=None):
            self.exited = True
            return 0

        def terminate(self):
            self.exited = True

    consumer = DwsConsumer(
        consume_argv("conv-1"), "conv-1", lambda _payload: None, lambda *_args: None,
        popen_factory=FakeProcess,
    )
    consumer.start()
    kwargs, process = process_rows[0]
    assert kwargs["stdin"] == subprocess.PIPE
    assert not process.stdin.closed
    consumer.close()
    assert process.stdin.closed


def test_transport_ready_then_exit_does_not_clear_retry_budget(tmp_path):
    from listener_engine import DwsEventTransport

    popen_calls = []
    stop_calls = []
    clock = [100.0]
    ready_line = "[event] ready event_key=user_im_message_receive_group bus_pid=42 subscribe_id=sub-1\n"

    class FakeProcess:
        def __init__(self, *_args, **_kwargs):
            self.stdin = io.StringIO()
            self.stdout = io.StringIO("")
            self.stderr = io.StringIO(ready_line)
            original = self.stderr.readline

            def read_ready(size=-1):
                line = original(size)
                if line:
                    self.exited = True
                return line

            self.stderr.readline = read_ready
            self.exited = False

        def poll(self):
            return 1 if self.exited else None

        def wait(self, timeout=None):
            self.exited = True
            return 1

        def terminate(self):
            self.exited = True

    def factory(argv, group_id, on_event, on_ready):
        def popen(*args, **kwargs):
            popen_calls.append(argv)
            return FakeProcess(*args, **kwargs)
        return DwsConsumer(
            argv, group_id, on_event, on_ready, popen_factory=popen,
            cleanup_state_dir=tmp_path / "state",
            stop_runner=lambda command, **kwargs: (
                stop_calls.append(command) or subprocess.CompletedProcess(command, 0, "", "")
            ),
        )

    def advance(delay):
        clock[0] += delay
        return False

    transport = DwsEventTransport(
        tmp_path / "state", consumer_factory=factory, ready_timeout=1,
        wait=advance, now=lambda: clock[0],
    )
    assert transport.start_group("conv-1", lambda _event: None, lambda *_args: None) is None
    assert len(popen_calls) == 2
    assert stop_calls == [
        ["dws", "event", "stop", "sub-1", "--yes"],
        ["dws", "event", "stop", "sub-1", "--yes"],
    ]
    retry = SubscriptionRetryStore(tmp_path / "state")
    assert not retry.begin_attempt("conv-1")


def test_consumer_does_not_dispatch_pre_ready_stdout_while_draining_both_pipes():
    ready_gate = threading.Event()
    stdout_read = threading.Event()
    ready_line = "[event] ready event_key=user_im_message_receive_group bus_pid=42 subscribe_id=sub-1\n"
    event = {"conversation_id": "conv-1", "content": "event body"}

    class StdoutPipe(io.StringIO):
        def readline(self, size=-1):
            stdout_read.set()
            return super().readline(size)

    class StderrPipe(io.StringIO):
        def readline(self, size=-1):
            if self.tell() == 0:
                ready_gate.wait(1)
            return super().readline(size)

    class FakeProcess:
        def __init__(self, *_args, **_kwargs):
            self.stdout = StdoutPipe(json.dumps(event) + "\n")
            self.stderr = StderrPipe(ready_line)

        def poll(self):
            return None

        def wait(self, timeout=None):
            return 0

        def terminate(self):
            ready_gate.set()

    seen = []
    consumer = DwsConsumer(
        consume_argv("conv-1"), "conv-1", seen.append, lambda *_args: None,
        popen_factory=FakeProcess,
    )
    consumer.start()
    assert stdout_read.wait(1)
    assert seen == []
    ready_gate.set()
    assert consumer.wait_ready(1)
    time.sleep(0.02)
    assert seen == []
    consumer.close()


def test_consumer_ready_then_exit_is_not_reported_as_a_live_ready_consumer():
    ready_read = threading.Event()

    class FakeProcess:
        def __init__(self, *_args, **_kwargs):
            self.stdout = io.StringIO("")
            self.stderr = io.StringIO(
                "[event] ready event_key=user_im_message_receive_group bus_pid=42 subscribe_id=sub-1\n"
            )

        def poll(self):
            return 1 if ready_read.is_set() else None

        def wait(self, timeout=None):
            return 1

        def terminate(self):
            pass

    original_readline = FakeProcess.__init__

    class ExitingProcess(FakeProcess):
        def __init__(self, *_args, **_kwargs):
            super().__init__()
            original = self.stderr.readline

            def readline(size=-1):
                line = original(size)
                if line:
                    ready_read.set()
                return line

            self.stderr.readline = readline

    consumer = DwsConsumer(
        consume_argv("conv-1"), "conv-1", lambda _payload: None,
        lambda *_args: None, popen_factory=ExitingProcess,
    )
    consumer.start()
    assert not consumer.wait_ready(1)
    consumer.close()


def test_consumer_cancellation_stops_only_owned_receipt_after_failed_graceful_exit(tmp_path, monkeypatch):
    receipt_seen = threading.Event()
    end_streams = threading.Event()
    stop_calls = []

    class Pipe:
        def __init__(self, lines=(), *, receipt=False):
            self.lines = iter(lines)
            self.receipt = receipt

        def readline(self, _size=-1):
            try:
                line = next(self.lines)
                if self.receipt:
                    receipt_seen.set()
                return line
            except StopIteration:
                end_streams.wait(1)
                return ""

        def close(self):
            end_streams.set()

    class FakeStdin:
        def close(self):
            end_streams.set()

    class FakeProcess:
        def __init__(self, *_args, **_kwargs):
            self.stdin = FakeStdin()
            self.stdout = Pipe()
            self.stderr = Pipe(
                ["[event] subscription event_key=user_im_message_receive_group subscribe_id=sub-123\n"],
                receipt=True,
            )
            self.exited = False

        def poll(self):
            return 1 if self.exited else None

        def wait(self, timeout=None):
            self.exited = True
            end_streams.set()
            return 1

        def terminate(self):
            self.exited = True
            end_streams.set()

    def fake_run(argv, **kwargs):
        stop_calls.append((argv, kwargs))
        return subprocess.CompletedProcess(argv, 0, "", "")

    cancel = threading.Event()
    monkeypatch.setattr("listener_engine.subprocess.run", fake_run)
    consumer = DwsConsumer(
        consume_argv("conv-1", executable="dws-fake"), "conv-1", lambda _payload: None,
        lambda *_args: None, popen_factory=FakeProcess,
        cleanup_state_dir=tmp_path / "state", stop_runner=fake_run,
    )
    consumer.start()
    assert receipt_seen.wait(1)
    cancel.set()
    assert not consumer.wait_ready(1, cancel)
    consumer.close()
    assert [row[0] for row in stop_calls] == [["dws-fake", "event", "stop", "sub-123", "--yes"]]
    cleanup_file = tmp_path / "state" / "subscription-cleanup-pending.json"
    assert not cleanup_file.exists()


def test_consumer_reused_subscription_is_preserved_on_close(tmp_path, monkeypatch):
    ready_line = "[event] ready event_key=user_im_message_receive_group bus_pid=42 subscribe_id=reuse-123\n"
    stop_calls = []

    class FakeProcess:
        def __init__(self, *_args, **_kwargs):
            self.stdin = io.StringIO()
            self.stdout = io.StringIO("")
            self.stderr = io.StringIO(ready_line)
            self.exited = False

        def poll(self):
            return 1 if self.exited else None

        def wait(self, timeout=None):
            self.exited = True
            return 1

        def terminate(self):
            pass

    consumer = DwsConsumer(
        ["dws-fake", *consume_argv("conv-1")[1:], "--subscribe-id", "reuse-123"],
        "conv-1", lambda _payload: None, lambda *_args: None,
        popen_factory=FakeProcess, cleanup_state_dir=tmp_path / "state",
        stop_runner=lambda argv, **kwargs: (
            stop_calls.append(argv) or subprocess.CompletedProcess(argv, 0, "", "")
        ),
    )
    consumer.start()
    assert consumer.wait_ready(1)
    consumer.close()
    assert stop_calls == []
    assert getattr(consumer, "cleanup_result", {}).get("status") == "reused_preserved"


def test_consumer_cleanup_failure_records_exact_owned_receipt_and_unknown_when_missing(tmp_path, monkeypatch):
    stop_calls = []

    class FakeProcess:
        def __init__(self, argv, *_args, **_kwargs):
            self.stdin = io.StringIO()
            self.stdout = io.StringIO("")
            self.stderr = io.StringIO(
                "[event] subscription event_key=user_im_message_receive_group subscribe_id=sub-pending\n"
            )
            self.exited = False

        def poll(self):
            return 1 if self.exited else None

        def wait(self, timeout=None):
            self.exited = True
            return 1

        def terminate(self):
            pass

    consumer = DwsConsumer(
        consume_argv("conv-1", executable="dws-fake"), "conv-1", lambda _payload: None,
        lambda *_args: None, popen_factory=FakeProcess,
        cleanup_state_dir=tmp_path / "state",
        stop_runner=lambda argv, **kwargs: (
            stop_calls.append(argv) or subprocess.CompletedProcess(argv, 1, "", "")
        ),
    )
    consumer.start()
    consumer.close()
    assert stop_calls == [["dws-fake", "event", "stop", "sub-pending", "--yes"]]
    cleanup_file = tmp_path / "state" / "subscription-cleanup-pending.json"
    assert consumer.cleanup_result["status"] == "cleanup_pending"
    assert json.loads(cleanup_file.read_text())["pending"][0]["subscribe_id"] == "sub-pending"
    assert os.stat(cleanup_file).st_mode & 0o777 == 0o600
    assert stop_calls == [["dws-fake", "event", "stop", "sub-pending", "--yes"]]

    missing_dir = tmp_path / "missing-state"

    class NoReceiptProcess(FakeProcess):
        def __init__(self, *_args, **_kwargs):
            self.stdin = io.StringIO()
            self.stdout = io.StringIO("")
            self.stderr = io.StringIO("")
            self.exited = False

    missing = DwsConsumer(
        consume_argv("conv-2", executable="dws-fake"), "conv-2", lambda _payload: None,
        lambda *_args: None, popen_factory=NoReceiptProcess, cleanup_state_dir=missing_dir,
        stop_runner=lambda argv, **kwargs: (
            stop_calls.append(argv) or subprocess.CompletedProcess(argv, 1, "", "")
        ),
    )
    missing.start()
    missing.close()
    missing_record = json.loads((missing_dir / "subscription-cleanup-pending.json").read_text())["pending"][0]
    assert missing.cleanup_result["status"] == "cleanup_pending"
    assert missing_record["subscribe_id"] is None
    assert missing_record["ownership"] == "unknown"
    assert stop_calls == [["dws-fake", "event", "stop", "sub-pending", "--yes"]]


def test_transport_cancel_waits_for_inflight_create_then_prevents_any_new_attempt(tmp_path):
    from listener_engine import DwsEventTransport

    entered_start = threading.Event()
    release_start = threading.Event()
    cancel_returned = threading.Event()
    close_called = threading.Event()
    calls = []

    class FakeConsumer:
        def __init__(self, argv, *_args):
            self.argv = argv

        def start(self):
            calls.append(self.argv)
            entered_start.set()
            release_start.wait(1)

        def wait_ready(self, _timeout, cancel):
            cancel.wait(1)
            return False

        def wait(self, _timeout=0):
            return None

        def failure_metadata(self):
            return {"retryable": True}

        def close(self):
            close_called.set()
            return {"status": "graceful"}

    transport = DwsEventTransport(
        tmp_path / "state", consumer_factory=FakeConsumer, ready_timeout=1,
    )
    result = []
    creator = threading.Thread(
        target=lambda: result.append(transport.start_group("conv-1", lambda _event: None, lambda *_args: None))
    )
    creator.start()
    assert entered_start.wait(1)
    stopper = threading.Thread(target=lambda: (transport.cancel(), cancel_returned.set()))
    stopper.start()
    assert not cancel_returned.wait(0.05)
    release_start.set()
    assert cancel_returned.wait(1)
    creator.join(1)
    stopper.join(1)
    assert result == [None]
    assert close_called.is_set()
    assert calls == [consume_argv("conv-1")]
    assert transport.start_group("conv-1", lambda _event: None, lambda *_args: None) is None
    assert calls == [consume_argv("conv-1")]


def test_transport_refuses_ready_without_a_documented_receipt_and_persists_unknown(tmp_path):
    from listener_engine import DwsEventTransport

    exit_requested = threading.Event()
    stop_calls = []

    class FakeStdin:
        def close(self):
            exit_requested.set()

    class FakeProcess:
        def __init__(self, *_args, **_kwargs):
            self.stdin = FakeStdin()
            self.stdout = io.StringIO("")
            self.stderr = io.StringIO("[event] ready event_count=1 bus_pid=42\n")

        def poll(self):
            return 0 if exit_requested.is_set() else None

        def wait(self, timeout=None):
            exit_requested.set()
            return 0

        def terminate(self):
            exit_requested.set()

    def factory(argv, group_id, on_event, on_ready):
        return DwsConsumer(
            argv, group_id, on_event, on_ready, popen_factory=FakeProcess,
            cleanup_state_dir=tmp_path / "state",
            stop_runner=lambda command, **kwargs: (
                stop_calls.append(command) or subprocess.CompletedProcess(command, 0, "", "")
            ),
        )

    transport = DwsEventTransport(
        tmp_path / "state", consumer_factory=factory, ready_timeout=1,
    )
    assert transport.start_group("conv-1", lambda _event: None, lambda *_args: None) is None
    pending = transport.cleanup_ledger.snapshot()
    assert pending == [{
        "group_id": "conv-1", "subscribe_id": None,
        "ownership": "unknown", "status": "cleanup_pending",
    }]
    assert os.stat(tmp_path / "state" / "subscription-cleanup-pending.json").st_mode & 0o777 == 0o600
    assert stop_calls == []


def test_transport_smoke_reports_attempted_ready_failed_and_canceled_groups_separately():
    from listener_engine import run_transport_smoke

    cancel = threading.Event()

    class Handle:
        def wait(self, _timeout=0):
            return None

        def close(self):
            pass

    class FakeTransport:
        cancel_event = cancel

        def start_group(self, group_id, _on_event, _on_ready):
            if group_id == "failed":
                return None
            if group_id == "canceled":
                cancel.wait(1)
                return None
            return Handle()

    result = run_transport_smoke(
        groups=("ready", "failed", "canceled"), transport=FakeTransport(), duration_seconds=1,
    )
    assert result["attempted_groups"] == 3
    assert result["ready_consumers"] == 1
    assert result["failed_groups"] == 1
    assert result["canceled_groups"] == 1
    assert result["success"] is False


def test_transport_smoke_refuses_success_when_cleanup_metadata_is_pending():
    from listener_engine import run_transport_smoke

    cancel = threading.Event()

    class Handle:
        def wait(self, _timeout=0):
            return None

        def close(self):
            return {"status": "cleanup_pending"}

    class FakeTransport:
        cancel_event = cancel

        def start_group(self, group_id, _on_event, on_ready):
            on_ready(group_id, False)
            return Handle()

    result = run_transport_smoke(
        groups=("conv-1",), transport=FakeTransport(), duration_seconds=0.01,
    )
    assert result["ready_consumers"] == 1
    assert result["cleanup_pending"] == 1
    assert result["success"] is False


def test_transport_smoke_downgrades_a_consumer_that_exits_after_ready():
    from listener_engine import run_transport_smoke

    cancel = threading.Event()

    class Handle:
        def wait(self, _timeout=0):
            return 1 if cancel.is_set() else None

        def close(self):
            pass

    class FakeTransport:
        cancel_event = cancel

        def start_group(self, group_id, _on_event, on_ready):
            on_ready(group_id, False)
            return Handle()

    result = run_transport_smoke(
        groups=("conv-1",), transport=FakeTransport(), duration_seconds=0.01,
    )
    assert result["ready_consumers"] == 0
    assert result["failed_groups"] == 1
    assert result["success"] is False


def test_pending_state_is_atomic_private_minimal_and_survives_restart(tmp_path):
    state = AtomicPendingState(tmp_path / "state")
    state.notify("conv-1")
    state.notify("conv-1")
    state.notify("conv-2")
    restored = AtomicPendingState(tmp_path / "state")
    assert restored.snapshot() == state.snapshot()
    state_file = tmp_path / "state" / "pending.json"
    assert os.stat(state_file).st_mode & 0o777 == 0o600
    assert os.stat(tmp_path / "state").st_mode & 0o777 == 0o700
    raw = json.loads(state_file.read_text())
    assert set(raw) == {"version", "generation", "pending"}
    assert "private" not in state_file.read_text()


def test_history_event_sidecar_survives_legacy_pending_rewrites_and_is_fail_closed(tmp_path):
    state = AtomicPendingState(tmp_path / "state")
    state.notify_history_event("conv-1", "synthetic-message-id", "2026-11-07T08:00:00Z")

    pending_payload = json.loads(state.path.read_text())
    assert set(pending_payload) == {"version", "generation", "pending"}
    assert set(json.loads(state.history_path.read_text())) == {"version", "pending", "errors"}
    assert os.stat(state.history_path).st_mode & 0o777 == 0o600
    replay_snapshot = state.history_event_snapshot()
    assert replay_snapshot["conv-1"]

    # A legacy release only rewrites pending.json; the replay sidecar remains intact.
    state.force_pending(("conv-1",))
    state.acknowledge(state.snapshot())
    restored = AtomicPendingState(tmp_path / "state")
    assert restored.history_event_snapshot() == replay_snapshot

    restored.history_path.write_text('{"version":1,"pending":{},"errors":{},"unexpected":"data"}')
    with pytest.raises(StateError, match="history_event_state_invalid"):
        restored.history_event_snapshot()


def test_history_event_ack_removes_only_the_validated_snapshot(tmp_path):
    state = AtomicPendingState(tmp_path / "state")
    state.notify_history_event("conv-1", "older-event", "2026-11-07T08:00:00Z")
    initial_snapshot = state.history_event_snapshot()
    initial_hash = next(iter(initial_snapshot["conv-1"]))
    assert initial_snapshot["conv-1"][initial_hash]["event_time"] == "2026-11-07T16:00:00+08:00"

    state.notify_history_event("conv-1", "newer-event", "2026-11-07T08:01:00Z")
    state.acknowledge_history_events(initial_snapshot, {})

    remaining = state.history_event_snapshot()["conv-1"]
    assert set(remaining) != {initial_hash}
    assert len(remaining) == 1
    assert next(iter(remaining.values()))["event_time"] == "2026-11-07T16:01:00+08:00"


def test_duplicate_notifications_coalesce_and_new_generation_during_worker_gets_second_pass(tmp_path):
    state = AtomicPendingState(tmp_path / "state")
    for _ in range(5):
        state.notify("conv-1")
    calls = []

    def run_worker():
        calls.append("run")
        if len(calls) == 1:
            WakeupRouter("conv-1", ("conv-1",), state).handle(
                {"data": {
                    "conversationId": "conv-1", "messageId": "msg-during-worker",
                    "createTime": "2026-11-07T09:00:00+08:00",
                }}
            )
        return {"status": "COMPLETED", "history_replay_complete": True}

    result = CatchupCoordinator(state, run_worker, tmp_path / "work.lock").run_burst()
    assert calls == ["run", "run"]
    assert len(result) == 2
    assert state.snapshot() == {}


def test_repeated_wakeups_before_sweep_coalesce_to_one_history_pass(tmp_path):
    state = AtomicPendingState(tmp_path / "state")
    for _ in range(20):
        state.notify("conv-1")
    calls = []
    coordinator = CatchupCoordinator(
        state, lambda: (calls.append(1) or {"status": "COMPLETED"}), tmp_path / "work.lock"
    )
    coordinator.run_burst()
    assert calls == [1]
    assert state.snapshot() == {}


def test_unconfigured_and_mismatched_group_notifications_never_schedule(tmp_path):
    state = AtomicPendingState(tmp_path / "state")
    router = WakeupRouter("configured", ("configured",), state)
    assert not router.handle({"conversation_id": "outside"})
    assert not router.handle({"message_id": "no-conversation"})
    assert state.snapshot() == {}
    assert router.unconfigured_count == 1
    assert router.invalid_count == 1


def test_unknown_or_incomplete_worker_outcome_retains_pending(tmp_path):
    state = AtomicPendingState(tmp_path / "state")
    state.notify("conv-1")
    before = state.snapshot()
    result = CatchupCoordinator(
        state, lambda: {"status": "REVIEW", "reason": "history_incomplete"}, tmp_path / "work.lock"
    ).run_once()
    assert result["status"] == "REVIEW"
    assert state.snapshot() == before


def test_recovery_generation_survives_ack_from_worker_already_in_progress(tmp_path):
    state = AtomicPendingState(tmp_path / "state")
    state.notify("conv-1")
    active_worker_snapshot = state.snapshot()
    state.force_pending(("conv-1",))
    state.acknowledge(active_worker_snapshot)
    assert state.snapshot()["conv-1"] > active_worker_snapshot["conv-1"]


def test_worker_failure_applies_backoff_without_consuming_pending(tmp_path):
    state = AtomicPendingState(tmp_path / "state")
    state.notify("conv-1")
    clock = [100.0]
    calls = []
    coordinator = CatchupCoordinator(
        state,
        lambda: (calls.append(1) or {"status": "REVIEW"}),
        tmp_path / "work.lock",
        now=lambda: clock[0],
    )
    assert coordinator.run_once()["status"] == "REVIEW"
    assert coordinator.run_once()["status"] == "DEFERRED"
    assert calls == [1]
    assert state.snapshot()
    clock[0] = 160.0
    assert coordinator.run_once()["status"] == "REVIEW"
    assert calls == [1, 1]


def test_event_during_failed_worker_gets_one_bounded_followup_then_backoff(tmp_path):
    state = AtomicPendingState(tmp_path / "state")
    state.notify("conv-1")
    calls = []

    def failed_worker():
        calls.append(1)
        if len(calls) == 1:
            state.notify("conv-1")
        return {"status": "REVIEW"}

    coordinator = CatchupCoordinator(state, failed_worker, tmp_path / "work.lock")
    results = coordinator.run_burst()
    assert len(results) == 2
    assert calls == [1, 1]
    assert state.snapshot()
    assert coordinator.run_once()["status"] == "DEFERRED"


def test_shared_advisory_lock_prevents_concurrent_sweeps_double_running(tmp_path):
    state = AtomicPendingState(tmp_path / "state")
    state.notify("conv-1")
    counter = 0
    counter_lock = threading.Lock()

    def worker():
        nonlocal counter
        with counter_lock:
            counter += 1
        time.sleep(0.04)
        return {"status": "COMPLETED"}

    first = CatchupCoordinator(state, worker, tmp_path / "work.lock")
    second = CatchupCoordinator(state, worker, tmp_path / "work.lock")
    start = threading.Barrier(3)

    def sweep(coordinator):
        start.wait()
        coordinator.run_once()

    a = threading.Thread(target=sweep, args=(first,))
    b = threading.Thread(target=sweep, args=(second,))
    a.start()
    b.start()
    start.wait()
    a.join()
    b.join()
    assert counter == 1
    assert state.snapshot() == {}


def test_opening_wake_uses_one_clock_boundary_without_weekday_assumptions():
    from datetime import datetime, timezone, timedelta

    china = timezone(timedelta(hours=8))
    assert next_opening_wake(datetime.fromisoformat("2026-10-10T08:59:00+08:00")) == datetime.fromisoformat(
        "2026-10-10T09:00:00+08:00"
    )
    assert next_opening_wake(datetime.fromisoformat("2026-10-10T09:00:00+08:00")) == datetime.fromisoformat(
        "2026-10-11T09:00:00+08:00"
    )


def test_startup_that_crosses_0900_runs_one_pending_opening_catchup(tmp_path):
    from datetime import datetime
    from listener_engine import ListenerSupervisor

    clock = [datetime.fromisoformat("2026-10-10T08:59:00+08:00")]
    opened = threading.Event()
    now_calls = [0]

    def current_time():
        now_calls[0] += 1
        if now_calls[0] >= 2:
            opened.wait(1)
        return clock[0]

    calls = []
    state = AtomicPendingState(tmp_path / "state")
    coordinator = CatchupCoordinator(
        state,
        lambda: (calls.append(1) or {"status": "SKIPPED" if len(calls) == 1 else "COMPLETED"}),
        tmp_path / "work.lock",
    )

    class Handle:
        def close(self):
            pass

        def wait(self, _timeout=0):
            return None

    class OpeningTransport:
        def start_group(self, _group, _on_event, _on_ready):
            clock[0] = datetime.fromisoformat("2026-10-10T09:00:00+08:00")
            opened.set()
            supervisor.request_stop()
            return Handle()

    supervisor = ListenerSupervisor(
        scope_loader=lambda: ("conv-1",), state=state, coordinator=coordinator,
        transport=OpeningTransport(), now=current_time,
    )
    assert supervisor.run_forever() == 0
    assert calls == [1, 1]
    assert state.snapshot() == {}


@pytest.mark.parametrize("retryable, expected", [(False, 0), (True, 2), (None, 1), ("unknown", 1)])
def test_subscription_attempt_budget_matches_official_retryable_contract(retryable, expected):
    assert subscription_retry_limit(retryable) == expected


def test_subscription_retry_budget_and_cooldown_survive_supervisor_respawn(tmp_path):
    store = SubscriptionRetryStore(tmp_path / "state")
    assert store.begin_attempt("conv-1", now=100)
    first = store.failed(
        "conv-1", {"retryable": True, "retry_after_seconds": 5}, now=100
    )
    assert first["retry"] is True
    assert first["delay_seconds"] == 5
    assert not SubscriptionRetryStore(tmp_path / "state").begin_attempt("conv-1", now=104)
    restarted = SubscriptionRetryStore(tmp_path / "state")
    assert restarted.begin_attempt("conv-1", now=105)
    second = restarted.failed("conv-1", {"retryable": True}, now=105)
    assert second["retry"] is True
    assert restarted.begin_attempt("conv-1", now=107)
    third = restarted.failed("conv-1", {"retryable": True}, now=107)
    assert third["retry"] is False
    assert not SubscriptionRetryStore(tmp_path / "state").begin_attempt("conv-1", now=1000)
    assert os.stat(tmp_path / "state" / "subscription-attempts.json").st_mode & 0o777 == 0o600


def test_injected_dws_transport_retries_with_documented_argv_and_honors_delay(tmp_path):
    from listener_engine import DwsEventTransport

    clock = [100.0]
    calls = []

    class FakeConsumer:
        def __init__(self, argv, group_id, on_event, on_ready):
            self.argv = argv
            self.group_id = group_id
            self.ready = len(calls) > 0
            self.failure = {"retryable": True, "retry_after_seconds": 3}

        def start(self):
            calls.append(self.argv)

        def wait_ready(self, _timeout, cancel):
            return self.ready and not cancel.is_set()

        def failure_metadata(self):
            return self.failure

        def close(self):
            pass

    def wait(delay):
        clock[0] += delay
        return False

    transport = DwsEventTransport(
        tmp_path / "state", executable="dws-fake", consumer_factory=FakeConsumer,
        wait=wait, now=lambda: clock[0],
    )
    handle = transport.start_group("conv-1", lambda _payload: None, lambda *_args: None)
    assert handle is not None
    assert len(calls) == 2
    assert calls == [consume_argv("conv-1", "dws-fake")] * 2
    assert clock[0] == 103
    assert "conv-1" not in (tmp_path / "state" / "subscription-attempts.json").read_text()


def test_injected_dws_transport_stops_without_extra_attempt_on_retryable_false(tmp_path):
    from listener_engine import DwsEventTransport

    calls = []

    class FakeConsumer:
        def __init__(self, argv, *_args):
            self.argv = argv

        def start(self):
            calls.append(self.argv)

        def wait_ready(self, _timeout, _cancel):
            return False

        def failure_metadata(self):
            return {"retryable": False}

        def close(self):
            pass

    transport = DwsEventTransport(
        tmp_path / "state", consumer_factory=FakeConsumer,
        wait=lambda _delay: pytest.fail("retryable=false must not wait or retry"),
    )
    assert transport.start_group("conv-1", lambda _payload: None, lambda *_args: None) is None
    assert calls == [consume_argv("conv-1")]
    assert not SubscriptionRetryStore(tmp_path / "state").begin_attempt("conv-1")


def test_daily_recovery_forces_configured_sweep_and_preserves_skipped_pending(tmp_path):
    state = AtomicPendingState(tmp_path / "state")
    coordinator = CatchupCoordinator(
        state, lambda: {"status": "SKIPPED", "reason": "calendar_gate_closed"}, tmp_path / "work.lock"
    )
    result = run_daily_recovery(
        state=state, scope_loader=lambda: ("conv-1", "conv-2"), coordinator=coordinator
    )
    assert result["status"] == "SKIPPED"
    assert set(state.snapshot()) == {"conv-1", "conv-2"}


def test_daily_recovery_is_once_per_local_day_unless_explicitly_forced(tmp_path):
    state = AtomicPendingState(tmp_path / "state")
    calls = []
    coordinator = CatchupCoordinator(
        state, lambda: (calls.append(1) or {"status": "COMPLETED"}), tmp_path / "work.lock"
    )
    scope = lambda: ("conv-1", "conv-2")
    first = run_daily_recovery(state=state, scope_loader=scope, coordinator=coordinator)
    second = run_daily_recovery(state=state, scope_loader=scope, coordinator=coordinator)
    assert first["status"] == "COMPLETED"
    assert second["status"] == "SKIPPED"
    assert second["reason"] == "daily_recovery_already_run"
    forced = run_daily_recovery(
        state=state, scope_loader=scope, coordinator=coordinator, force=True
    )
    assert forced["status"] == "COMPLETED"
    assert calls == [1, 1]


def test_daily_recovery_waits_for_live_worker_without_losing_newer_wakeups(tmp_path):
    from datetime import date

    state = AtomicPendingState(tmp_path / "state")
    state.notify("conv-1")
    initial_generation = state.snapshot()["conv-1"]
    worker_entered = threading.Event()
    release_worker = threading.Event()
    daily_started = threading.Event()
    active = 0
    maximum_active = 0
    guard = threading.Lock()
    daily_observed = []

    def live_worker():
        nonlocal active, maximum_active
        with guard:
            active += 1
            maximum_active = max(maximum_active, active)
        worker_entered.set()
        release_worker.wait(2)
        with guard:
            active -= 1
        return {"status": "COMPLETED"}

    live = CatchupCoordinator(state, live_worker, tmp_path / "work.lock")

    def daily_worker():
        nonlocal active, maximum_active
        with guard:
            active += 1
            maximum_active = max(maximum_active, active)
        daily_observed.append(state.snapshot()["conv-1"])
        daily_started.set()
        with guard:
            active -= 1
        return {"status": "COMPLETED"}

    daily = CatchupCoordinator(state, daily_worker, tmp_path / "work.lock")
    live_thread = threading.Thread(target=live.run_once)
    live_thread.start()
    assert worker_entered.wait(1)
    result = []
    daily_thread = threading.Thread(target=lambda: result.append(run_daily_recovery(
        state=state, scope_loader=lambda: ("conv-1",), coordinator=daily,
        recovery_date=date(2026, 10, 9),
    )))
    daily_thread.start()

    deadline = time.monotonic() + 1
    while state.snapshot()["conv-1"] <= initial_generation and time.monotonic() < deadline:
        time.sleep(0.01)
    forced_generation = state.snapshot()["conv-1"]
    assert forced_generation > initial_generation
    state.notify("conv-1")
    live_generation = state.snapshot()["conv-1"]
    release_worker.set()
    live_thread.join(2)
    daily_thread.join(2)

    assert result[0]["status"] == "COMPLETED"
    assert daily_started.is_set()
    assert daily_observed == [live_generation]
    assert maximum_active == 1
    assert state.snapshot() == {}


def test_worker_argv_preserves_gates_floor_model_flags_and_recovery_mode():
    args = SimpleNamespace(
        mode="production", write_ledger=True, allow_external_writes=True,
        llm_jev=False, jev_v2_policy=True, semantic_fallback=False,
        title_fields=False, date_role_fallback=False, supervised_rollout=False,
        not_before="2026-10-08T00:00:00+08:00", resource_dir="/tmp/resources",
        recover_once=True,
    )
    argv = _worker_argv(args)
    assert argv[2:5] == ["--once", "--summary-only", "--mode"]
    assert "--write-ledger" in argv
    assert "--allow-external-writes" in argv
    assert "--jev-v2-policy" in argv
    assert "--not-before" in argv
    assert "2026-10-08T00:00:00+08:00" in argv
    assert "--daily-recovery" in argv
    assert "--semantic-fallback" not in argv
    assert "--pending-event-state-dir" not in argv
    replay_argv = _worker_argv(args, pending_event_state_dir="/tmp/shared-listener-state")
    assert replay_argv[replay_argv.index("--pending-event-state-dir") + 1] == "/tmp/shared-listener-state"


def test_new_cli_help_runs_isolated_without_runtime_or_network_setup():
    completed = subprocess.run(
        [sys.executable, "-I", str(ROOT / "listener_engine.py"), "--help"],
        capture_output=True, text=True, timeout=10,
    )
    assert completed.returncode == 0
    assert "--transport-smoke" in completed.stdout
    assert "--recover-once" in completed.stdout
    assert completed.stderr == ""


def test_transport_smoke_starts_all_configured_group_attempts_concurrently(tmp_path, monkeypatch, capsys):
    import listener_engine

    started_two = threading.Event()
    guard = threading.Lock()
    active = 0
    maximum_active = 0

    class FakeAdapter:
        def query(self, request):
            assert request == {"view": "runtime_config"}
            return {"config": {"config_group": [
                {"conversation_id": "conv-1", "purpose": "todo_scan"},
                {"conversation_id": "conv-2", "purpose": "todo_scan"},
            ]}}

    class Handle:
        def close(self):
            pass

    class FakeTransport:
        def __init__(self, _state_dir, *, executable, cancel_event):
            self.cancel_event = cancel_event

        def start_group(self, group_id, _on_event, on_ready):
            nonlocal active, maximum_active
            with guard:
                active += 1
                maximum_active = max(maximum_active, active)
                if active == 2:
                    started_two.set()
            started_two.wait(0.3)
            on_ready(group_id, False)
            with guard:
                active -= 1
            return Handle()

    workflows = types.ModuleType("workflows")
    workflows.__path__ = []
    adapter_package = types.ModuleType("workflows.polardb_work_adapter")
    adapter_package.__path__ = []
    query_module = types.ModuleType("workflows.polardb_work_adapter.query")
    query_module.WorkQueryAdapter = FakeAdapter
    monkeypatch.setitem(sys.modules, "workflows", workflows)
    monkeypatch.setitem(sys.modules, "workflows.polardb_work_adapter", adapter_package)
    monkeypatch.setitem(sys.modules, "workflows.polardb_work_adapter.query", query_module)
    monkeypatch.setattr(listener_engine, "DwsEventTransport", FakeTransport)

    result = listener_engine.main([
        "--state-dir", str(tmp_path / "state"), "--transport-smoke", "--duration-seconds", "1",
    ])
    record = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert result == 0
    assert maximum_active == 2
    assert record["attempted_groups"] == 2
    assert record["ready_markers"] == 2
    assert record["ready_consumers"] == 2
    assert record["failed_groups"] == 0
    assert record["canceled_groups"] == 0
    assert record["delivery_coverage"] == "unverified_by_ready_marker"


def test_listener_rejects_model_resolution_without_durable_source_ledger():
    from listener_engine import main

    with pytest.raises(SystemExit) as raised:
        main(["--state-dir", "/tmp/unused-listener-state", "--llm-jev"])
    assert raised.value.code == 2


def test_supervisor_startup_and_event_envelopes_use_real_router_with_fake_transport(tmp_path, capsys):
    class Handle:
        def close(self):
            pass

        def wait(self, _timeout=0):
            return None

    class FakeTransport:
        def __init__(self):
            self.calls = []
            self.callbacks = {}

        def start_group(self, group_id, on_event, on_ready):
            self.calls.append(consume_argv(group_id))
            self.callbacks[group_id] = on_event
            on_ready(group_id, False)
            return Handle()

    state = AtomicPendingState(tmp_path / "state")
    worker_calls = []
    coordinator = CatchupCoordinator(
        state, lambda: (worker_calls.append(1) or {
            "status": "COMPLETED", "history_replay_complete": True,
        }), tmp_path / "work.lock"
    )
    transport = FakeTransport()
    from listener_engine import ListenerSupervisor

    supervisor = ListenerSupervisor(
        scope_loader=lambda: ("conv-1",), state=state, coordinator=coordinator,
        transport=transport, coalesce_seconds=0,
    )
    supervisor.start()
    assert supervisor.wait_for_startups(1)
    assert len(worker_calls) == 1  # startup catchup
    assert transport.calls == [consume_argv("conv-1")]
    assert transport.callbacks["conv-1"]({"payload": {
        "open_conversation_id": "conv-1", "message_id": "msg-router",
        "create_time": "2026-11-07T09:00:00+08:00",
    }})
    assert state.snapshot()
    supervisor._process_notice_batch(supervisor.notices.get_nowait())
    assert len(worker_calls) == 2
    assert state.snapshot() == {}
    assert "private" not in capsys.readouterr().out


def test_supervisor_fails_closed_before_subscription_when_scope_loader_errors(tmp_path):
    class NeverTransport:
        def __init__(self):
            self.called = False

        def start_group(self, *_args):
            self.called = True
            raise AssertionError("must not subscribe without authoritative scope")

    transport = NeverTransport()
    state = AtomicPendingState(tmp_path / "state")
    coordinator = CatchupCoordinator(state, lambda: {"status": "COMPLETED"}, tmp_path / "work.lock")
    from listener_engine import ListenerSupervisor

    supervisor = ListenerSupervisor(
        scope_loader=lambda: (_ for _ in ()).throw(ScopeError("runtime_config_unavailable")),
        state=state, coordinator=coordinator, transport=transport,
    )
    with pytest.raises(ScopeError):
        supervisor.start()
    assert not transport.called


def test_supervisor_starts_group_consumers_concurrently_without_one_retry_blocking_scope(tmp_path):
    class Handle:
        def close(self):
            pass

        def wait(self, _timeout=0):
            return None

    slow_entered = threading.Event()
    release_slow = threading.Event()
    fast_entered = threading.Event()

    class StaggeredTransport:
        def start_group(self, group_id, _on_event, _on_ready):
            if group_id == "slow":
                slow_entered.set()
                release_slow.wait(2)
            else:
                fast_entered.set()
            return Handle()

    state = AtomicPendingState(tmp_path / "state")
    coordinator = CatchupCoordinator(
        state, lambda: {"status": "COMPLETED"}, tmp_path / "work.lock"
    )
    from listener_engine import ListenerSupervisor

    supervisor = ListenerSupervisor(
        scope_loader=lambda: ("slow", "fast"), state=state, coordinator=coordinator,
        transport=StaggeredTransport(),
    )
    supervisor.start()
    assert slow_entered.wait(1)
    assert fast_entered.wait(1)
    release_slow.set()
    assert supervisor.wait_for_startups(1)
    assert set(supervisor.consumers) == {"slow", "fast"}
    supervisor.close()
