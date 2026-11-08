"""Resident polling entry point for incremental DWS history scans."""

from __future__ import annotations

import argparse
import math
from pathlib import Path
import signal
import sys
import threading
import time
from typing import Any, Callable

SCRIPT_DIRECTORY = Path(__file__).resolve().parent
if str(SCRIPT_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIRECTORY))

import event_worker
from event_worker import _parse_not_before_argument, summary_result, validate_history_options
from listener_engine import (
    AtomicPendingState,
    ScopeError,
    StateError,
    WorkLock,
    WorkLockCancelled,
    _validate_group_ids,
    load_configured_group_ids,
    safe_log,
)


DEFAULT_INTERVAL_SECONDS = 900
DEFAULT_NOT_BEFORE = "2026-10-08T00:00:00+08:00"
APPROVED_ROLLOUT_FLOOR = _parse_not_before_argument(DEFAULT_NOT_BEFORE)
DEFAULT_RETRY_LIMIT = 2
DEFAULT_RETRY_BASE_SECONDS = 30.0
MAX_RETRY_DELAY_SECONDS = 300.0


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Resident incremental DWS history poller")
    parser.add_argument("--state-dir", required=True, help="shared private durable listener state directory")
    parser.add_argument("--interval-seconds", type=int, default=DEFAULT_INTERVAL_SECONDS)
    parser.add_argument("--max-cycles", type=int, help="bounded cycle count for tests and acceptance")
    parser.add_argument("--mode", choices=("shadow", "test", "production"), default="shadow")
    parser.add_argument("--write-ledger", action="store_true")
    parser.add_argument("--allow-external-writes", action="store_true")
    parser.add_argument("--llm-jev", action="store_true")
    parser.add_argument("--jev-v2-policy", action="store_true")
    parser.add_argument("--semantic-fallback", action="store_true")
    parser.add_argument("--title-fields", action="store_true")
    parser.add_argument("--date-role-fallback", action="store_true")
    parser.add_argument("--supervised-rollout", action="store_true")
    parser.add_argument(
        "--not-before", type=_parse_not_before_argument,
        default=APPROVED_ROLLOUT_FLOOR, metavar="RFC3339+08:00",
    )
    parser.add_argument("--resource-dir", default="/root/.hermes/cache/scratch/dingtalk_auto_todo_events")
    return parser


def run_resident(
    poller: Any,
    *,
    state: Any,
    scope_loader: Callable[[], Any],
    lock_path: str | Path,
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
    max_cycles: int | None = None,
    stop_event: threading.Event | None = None,
    monotonic: Callable[[], float] | None = None,
    wait: Callable[[float], bool] | None = None,
    retry_limit: int = 2,
    retry_base_seconds: float = 30,
    output: Callable[[dict[str, Any]], None] | None = None,
) -> int:
    """Run serialized history cycles on a monotonic cadence until stopped."""
    interval_seconds = float(interval_seconds)
    if not math.isfinite(interval_seconds) or interval_seconds <= 0:
        raise ValueError("interval_seconds_must_be_positive")
    if max_cycles is not None and max_cycles < 1:
        raise ValueError("max_cycles_must_be_positive")
    if retry_limit < 0 or retry_base_seconds < 0:
        raise ValueError("retry_options_must_be_nonnegative")

    stop = stop_event or threading.Event()
    monotonic_now = monotonic or time.monotonic
    wait_for_stop = wait or stop.wait
    emit = output or safe_log
    attempts = 0
    last_status: str | None = None
    next_start = monotonic_now()

    while not stop.is_set() and (max_cycles is None or attempts < max_cycles):
        remaining = next_start - monotonic_now()
        if remaining > 0 and wait_for_stop(remaining):
            break
        if stop.is_set():
            break

        retry_index = 0
        while not stop.is_set() and (max_cycles is None or attempts < max_cycles):
            attempts += 1
            outcome = _run_cycle(
                poller,
                state=state,
                scope_loader=scope_loader,
                lock_path=lock_path,
                stop_event=stop,
            )
            if outcome.get("status") != "STOPPED":
                emit(outcome)
            status = outcome.get("status")
            last_status = str(status) if status is not None else "UNKNOWN"
            if status not in {"REVIEW", "UNKNOWN", "BLOCKED"}:
                break
            if retry_index >= retry_limit or stop.is_set():
                break
            delay = min(
                retry_base_seconds * (2 ** retry_index),
                MAX_RETRY_DELAY_SECONDS,
            )
            retry_index += 1
            if delay > 0 and wait_for_stop(delay):
                break

        if stop.is_set() or (max_cycles is not None and attempts >= max_cycles):
            break

        # Keep an absolute cadence. If work/retries crossed one or more ticks,
        # skip them and wait for the next future tick instead of spinning.
        next_start += interval_seconds
        now = monotonic_now()
        if next_start <= now:
            missed = math.floor((now - next_start) / interval_seconds) + 1
            next_start += missed * interval_seconds
    if max_cycles is not None and last_status in {"REVIEW", "UNKNOWN", "BLOCKED"}:
        return 2
    return 0


def _run_cycle(
    poller: Any,
    *,
    state: AtomicPendingState,
    scope_loader: Callable[[], Any],
    lock_path: str | Path,
    stop_event: threading.Event,
) -> dict[str, Any]:
    """Share recovery's lock and pending generations around one poller pass."""
    if stop_event.is_set():
        return {"status": "STOPPED"}
    try:
        with WorkLock(lock_path, cancel_event=stop_event):
            if stop_event.is_set():
                return {"status": "STOPPED"}
            group_ids = _validate_group_ids(scope_loader())
            state.retain_scope(group_ids)
            state.ensure_pending(group_ids)
            snapshot = state.snapshot()
            history_snapshot = state.history_event_snapshot()
            history_error_snapshot = state.history_event_errors_snapshot()
            poller.history_replay_state = state
            try:
                result = poller.run_once()
            except Exception as exc:
                return {
                    "status": "REVIEW",
                    "reason": "poll_failed_" + type(exc).__name__,
                    "messages": 0,
                    "writes": 0,
                }
            if not isinstance(result, dict):
                result = {"status": "UNKNOWN", "reason": "poll_result_invalid"}
            outcome = summary_result(result)
            history_pending = any(history_snapshot.values()) or any(history_error_snapshot.values())
            if (
                outcome.get("status") == "COMPLETED"
                and history_pending
                and outcome.get("history_replay_complete") is not True
            ):
                outcome["status"] = "REVIEW"
                outcome["reason"] = "history_replay_unverified"
            if outcome.get("status") == "COMPLETED":
                state.acknowledge(snapshot)
                replay_ack_authorized = (
                    getattr(poller, "mode", None) == "production"
                    and bool(getattr(poller, "write_ledger", False))
                )
                if history_pending and replay_ack_authorized:
                    state.acknowledge_history_events(history_snapshot, history_error_snapshot)
            return outcome
    except WorkLockCancelled:
        return {"status": "STOPPED"}
    except (ScopeError, StateError):
        return {"status": "BLOCKED", "reason": "scope_or_state_error"}
    except Exception as exc:
        return {
            "status": "REVIEW",
            "reason": "resident_cycle_failed_" + type(exc).__name__,
            "messages": 0,
            "writes": 0,
        }


def _validate_resident_options(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    if args.interval_seconds <= 0:
        parser.error("--interval-seconds must be positive")
    if args.max_cycles is not None and args.max_cycles < 1:
        parser.error("--max-cycles must be positive")
    if args.not_before < APPROVED_ROLLOUT_FLOOR:
        parser.error("--not-before may not precede 2026-10-08T00:00:00+08:00")
    validate_history_options(args, parser)


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    _validate_resident_options(args, parser)
    poller = event_worker.build_history_poller(args)
    state = AtomicPendingState(args.state_dir)
    scope_loader = lambda: load_configured_group_ids(poller.adapter)
    stop = threading.Event()
    previous_handlers: dict[int, Any] = {}
    for sig in (signal.SIGTERM, signal.SIGINT):
        previous_handlers[sig] = signal.signal(sig, lambda _signum, _frame: stop.set())
    try:
        return run_resident(
            poller,
            state=state,
            scope_loader=scope_loader,
            lock_path=Path(args.state_dir) / "work.lock",
            interval_seconds=args.interval_seconds,
            max_cycles=args.max_cycles,
            stop_event=stop,
        )
    finally:
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    raise SystemExit(main())
