"""Wake the existing Cifra review processor from bot Stream or DWS events."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
from collections import OrderedDict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import queue
import signal
import subprocess
import threading
import time
from typing import Any
from zoneinfo import ZoneInfo

from dws_runtime import build_dws_argv, start_dws_stream
from review_card_adapter import (
    CARD_CALLBACK_TOPIC,
    CardActionCallback,
    claim_card_callback_work,
    card_identity_map_key,
    finish_card_callback_work,
    load_card_identity_map,
    parse_stream_card_callback,
    process_card_callback,
    recover_interrupted_card_work,
)
from review_inbox import connect
from review_inbox_poll import read_dws
from review_inbox_worker import run as run_review_worker


DWS_REVIEW_EVENT_KEY = "user_im_message_receive_o2o_all"
DWS_REVIEW_READY_TIMEOUT_SECONDS = 120.0
DWS_REVIEW_STOP_TIMEOUT_SECONDS = 15.0
DWS_REVIEW_MAX_LINE_CHARS = 65536
DWS_REVIEW_MAX_SEEN_MESSAGE_IDS = 4096
CHINA_TZ = ZoneInfo("Asia/Shanghai")
STREAM_HTTP_CONNECT_TIMEOUT_SECONDS = 5.0
STREAM_HTTP_READ_TIMEOUT_SECONDS = 15.0
STREAM_WEBSOCKET_OPEN_TIMEOUT_SECONDS = 10.0
STREAM_WEBSOCKET_CLOSE_TIMEOUT_SECONDS = 5.0
STREAM_CALLBACK_DRAIN_TIMEOUT_SECONDS = 30.0
STREAM_TASK_SHUTDOWN_TIMEOUT_SECONDS = 60.0
REVIEW_WORKER_SHUTDOWN_TIMEOUT_SECONDS = 90.0
_STOP_RECEIVE = object()


class _RequestsTimeoutProxy:
    """Add finite timeouts to the SDK's synchronous requests without editing it."""

    def __init__(self, requests_module: Any, timeout: tuple[float, float]) -> None:
        self._requests_module = requests_module
        self._timeout = timeout

    def post(self, *args: Any, **kwargs: Any) -> Any:
        kwargs["timeout"] = self._timeout
        return self._requests_module.post(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._requests_module, name)


async def _sleep_until_stopped(stop_event: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except TimeoutError:
        pass


async def _receive_or_stop(websocket: Any, stop_event: asyncio.Event) -> Any:
    receive_task = asyncio.create_task(websocket.recv())
    stop_task = asyncio.create_task(stop_event.wait())
    done, pending = await asyncio.wait(
        (receive_task, stop_task), return_when=asyncio.FIRST_COMPLETED,
    )
    if receive_task in done:
        stop_task.cancel()
        await asyncio.gather(stop_task, return_exceptions=True)
        return receive_task.result()
    receive_task.cancel()
    await asyncio.gather(receive_task, return_exceptions=True)
    return _STOP_RECEIVE


def _track_callback_task(task: asyncio.Task[Any], tasks: set[asyncio.Task[Any]]) -> None:
    tasks.add(task)

    def finish(completed: asyncio.Task[Any]) -> None:
        tasks.discard(completed)
        if not completed.cancelled():
            completed.exception()

    task.add_done_callback(finish)


async def _drain_callback_tasks(
    tasks: set[asyncio.Task[Any]], timeout_seconds: float,
) -> None:
    if not tasks:
        return
    _, pending = await asyncio.wait(tuple(tasks), timeout=timeout_seconds)
    if pending:
        for task in pending:
            task.cancel()
        await asyncio.wait(pending, timeout=1.0)


async def run_shutdown_aware_stream(
    client: Any,
    dingtalk_stream: Any,
    stop_event: asyncio.Event,
) -> None:
    """Run the SDK's public hooks in a stoppable loop and drain callbacks before close."""
    stream_module = dingtalk_stream.stream
    original_requests = stream_module.requests
    stream_module.requests = _RequestsTimeoutProxy(
        original_requests,
        (STREAM_HTTP_CONNECT_TIMEOUT_SECONDS, STREAM_HTTP_READ_TIMEOUT_SECONDS),
    )
    try:
        client.pre_start()
        while not stop_event.is_set():
            try:
                connection = client.open_connection()
            except Exception:
                if not stop_event.is_set():
                    await _sleep_until_stopped(stop_event, 3.0)
                continue
            if stop_event.is_set():
                break
            if not connection:
                await _sleep_until_stopped(stop_event, 10.0)
                continue

            uri = f"{connection['endpoint']}?ticket={stream_module.quote_plus(connection['ticket'])}"
            callbacks: set[asyncio.Task[Any]] = set()
            keepalive_task: asyncio.Task[Any] | None = None
            try:
                async with stream_module.websockets.connect(
                    uri,
                    open_timeout=STREAM_WEBSOCKET_OPEN_TIMEOUT_SECONDS,
                    close_timeout=STREAM_WEBSOCKET_CLOSE_TIMEOUT_SECONDS,
                ) as websocket:
                    client.websocket = websocket
                    keepalive_task = asyncio.create_task(client.keepalive(websocket))
                    try:
                        while not stop_event.is_set():
                            raw_message = await _receive_or_stop(websocket, stop_event)
                            if raw_message is _STOP_RECEIVE:
                                break
                            callback = asyncio.create_task(
                                client.background_task(json.loads(raw_message))
                            )
                            _track_callback_task(callback, callbacks)
                    finally:
                        await _drain_callback_tasks(
                            callbacks, STREAM_CALLBACK_DRAIN_TIMEOUT_SECONDS,
                        )
            except asyncio.CancelledError:
                stop_event.set()
                raise
            except Exception:
                if not stop_event.is_set():
                    await _sleep_until_stopped(stop_event, 3.0)
            finally:
                client.websocket = None
                if keepalive_task is not None and not keepalive_task.done():
                    keepalive_task.cancel()
                    await asyncio.gather(keepalive_task, return_exceptions=True)
    finally:
        stream_module.requests = original_requests


@dataclass(frozen=True)
class CardCallbackConfig:
    """Explicit deployment contract for the optional card callback topic."""

    template_id: str
    identity_map: dict[str, str]


def load_card_callback_config(args: argparse.Namespace) -> CardCallbackConfig | None:
    """Validate opt-in card settings before loading SDK credentials or opening Stream."""
    if not getattr(args, "card_callbacks", False):
        return None
    if getattr(args, "transport", "stream") != "stream":
        raise ValueError("card_callbacks_require_app_stream")
    template_id = getattr(args, "card_template_id", None)
    identity_map_path = getattr(args, "card_identity_map_file", None)
    if not isinstance(template_id, str) or not template_id.strip() or len(template_id) > 256:
        raise ValueError("card_template_id_required")
    if not isinstance(identity_map_path, str) or not identity_map_path.strip():
        raise ValueError("card_identity_map_required")
    identity_map = load_card_identity_map(Path(identity_map_path))
    return CardCallbackConfig(template_id, identity_map)


def _stream_callback_time(callback: Any) -> str | None:
    """Convert the SDK's server push timestamp (Unix milliseconds) to local time."""
    headers = getattr(callback, "headers", None)
    timestamp = getattr(headers, "time", None)
    if isinstance(timestamp, bool) or not isinstance(timestamp, int) or timestamp <= 0:
        return None
    seconds, milliseconds = divmod(timestamp, 1000)
    try:
        utc_time = datetime.fromtimestamp(seconds, tz=timezone.utc).replace(
            microsecond=milliseconds * 1000,
        )
        local_time = utc_time.astimezone(CHINA_TZ)
    except (OverflowError, OSError, ValueError):
        return None
    return local_time.strftime("%Y-%m-%d %H:%M:%S.") + f"{milliseconds:03d}"


def register_stream_handlers(client: Any, dingtalk_stream: Any, AckMessage: Any,
                             wakeups: "ReviewWakeups",
                             card_config: CardCallbackConfig | None = None) -> tuple[Any, ...]:
    """Register the unchanged chatbot handler and, only when opted in, the card topic."""
    class CifraReviewHandler(dingtalk_stream.ChatbotHandler):
        def __init__(self) -> None:
            super().__init__()

        async def process(self, callback: Any) -> tuple[int, str]:
            try:
                payload = callback.data
                if not isinstance(payload, dict):
                    return AckMessage.STATUS_OK, "OK"
                message = dingtalk_stream.chatbot.ChatbotMessage.from_dict(payload)
                wakeups.enqueue(
                    message.conversation_id,
                    message.message_id,
                    message.sender_id,
                )
            except Exception:
                emit("stream_callback_rejected")
            return AckMessage.STATUS_OK, "OK"

    chatbot_handler = CifraReviewHandler()
    client.register_callback_handler(
        dingtalk_stream.chatbot.ChatbotMessage.TOPIC,
        chatbot_handler,
    )
    handlers: list[Any] = [chatbot_handler]
    if card_config is None:
        return tuple(handlers)

    class CifraCardActionHandler(dingtalk_stream.ChatbotHandler):
        def __init__(self) -> None:
            super().__init__()

        async def process(self, callback: Any) -> tuple[int, str]:
            payload = getattr(callback, "data", None)
            if not isinstance(payload, dict):
                return AckMessage.STATUS_OK, "OK"
            normalized = parse_stream_card_callback(
                payload,
                message_time=_stream_callback_time(callback),
            )
            if normalized is None:
                emit("card_callback_rejected")
                return AckMessage.STATUS_OK, "OK"
            try:
                identity_key = card_identity_map_key(
                    normalized.user_id, normalized.user_id_type,
                )
            except ValueError:
                emit("card_callback_rejected")
                return AckMessage.STATUS_OK, "OK"
            if not card_config.identity_map.get(identity_key):
                emit("card_callback_rejected")
                return AckMessage.STATUS_OK, "OK"
            try:
                await asyncio.to_thread(wakeups.persist_card_callback, normalized)
            except Exception:
                emit("card_callback_persistence_failed")
                raise
            return AckMessage.STATUS_OK, "OK"

    card_handler = CifraCardActionHandler()
    client.register_callback_handler(CARD_CALLBACK_TOPIC, card_handler)
    handlers.append(card_handler)
    return tuple(handlers)


@dataclass(frozen=True)
class DwsReviewMessage:
    """Validated flattened DWS fields; content is unchanged after JSON decoding."""

    event_id: str
    message_id: str
    conversation_id: str
    sender_id: str
    create_time: str
    content: str


def parse_dws_review_event(payload: Any) -> DwsReviewMessage | None:
    """Parse one official flattened all-direct event without normalizing its content."""
    if not isinstance(payload, dict) or payload.get("type") != DWS_REVIEW_EVENT_KEY:
        return None

    fields = (
        ("event_id", 512),
        ("message_id", 256),
        ("conversation_id", 512),
        ("sender_open_dingtalk_id", 512),
        ("create_time", 64),
    )
    values: dict[str, str] = {}
    for field, max_length in fields:
        value = payload.get(field)
        if (not isinstance(value, str) or not value or len(value) > max_length
                or value != value.strip()):
            return None
        values[field] = value

    content = payload.get("content")
    if not isinstance(content, str) or not content or len(content) > 4000:
        return None
    try:
        datetime.strptime(values["create_time"], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None

    return DwsReviewMessage(
        event_id=values["event_id"],
        message_id=values["message_id"],
        conversation_id=values["conversation_id"],
        sender_id=values["sender_open_dingtalk_id"],
        create_time=values["create_time"],
        content=content,
    )


def parse_dws_review_event_line(line: str) -> DwsReviewMessage | None:
    """Parse one bounded NDJSON line from `dws event consume --flatten`."""
    if not isinstance(line, str) or not line or len(line) > DWS_REVIEW_MAX_LINE_CHARS:
        return None
    try:
        payload = json.loads(line)
    except (json.JSONDecodeError, TypeError):
        return None
    return parse_dws_review_event(payload)


def build_dws_review_event_argv(profile: str, conversation_id: str, *,
                                executable: str = "dws") -> list[str]:
    """Build a user-OAuth consumer scoped to exactly one Cifra DM conversation."""
    if not isinstance(profile, str) or not profile.strip():
        raise ValueError("dws_profile_required")
    if (not isinstance(conversation_id, str) or not conversation_id
            or len(conversation_id) > 512 or conversation_id != conversation_id.strip()):
        raise ValueError("dws_conversation_id_invalid")
    filter_json = json.dumps(
        {"field": "conversation_id", "op": "eq", "value": conversation_id},
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return build_dws_argv(
        ["--profile", profile, "event", "consume", DWS_REVIEW_EVENT_KEY,
         "--as", "user", "--filter-json", filter_json, "--flatten", "-f", "ndjson"],
        executable=executable,
    )


def resolve_cifra_conversation_id(profile: str, bot_open_id: str) -> str:
    """Resolve and validate the configured bot's one-to-one conversation via DWS read API."""
    if not isinstance(profile, str) or not profile.strip():
        raise ValueError("dws_profile_required")
    if not isinstance(bot_open_id, str) or not bot_open_id.strip():
        raise ValueError("bot_open_id_required")
    info = read_dws([
        "chat", "+conversation-info", "--open-dingtalk-id", bot_open_id,
        "--profile", profile,
    ])
    result = info.get("result")
    conversation = result.get("conversationInfo") if isinstance(result, dict) else None
    conversation_id = conversation.get("openConversationId") if isinstance(conversation, dict) else None
    if (not isinstance(conversation, dict) or info.get("success") is not True
            or conversation.get("singleChat") is not True
            or not isinstance(conversation_id, str) or not conversation_id
            or len(conversation_id) > 512):
        raise ValueError("conversation_identity_mismatch")
    return conversation_id


def dispatch_dws_review_event_lines(lines: Iterable[str], wakeups: "ReviewWakeups",
                                    conversation_id: str) -> int:
    """Forward only validated, scoped wakeups; message text never enters the queue."""
    accepted = 0
    for line in lines:
        message = parse_dws_review_event_line(line)
        if message is not None and wakeups.enqueue_dws_message(message, conversation_id):
            accepted += 1
    return accepted


def _read_dws_ready(stderr: Any, ready: threading.Event) -> None:
    for line in iter(stderr.readline, ""):
        if "[event] ready" in line:
            ready.set()


def _read_dws_event_stdout(stdout: Any, ready: threading.Event,
                           wakeups: "ReviewWakeups", conversation_id: str) -> None:
    if not ready.wait(DWS_REVIEW_READY_TIMEOUT_SECONDS):
        return
    dispatch_dws_review_event_lines(iter(stdout.readline, ""), wakeups, conversation_id)


def _load_stream_sdk() -> tuple[Any, Any] | None:
    try:
        import dingtalk_stream
        from dingtalk_stream import AckMessage
    except ImportError:
        return None
    return dingtalk_stream, AckMessage


def _run_dws_review_event_process(args: argparse.Namespace, wakeups: "ReviewWakeups",
                                  stop_event: threading.Event) -> int:
    """Own one optional DWS CLI stream process and close it gracefully on stop."""
    try:
        conversation_id = resolve_cifra_conversation_id(args.profile, args.bot_open_id)
        if stop_event.is_set():
            return 0
        argv = build_dws_review_event_argv(args.profile, conversation_id, executable=args.dws)
        process = start_dws_stream(
            argv[1:], executable=argv[0], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1, close_fds=True,
        )
    except (OSError, ValueError, RuntimeError):
        emit("dws_event_start_failed")
        return 2

    ready = threading.Event()
    stderr_thread = threading.Thread(
        target=_read_dws_ready, args=(process.stderr, ready),
        name="dws-review-stderr", daemon=True,
    )
    stderr_thread.start()
    deadline = time.monotonic() + DWS_REVIEW_READY_TIMEOUT_SECONDS
    while (not stop_event.is_set() and not ready.is_set()
           and process.poll() is None and time.monotonic() < deadline):
        ready.wait(0.1)

    stdout_thread: threading.Thread | None = None
    if ready.is_set() and not stop_event.is_set():
        emit("dws_event_ready")
        stdout_thread = threading.Thread(
            target=_read_dws_event_stdout,
            args=(process.stdout, ready, wakeups, conversation_id),
            name="dws-review-stdout", daemon=True,
        )
        stdout_thread.start()
    elif not stop_event.is_set():
        emit("dws_event_not_ready")

    try:
        while not stop_event.wait(0.2):
            if process.poll() is not None:
                emit("dws_event_process_exited")
                break
    finally:
        stdin = getattr(process, "stdin", None)
        if stdin is not None:
            try:
                stdin.close()
            except (OSError, ValueError):
                pass
        try:
            process.wait(timeout=DWS_REVIEW_STOP_TIMEOUT_SECONDS)
        except (OSError, subprocess.TimeoutExpired, TimeoutError):
            try:
                process.terminate()
                process.wait(timeout=5.0)
            except (OSError, subprocess.TimeoutExpired, TimeoutError):
                pass
        stderr_thread.join(timeout=2.0)
        if stdout_thread is not None:
            stdout_thread.join(timeout=2.0)

    if stop_event.is_set():
        return 0
    return 1


def emit(status: str, *, outcome: str | None = None) -> None:
    record: dict[str, str] = {"status": status}
    if outcome and outcome.replace("_", "").isalnum():
        record["outcome"] = outcome
    print(json.dumps(record, sort_keys=True), flush=True)


def read_credentials(path: Path) -> tuple[str, str]:
    if path.is_symlink() or not path.is_file():
        raise ValueError("stream_credentials_unavailable")
    info = path.stat()
    if info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise ValueError("stream_credentials_permissions_unsafe")
    try:
        credentials = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        raise ValueError("stream_credentials_invalid") from None
    if not isinstance(credentials, dict):
        raise ValueError("stream_credentials_invalid")
    client_id = credentials.get("client_id")
    client_secret = credentials.get("client_secret")
    if (not isinstance(client_id, str) or not client_id.strip()
            or not isinstance(client_secret, str) or not client_secret.strip()):
        raise ValueError("stream_credentials_invalid")
    return client_id, client_secret


class ReviewWakeups:
    def __init__(self, *, db_path: str, profile: str, bot_open_id: str,
                 app_id: str, lock_path: str | None = None,
                 card_config: CardCallbackConfig | None = None) -> None:
        self.db_path = db_path
        self.profile = profile
        self.bot_open_id = bot_open_id
        self.app_id = app_id
        self.lock_path = lock_path
        self.card_config = card_config
        self.pending: queue.Queue[tuple[str, str]] = queue.Queue(maxsize=64)
        self.card_pending: queue.Queue[str] = queue.Queue(maxsize=64)
        self._seen_dws_message_ids: OrderedDict[str, None] = OrderedDict()
        self._seen_dws_message_lock = threading.Lock()
        self.stopping = threading.Event()
        self.thread = threading.Thread(target=self._run, name="cifra-review-events", daemon=True)

    def start(self) -> None:
        if self.card_config is not None:
            conn = connect(self.db_path)
            try:
                for callback_id in recover_interrupted_card_work(conn):
                    self.enqueue_card_callback(callback_id)
            finally:
                conn.close()
        self.thread.start()

    def stop(self) -> bool:
        self.stopping.set()
        self.thread.join(timeout=REVIEW_WORKER_SHUTDOWN_TIMEOUT_SECONDS)
        still_running = self.thread.is_alive()
        if still_running:
            emit("review_worker_shutdown_timeout")
        return not still_running

    def enqueue(self, conversation_id: Any, message_id: Any, sender_id: Any) -> None:
        if (not isinstance(conversation_id, str) or not conversation_id
                or len(conversation_id) > 512 or not isinstance(message_id, str)
                or not message_id or len(message_id) > 256
                or not isinstance(sender_id, str) or not sender_id
                or sender_id == self.bot_open_id):
            return
        conn = connect(self.db_path)
        try:
            batches = conn.execute(
                "SELECT id FROM batch WHERE state='open' AND conversation_id=? LIMIT 2",
                (conversation_id,),
            ).fetchall()
        finally:
            conn.close()
        if len(batches) != 1:
            return
        try:
            self.pending.put_nowait((batches[0][0], conversation_id))
        except queue.Full:
            emit("review_event_queue_full")

    def enqueue_dws_message(self, message: DwsReviewMessage, expected_conversation_id: str) -> bool:
        """Queue an exact owner reply event as a wakeup; decisions still come from history."""
        if (not isinstance(message, DwsReviewMessage)
                or not isinstance(expected_conversation_id, str) or not expected_conversation_id
                or message.conversation_id != expected_conversation_id
                or message.sender_id == self.bot_open_id):
            return False
        conn = None
        try:
            conn = connect(self.db_path)
            batches = conn.execute(
                "SELECT id,owner_id FROM batch WHERE state='open' AND conversation_id=? LIMIT 2",
                (expected_conversation_id,),
            ).fetchall()
        except Exception:
            emit("review_event_state_unavailable")
            return False
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
        if len(batches) != 1 or batches[0][1] != message.sender_id:
            return False

        with self._seen_dws_message_lock:
            if message.message_id in self._seen_dws_message_ids:
                return False
            try:
                self.pending.put_nowait((batches[0][0], expected_conversation_id))
            except queue.Full:
                emit("review_event_queue_full")
                return False
            self._seen_dws_message_ids[message.message_id] = None
            self._seen_dws_message_ids.move_to_end(message.message_id)
            while len(self._seen_dws_message_ids) > DWS_REVIEW_MAX_SEEN_MESSAGE_IDS:
                self._seen_dws_message_ids.popitem(last=False)
        return True

    def persist_card_callback(self, callback: CardActionCallback) -> str:
        """Commit a verified callback before its Stream handler may acknowledge it."""
        config = self.card_config
        if config is None or not isinstance(callback, CardActionCallback):
            raise ValueError("card_callbacks_disabled")
        try:
            identity_key = card_identity_map_key(callback.user_id, callback.user_id_type)
        except ValueError:
            raise ValueError("card_identity_invalid") from None
        owner_id = config.identity_map.get(identity_key)
        if not owner_id:
            raise ValueError("card_identity_unmapped")
        conn = connect(self.db_path)
        try:
            result = process_card_callback(
                conn,
                callback,
                template_id=config.template_id,
                identity_resolver=lambda user_id, user_id_type: config.identity_map.get(
                    card_identity_map_key(user_id, user_id_type)
                ),
            )
            if result.get("status") not in ("recorded", "duplicate", "already_answered"):
                raise ValueError("card_callback_persistence_result_invalid")
            row = conn.execute(
                "SELECT work_state FROM review_card_evidence WHERE callback_id=?",
                (callback.callback_id,),
            ).fetchone()
            if result.get("status") in ("recorded", "duplicate"):
                if not row:
                    raise RuntimeError("card_callback_durable_row_missing")
                if row[0] == "pending":
                    self.enqueue_card_callback(callback.callback_id)
            return str(result["status"])
        finally:
            conn.close()

    def enqueue_card_callback(self, callback_id: str) -> bool:
        """Add a committed callback ID as a wake hint; SQL remains authoritative."""
        if (self.card_config is None or not isinstance(callback_id, str)
                or not callback_id or len(callback_id) > 256):
            return False
        try:
            self.card_pending.put_nowait(callback_id)
        except queue.Full:
            emit("card_callback_queue_full")
            return False
        return True

    def _next_pending_card_callback(self) -> str | None:
        if self.card_config is None:
            return None
        conn = connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT callback_id FROM review_card_evidence "
                "WHERE work_state='pending' ORDER BY message_time,callback_id LIMIT 1"
            ).fetchone()
            return row[0] if row and isinstance(row[0], str) else None
        finally:
            conn.close()

    def _run(self) -> None:
        prefer_card = True
        while not self.stopping.is_set():
            first_queue, second_queue = (
                (self.card_pending, self.pending) if prefer_card
                else (self.pending, self.card_pending)
            )
            prefer_card = not prefer_card
            item_queue = None
            item: Any = None
            try:
                item = first_queue.get_nowait()
                item_queue = first_queue
            except queue.Empty:
                try:
                    item = second_queue.get_nowait()
                    item_queue = second_queue
                except queue.Empty:
                    try:
                        item = first_queue.get(timeout=0.5)
                        item_queue = first_queue
                    except queue.Empty:
                        try:
                            callback_id = self._next_pending_card_callback()
                        except Exception:
                            emit("card_callback_state_unavailable")
                            continue
                        if callback_id is None:
                            continue
                        if self.stopping.is_set():
                            continue
                        self._process_card_callback(callback_id)
                        continue
            if self.stopping.is_set():
                if item_queue is not None:
                    item_queue.task_done()
                continue
            if item_queue is self.card_pending:
                try:
                    self._process_card_callback(item)
                finally:
                    self.card_pending.task_done()
                continue
            try:
                self._process_batch(item[0])
            finally:
                self.pending.task_done()

    def _process_card_callback(self, callback_id: str) -> None:
        config = self.card_config
        if config is None:
            return
        conn = None
        batch_id = None
        try:
            conn = connect(self.db_path)
            batch_id = claim_card_callback_work(conn, callback_id)
        except Exception:
            emit("card_callback_claim_failed")
            return
        finally:
            if conn is not None:
                conn.close()
        if batch_id is None:
                return
        unknown = False
        conn = None
        try:
            conn = connect(self.db_path)
            worker_result = run_review_worker(
                conn,
                self.profile,
                self.bot_open_id,
                self.app_id,
                self.lock_path,
                batch_id=batch_id,
            )
            outcome = worker_result.get("status") if isinstance(worker_result, dict) else None
            if not isinstance(worker_result, dict):
                unknown = True
            emit("card_callback_processed", outcome=outcome if isinstance(outcome, str) else None)
        except Exception:
            emit("card_callback_processing_error")
            unknown = True
        finally:
            if conn is not None:
                conn.close()
            state_conn = None
            try:
                state_conn = connect(self.db_path)
                finish_card_callback_work(state_conn, callback_id, unknown=unknown)
            except Exception:
                emit("card_callback_state_update_failed")
            finally:
                if state_conn is not None:
                    state_conn.close()

    def _process_batch(self, batch_id: str) -> None:
        for delay in (0.0, 0.5, 1.5, 3.0):
            if delay:
                time.sleep(delay)
            conn = connect(self.db_path)
            try:
                result = run_review_worker(
                    conn,
                    self.profile,
                    self.bot_open_id,
                    self.app_id,
                    self.lock_path,
                    batch_id=batch_id,
                )
            except Exception:
                emit("review_event_processing_error")
                return
            finally:
                conn.close()
            outcome = result.get("status") if isinstance(result, dict) else None
            if outcome in ("waiting", "waiting_for_clarification"):
                continue
            emit("review_event_processed", outcome=outcome if isinstance(outcome, str) else None)
            return
        emit("review_event_processed", outcome="history_not_ready")


async def run_dws_event_transport(args: argparse.Namespace) -> int:
    """Run the opt-in user-OAuth wake source alongside the existing SQL worker."""
    wakeups = ReviewWakeups(
        db_path=args.db,
        profile=args.profile,
        bot_open_id=args.bot_open_id,
        app_id=args.app_id,
        lock_path=args.lock,
    )
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stop_event.set)

    thread_stop = threading.Event()
    wakeups.start()
    transport_task = asyncio.create_task(
        asyncio.to_thread(_run_dws_review_event_process, args, wakeups, thread_stop)
    )
    stop_task = asyncio.create_task(stop_event.wait())
    emit("dws_event_starting")
    try:
        done, _ = await asyncio.wait(
            (transport_task, stop_task), return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_task in done:
            thread_stop.set()
        return await transport_task
    finally:
        thread_stop.set()
        stop_task.cancel()
        try:
            await transport_task
        except Exception:
            pass
        wakeups.stop()


async def run(args: argparse.Namespace) -> int:
    card_config = load_card_callback_config(args)
    if getattr(args, "transport", "stream") == "dws-event":
        return await run_dws_event_transport(args)

    if not args.credentials_file:
        raise ValueError("stream_credentials_unavailable")
    stream_sdk = _load_stream_sdk()
    if stream_sdk is None:
        emit("stream_dependency_unavailable")
        return 2
    dingtalk_stream, AckMessage = stream_sdk

    client_id, client_secret = read_credentials(Path(args.credentials_file))
    credential = dingtalk_stream.Credential(client_id, client_secret)
    client = dingtalk_stream.DingTalkStreamClient(credential)
    wakeups = ReviewWakeups(
        db_path=args.db,
        profile=args.profile,
        bot_open_id=args.bot_open_id,
        app_id=args.app_id,
        lock_path=args.lock,
        card_config=card_config,
    )
    register_stream_handlers(client, dingtalk_stream, AckMessage, wakeups, card_config)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stop_event.set)
    wakeups.start()
    stream_task = asyncio.create_task(
        run_shutdown_aware_stream(client, dingtalk_stream, stop_event)
    )
    emit("stream_starting")
    connected = False
    try:
        while not stop_event.is_set():
            if stream_task.done():
                try:
                    stream_task.result()
                except Exception:
                    emit("stream_stopped_unexpectedly")
                    return 1
                return 0
            is_connected = getattr(client, "websocket", None) is not None
            if is_connected and not connected:
                emit("stream_ready")
            elif connected and not is_connected:
                emit("stream_reconnecting")
            connected = is_connected
            await asyncio.sleep(1)
        return 0
    finally:
        stop_event.set()
        done, _ = await asyncio.wait(
            (stream_task,), timeout=STREAM_TASK_SHUTDOWN_TIMEOUT_SECONDS,
        )
        if not done:
            emit("stream_shutdown_timeout")
            stream_task.cancel()
            await asyncio.wait((stream_task,), timeout=1.0)
        else:
            try:
                stream_task.result()
            except asyncio.CancelledError:
                pass
            except Exception:
                emit("stream_stopped_unexpectedly")
        wakeups.stop()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--bot-open-id", required=True)
    parser.add_argument("--app-id", required=True)
    parser.add_argument("--credentials-file")
    parser.add_argument(
        "--transport", choices=("stream", "dws-event"), default="stream",
        help="message wake source; dws-event uses the configured user OAuth profile",
    )
    parser.add_argument("--dws", default="dws", help="DWS CLI executable")
    parser.add_argument("--lock")
    parser.add_argument("--card-callbacks", action="store_true",
                        help="opt in to the app-auth card action callback topic")
    parser.add_argument("--card-template-id",
                        help="required published template identifier when card callbacks are enabled")
    parser.add_argument("--card-identity-map-file",
                        help="private userId/userIdType to SQL owner ID map")
    args = parser.parse_args()
    if args.transport == "stream" and not args.credentials_file:
        parser.error("--credentials-file is required for --transport stream")
    logging.disable(logging.CRITICAL)
    try:
        return asyncio.run(run(args))
    except (OSError, ValueError):
        emit("stream_configuration_error")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
