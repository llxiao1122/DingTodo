import asyncio
import json
import os
import tempfile
import threading
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import review_stream_worker as worker
from review_card_adapter import (
    CARD_CALLBACK_TOPIC,
    CARD_DECISION_OPTIONS,
    CARD_FORM_FIELDS_PARAM,
    CARD_SUBMIT_ACTION_ID,
    card_identity_map_key,
    prepare_card_review_input,
)
from review_inbox import connect, register_batch


BATCH_ID = "batch_12345678"
CALLBACK_TIME = "2026-11-07 10:02:00.123"
STREAM_HEADER_TIME = int(datetime(
    2026, 11, 7, 2, 2, 0, 123000, tzinfo=timezone.utc,
).timestamp() * 1000)
EXPIRED_HEADER_TIME = int(datetime(
    2026, 11, 7, 1, 59, 59, 999000, tzinfo=timezone.utc,
).timestamp() * 1000)
PLATFORM_USER = "platform-user-42"
OWNER_ID = "owner-open-id"
TEMPLATE_ID = "published-template"


def card_payload(**overrides):
    payload = {
        "type": "actionCallback",
        "callbackId": "event-card-1",
        "outTrackId": BATCH_ID,
        "userId": PLATFORM_USER,
        "userIdType": 1,
        "content": json.dumps({
            "cardPrivateData": {
                "actionIds": [CARD_SUBMIT_ACTION_ID],
                "params": {CARD_FORM_FIELDS_PARAM: [{
                    "type": "SELECT",
                    "required": True,
                    "name": "dtd_review_1",
                    "label": "synthetic review item",
                    "placeholder": "请选择创建待办或忽略",
                    "options": CARD_DECISION_OPTIONS,
                    "default_number": 2,
                }]},
            },
        }),
    }
    payload.update(overrides)
    return payload


def stream_callback(payload=None, *, header_time=STREAM_HEADER_TIME, include_headers=True):
    callback = SimpleNamespace(data=card_payload() if payload is None else payload)
    if include_headers:
        callback.headers = SimpleNamespace(time=header_time)
    return callback


def identity_map_file(directory: str) -> str:
    path = Path(directory) / "identity-map.json"
    path.write_text(json.dumps({
        card_identity_map_key(PLATFORM_USER, 1): OWNER_ID,
    }), encoding="utf-8")
    os.chmod(path, 0o600)
    return str(path)


class FakeChatbotHandler:
    def __init__(self):
        pass


class FakeAckMessage:
    STATUS_OK = 200


class FakeStreamClient:
    def __init__(self):
        self.registered = []

    def register_callback_handler(self, topic, handler):
        self.registered.append((topic, handler))


class FakeWakeups:
    def __init__(self):
        self.chat_messages = []
        self.card_messages = []

    def enqueue(self, conversation_id, message_id, sender_id):
        self.chat_messages.append((conversation_id, message_id, sender_id))

    def persist_card_callback(self, callback):
        self.card_messages.append(callback)
        return "recorded"


class FakeSdk:
    ChatbotHandler = FakeChatbotHandler
    chatbot = SimpleNamespace(
        ChatbotMessage=SimpleNamespace(
            TOPIC="chatbot-topic",
            from_dict=staticmethod(lambda payload: SimpleNamespace(
                conversation_id=payload["conversation_id"],
                message_id=payload["message_id"],
                sender_id=payload["sender_id"],
            )),
        ),
    )


def test_default_stream_registration_does_not_add_card_topic():
    client = FakeStreamClient()
    wakeups = FakeWakeups()

    handlers = worker.register_stream_handlers(
        client, FakeSdk, FakeAckMessage, wakeups,
    )

    assert len(handlers) == 1
    assert [topic for topic, _handler in client.registered] == ["chatbot-topic"]


def test_opted_in_registration_adds_only_card_callback_topic():
    client = FakeStreamClient()
    wakeups = FakeWakeups()
    config = worker.CardCallbackConfig(
        TEMPLATE_ID,
        {card_identity_map_key(PLATFORM_USER, 1): OWNER_ID},
    )

    handlers = worker.register_stream_handlers(
        client, FakeSdk, FakeAckMessage, wakeups, config,
    )

    assert len(handlers) == 2
    assert [topic for topic, _handler in client.registered] == [
        "chatbot-topic",
        CARD_CALLBACK_TOPIC,
    ]


def test_card_handler_persists_before_returning_ack():
    client = FakeStreamClient()
    wakeups = FakeWakeups()
    config = worker.CardCallbackConfig(
        TEMPLATE_ID,
        {card_identity_map_key(PLATFORM_USER, 1): OWNER_ID},
    )
    worker.register_stream_handlers(client, FakeSdk, FakeAckMessage, wakeups, config)
    handler = client.registered[1][1]
    result = asyncio.run(handler.process(stream_callback(
        card_payload(eventTime="not-a-trusted-timestamp"),
    )))

    assert result == (FakeAckMessage.STATUS_OK, "OK")
    assert len(wakeups.card_messages) == 1
    assert wakeups.card_messages[0].message_time == CALLBACK_TIME


@pytest.mark.parametrize(
    "updates",
    [
        {"userId": "unmapped-user"},
        {"content": "not-json"},
    ],
)
def test_card_handler_fails_closed_but_acks_malformed_or_unmapped_event(updates):
    client = FakeStreamClient()
    wakeups = FakeWakeups()
    config = worker.CardCallbackConfig(
        TEMPLATE_ID,
        {card_identity_map_key(PLATFORM_USER, 1): OWNER_ID},
    )
    worker.register_stream_handlers(client, FakeSdk, FakeAckMessage, wakeups, config)

    result = asyncio.run(client.registered[1][1].process(
        stream_callback(card_payload(**updates)),
    ))

    assert result == (FakeAckMessage.STATUS_OK, "OK")
    assert wakeups.card_messages == []


def test_card_handler_rejects_missing_or_invalid_sdk_push_timestamps():
    invalid_callbacks = [
        stream_callback(include_headers=False),
        stream_callback(header_time=None),
        stream_callback(header_time="1794016920123"),
        stream_callback(header_time=True),
        stream_callback(header_time=False),
        stream_callback(header_time=0),
        stream_callback(header_time=-1),
        stream_callback(header_time=1.5),
    ]
    for callback in invalid_callbacks:
        client = FakeStreamClient()
        wakeups = FakeWakeups()
        config = worker.CardCallbackConfig(
            TEMPLATE_ID,
            {card_identity_map_key(PLATFORM_USER, 1): OWNER_ID},
        )
        worker.register_stream_handlers(client, FakeSdk, FakeAckMessage, wakeups, config)

        result = asyncio.run(client.registered[1][1].process(callback))

        assert result == (FakeAckMessage.STATUS_OK, "OK")
        assert wakeups.card_messages == []


def test_expired_sdk_push_timestamp_is_rejected_by_batch_prompt_gate(tmp_path):
    connection = connect(str(tmp_path / "review.sqlite3"))
    register_batch(
        connection,
        BATCH_ID,
        "owner-conversation",
        OWNER_ID,
        "text-prompt",
        "2026-11-07 10:00:00",
        {1: ["701"]},
    )
    client = FakeStreamClient()
    wakeups = FakeWakeups()
    config = worker.CardCallbackConfig(
        TEMPLATE_ID,
        {card_identity_map_key(PLATFORM_USER, 1): OWNER_ID},
    )
    worker.register_stream_handlers(client, FakeSdk, FakeAckMessage, wakeups, config)

    try:
        result = asyncio.run(client.registered[1][1].process(
            stream_callback(header_time=EXPIRED_HEADER_TIME),
        ))
        assert result == (FakeAckMessage.STATUS_OK, "OK")
        assert len(wakeups.card_messages) == 1
        assert wakeups.card_messages[0].message_time == "2026-11-07 09:59:59.999"
        with pytest.raises(ValueError, match="card_callback_not_after_prompt"):
            prepare_card_review_input(
                connection,
                wakeups.card_messages[0],
                identity_resolver=lambda _user_id, _user_id_type: OWNER_ID,
            )
    finally:
        connection.close()


def test_card_configuration_is_required_only_when_feature_is_enabled():
    disabled = SimpleNamespace(card_callbacks=False, transport="stream")
    assert worker.load_card_callback_config(disabled) is None

    enabled_but_incomplete = SimpleNamespace(
        card_callbacks=True,
        transport="stream",
        card_template_id=None,
        card_identity_map_file=None,
    )
    with pytest.raises(ValueError, match="card_template_id_required"):
        worker.load_card_callback_config(enabled_but_incomplete)


def test_card_configuration_loads_private_identity_map_and_rejects_other_transport(tmp_path):
    path = identity_map_file(str(tmp_path))
    args = SimpleNamespace(
        card_callbacks=True,
        transport="stream",
        card_template_id=TEMPLATE_ID,
        card_identity_map_file=path,
    )

    config = worker.load_card_callback_config(args)

    assert config is not None
    assert config.identity_map[card_identity_map_key(PLATFORM_USER, 1)] == OWNER_ID
    assert not hasattr(config, "timestamp_field")
    with pytest.raises(ValueError, match="card_callbacks_require_app_stream"):
        worker.load_card_callback_config(SimpleNamespace(**{
            **vars(args),
            "transport": "dws-event",
        }))


def test_card_queue_contains_only_persisted_callback_id_hints():
    config = worker.CardCallbackConfig(
        TEMPLATE_ID,
        {card_identity_map_key(PLATFORM_USER, 1): OWNER_ID},
    )
    wakeups = worker.ReviewWakeups(
        db_path="unused-in-fake-test",
        profile="test-profile",
        bot_open_id="bot-open-id",
        app_id="app-id",
        card_config=config,
    )
    assert wakeups.enqueue_card_callback("event-card-1")
    assert wakeups.enqueue_card_callback("event-card-1")
    assert wakeups.card_pending.qsize() == 2
    assert wakeups.card_pending.get_nowait() == "event-card-1"


def test_card_handler_commits_evidence_before_ack_and_restart_recovers_it(tmp_path, monkeypatch):
    database_path = str(tmp_path / "review.sqlite3")
    connection = connect(database_path)
    register_batch(
        connection,
        BATCH_ID,
        "owner-conversation",
        OWNER_ID,
        "text-prompt",
        "2026-11-07 10:00:00",
        {1: ["701"]},
    )
    connection.close()
    config = worker.CardCallbackConfig(
        TEMPLATE_ID,
        {card_identity_map_key(PLATFORM_USER, 1): OWNER_ID},
    )
    original_wakeups = worker.ReviewWakeups(
        db_path=database_path,
        profile="test-profile",
        bot_open_id="bot-open-id",
        app_id="app-id",
        card_config=config,
    )
    client = FakeStreamClient()
    worker.register_stream_handlers(client, FakeSdk, FakeAckMessage, original_wakeups, config)

    result = asyncio.run(client.registered[1][1].process(stream_callback()))

    assert result == (FakeAckMessage.STATUS_OK, "OK")
    connection = connect(database_path)
    try:
        assert connection.execute(
            "SELECT source FROM reply WHERE batch_id=?", (BATCH_ID,),
        ).fetchone()[0] == "card"
        assert connection.execute(
            "SELECT work_state FROM review_card_evidence WHERE callback_id=?",
            ("event-card-1",),
        ).fetchone()[0] == "pending"
    finally:
        connection.close()

    processed = threading.Event()
    review_calls = []

    def fake_review_worker(conn, profile, bot_open_id, app_id, lock_path, *, batch_id):
        assert batch_id == BATCH_ID
        review_calls.append(batch_id)
        processed.set()
        return {"status": "pending"}

    monkeypatch.setattr(worker, "run_review_worker", fake_review_worker)
    restarted = worker.ReviewWakeups(
        db_path=database_path,
        profile="test-profile",
        bot_open_id="bot-open-id",
        app_id="app-id",
        card_config=config,
    )
    restarted.start()
    assert processed.wait(2)
    assert restarted.stop()

    original_wakeups.card_pending.get_nowait()
    assert asyncio.run(client.registered[1][1].process(stream_callback())) == (
        FakeAckMessage.STATUS_OK, "OK",
    )
    assert original_wakeups.card_pending.empty()
    assert review_calls == [BATCH_ID]

    connection = connect(database_path)
    try:
        assert connection.execute(
            "SELECT work_state FROM review_card_evidence WHERE callback_id=?",
            ("event-card-1",),
        ).fetchone()[0] == "dispatched"
        assert connection.execute("SELECT COUNT(*) FROM reply").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM review_card_evidence").fetchone()[0] == 1
    finally:
        connection.close()


@pytest.mark.parametrize(
    "failure",
    [OSError("synthetic database unavailable"), ValueError("synthetic persistence invariant")],
)
def test_card_persistence_failure_does_not_return_stream_ack(monkeypatch, failure):
    config = worker.CardCallbackConfig(
        TEMPLATE_ID,
        {card_identity_map_key(PLATFORM_USER, 1): OWNER_ID},
    )
    wakeups = worker.ReviewWakeups(
        db_path="unavailable-db",
        profile="test-profile",
        bot_open_id="bot-open-id",
        app_id="app-id",
        card_config=config,
    )
    client = FakeStreamClient()
    worker.register_stream_handlers(client, FakeSdk, FakeAckMessage, wakeups, config)
    monkeypatch.setattr(
        worker,
        "connect",
        lambda _path: (_ for _ in ()).throw(failure),
    )

    with pytest.raises(type(failure), match="synthetic"):
        asyncio.run(client.registered[1][1].process(stream_callback()))


class FakeRequests:
    def __init__(self):
        self.timeouts = []

    def post(self, *_args, **kwargs):
        self.timeouts.append(kwargs.get("timeout"))
        return object()


class FakeWebsocket:
    def __init__(self):
        self.connected = asyncio.Event()
        self.closed = False

    async def __aenter__(self):
        self.connected.set()
        return self

    async def __aexit__(self, *_args):
        self.closed = True

    async def recv(self):
        await asyncio.Future()


class FakeWebsocketConnect:
    def __init__(self):
        self.websocket = FakeWebsocket()
        self.options = None

    def __call__(self, _uri, **options):
        self.options = options
        return self.websocket


class FakeShutdownStreamClient:
    def __init__(self, stream_module):
        self.stream_module = stream_module
        self.websocket = None
        self.keepalive_cancelled = asyncio.Event()

    def pre_start(self):
        pass

    def open_connection(self):
        self.stream_module.requests.post("https://example.invalid/token")
        return {"endpoint": "wss://example.invalid/stream", "ticket": "synthetic"}

    async def keepalive(self, websocket):
        try:
            await asyncio.Future()
        finally:
            self.keepalive_cancelled.set()


@pytest.mark.parametrize("cancel_task", [False, True])
def test_shutdown_aware_stream_bounds_sdk_http_and_closes_on_stop_or_cancel(cancel_task):
    async def scenario():
        requests = FakeRequests()
        connect = FakeWebsocketConnect()
        stream_module = SimpleNamespace(
            requests=requests,
            quote_plus=urllib.parse.quote_plus,
            websockets=SimpleNamespace(connect=connect),
        )
        sdk = SimpleNamespace(stream=stream_module)
        client = FakeShutdownStreamClient(stream_module)
        stop_event = asyncio.Event()
        original_requests = stream_module.requests
        task = asyncio.create_task(
            worker.run_shutdown_aware_stream(client, sdk, stop_event),
        )
        for _ in range(100):
            if connect.websocket.connected.is_set():
                break
            await asyncio.sleep(0.01)
        assert connect.websocket.connected.is_set()
        assert requests.timeouts == [(
            worker.STREAM_HTTP_CONNECT_TIMEOUT_SECONDS,
            worker.STREAM_HTTP_READ_TIMEOUT_SECONDS,
        )]
        assert connect.options == {
            "open_timeout": worker.STREAM_WEBSOCKET_OPEN_TIMEOUT_SECONDS,
            "close_timeout": worker.STREAM_WEBSOCKET_CLOSE_TIMEOUT_SECONDS,
        }

        if cancel_task:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=2)
        else:
            stop_event.set()
            await asyncio.wait_for(task, timeout=2)

        assert connect.websocket.closed
        assert client.websocket is None
        assert client.keepalive_cancelled.is_set()
        assert stream_module.requests is original_requests

    asyncio.run(scenario())


def test_card_callback_is_processed_on_worker_thread_and_reaches_existing_worker(monkeypatch):
    config = worker.CardCallbackConfig(
        TEMPLATE_ID,
        {card_identity_map_key(PLATFORM_USER, 1): OWNER_ID},
    )
    wakeups = worker.ReviewWakeups(
        db_path="unused-in-fake-test",
        profile="test-profile",
        bot_open_id="bot-open-id",
        app_id="app-id",
        card_config=config,
    )
    callback_id = "event-card-1"
    calls = []
    processed = threading.Event()

    class FakeConnection:
        def close(self):
            calls.append(("close", threading.current_thread().name))

    monkeypatch.setattr(worker, "connect", lambda _path: FakeConnection())

    monkeypatch.setattr(
        worker, "recover_interrupted_card_work", lambda _conn: [callback_id],
    )
    monkeypatch.setattr(
        worker, "claim_card_callback_work",
        lambda _conn, received: calls.append(("claim", threading.current_thread().name, received))
        or BATCH_ID,
    )
    monkeypatch.setattr(
        worker, "finish_card_callback_work",
        lambda _conn, received, *, unknown=False: calls.append(
            ("finish", threading.current_thread().name, received, unknown)
        ),
    )

    def fake_review_worker(conn, profile, bot_open_id, app_id, lock_path, *, batch_id):
        calls.append(("review", threading.current_thread().name, batch_id))
        processed.set()
        return {"status": "pending"}

    monkeypatch.setattr(worker, "run_review_worker", fake_review_worker)

    wakeups.start()
    assert processed.wait(2)
    assert wakeups.stop()

    assert any(call[0] == "claim" and call[1] != threading.current_thread().name
               and call[2] == callback_id for call in calls)
    assert any(call[0] == "review" and call[1] != threading.current_thread().name
               and call[2] == BATCH_ID for call in calls)
    assert any(call[0] == "finish" and call[1] != threading.current_thread().name
               and call[2:] == (callback_id, False) for call in calls)
