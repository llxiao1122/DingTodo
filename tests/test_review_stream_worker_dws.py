import asyncio
import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import review_stream_worker as worker


def dws_message(**overrides):
    payload = {
        "type": worker.DWS_REVIEW_EVENT_KEY,
        "event_id": "event-1",
        "message_id": "message-1",
        "conversation_id": "cifra-dm",
        "sender_open_dingtalk_id": "owner-open-id",
        "create_time": "2026-11-07 10:01:00",
        "content": "  1, 创建待办  \n",
    }
    payload.update(overrides)
    return payload


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, batches):
        self.batches = batches
        self.closed = False
        self.statements = []

    def execute(self, statement, parameters):
        self.statements.append((statement, parameters))
        assert statement == (
            "SELECT id,owner_id FROM batch WHERE state='open' AND conversation_id=? LIMIT 2"
        )
        conversation_id = parameters[0]
        return FakeCursor([row for row in self.batches if row[2] == conversation_id])

    def close(self):
        self.closed = True


def build_wakeups(monkeypatch, batches=None):
    connection = FakeConnection(batches or [("batch-12345678", "owner-open-id", "cifra-dm")])
    monkeypatch.setattr(worker, "connect", lambda _path: connection)
    wakeups = worker.ReviewWakeups(
        db_path="unused-in-fake-tests",
        profile="test-profile",
        bot_open_id="bot-1",
        app_id="app-1",
    )
    return wakeups, connection


def test_dws_message_parser_preserves_exact_content():
    content = "  复核编号：batch-12345678；1, 创建待办 \n"

    message = worker.parse_dws_review_event(dws_message(content=content))

    assert message is not None
    assert message.content == content
    assert message.create_time == "2026-11-07 10:01:00"


@pytest.mark.parametrize(
    "field",
    [
        "event_id",
        "message_id",
        "conversation_id",
        "sender_open_dingtalk_id",
        "create_time",
        "content",
    ],
)
def test_dws_event_parser_rejects_missing_required_fields(field):
    payload = dws_message()
    del payload[field]

    assert worker.parse_dws_review_event(payload) is None


def test_dws_event_parser_rejects_invalid_time_and_non_message_event():
    assert worker.parse_dws_review_event(dws_message(create_time="not-a-time")) is None
    assert worker.parse_dws_review_event(dws_message(type="user_im_message_read_o2o")) is None


def test_dws_command_uses_personal_oauth_and_remote_conversation_filter():
    argv = worker.build_dws_review_event_argv(
        "corp:user", "cifra-dm", executable="dws-test",
    )

    assert argv[:5] == ["dws-test", "--profile", "corp:user", "event", "consume"]
    assert worker.DWS_REVIEW_EVENT_KEY in argv
    assert argv[argv.index("--as") + 1] == "user"
    filter_json = json.loads(argv[argv.index("--filter-json") + 1])
    assert filter_json == {"field": "conversation_id", "op": "eq", "value": "cifra-dm"}
    assert argv[-3:] == ["--flatten", "-f", "ndjson"]


def test_conversation_resolution_uses_only_read_only_conversation_info(monkeypatch):
    calls = []

    def fake_read(arguments):
        calls.append(arguments)
        return {
            "success": True,
            "result": {"conversationInfo": {"singleChat": True, "openConversationId": "cifra-dm"}},
        }

    monkeypatch.setattr(worker, "read_dws", fake_read)

    assert worker.resolve_cifra_conversation_id("corp:user", "bot-1") == "cifra-dm"
    assert calls == [[
        "chat", "+conversation-info", "--open-dingtalk-id", "bot-1",
        "--profile", "corp:user",
    ]]


def test_review_wakeup_filters_bot_wrong_conversation_and_wrong_owner(monkeypatch):
    wakeups, connection = build_wakeups(monkeypatch)

    bot_echo = worker.parse_dws_review_event(
        dws_message(sender_open_dingtalk_id="bot-1", message_id="bot-echo"),
    )
    wrong_conversation = worker.parse_dws_review_event(
        dws_message(message_id="wrong-conversation"),
    )
    wrong_owner = worker.parse_dws_review_event(
        dws_message(sender_open_dingtalk_id="owner-user-id", message_id="wrong-owner"),
    )

    assert bot_echo is not None and not wakeups.enqueue_dws_message(bot_echo, "cifra-dm")
    assert wrong_conversation is not None and not wakeups.enqueue_dws_message(wrong_conversation, "other-dm")
    assert wrong_owner is not None and not wakeups.enqueue_dws_message(wrong_owner, "cifra-dm")
    assert wakeups.pending.empty()
    assert len(connection.statements) == 1


def test_fake_ndjson_duplicate_message_id_enqueues_one_wakeup(monkeypatch):
    wakeups, connection = build_wakeups(monkeypatch)
    line = json.dumps(dws_message(), ensure_ascii=False)

    accepted = worker.dispatch_dws_review_event_lines(
        [line, line], wakeups, "cifra-dm",
    )

    assert accepted == 1
    assert wakeups.pending.get_nowait() == ("batch-12345678", "cifra-dm")
    assert wakeups.pending.empty()
    assert len(connection.statements) == 2
    assert all(statement.startswith("SELECT ") for statement, _ in connection.statements)


def test_fake_ndjson_malformed_lines_and_missing_fields_do_not_wake(monkeypatch):
    wakeups, connection = build_wakeups(monkeypatch)
    missing_message_id = dws_message()
    missing_message_id.pop("message_id")
    lines = [
        "not-json",
        json.dumps(missing_message_id),
        json.dumps(dws_message(type="unrelated-event")),
    ]

    accepted = worker.dispatch_dws_review_event_lines(lines, wakeups, "cifra-dm")

    assert accepted == 0
    assert wakeups.pending.empty()
    assert connection.statements == []


def test_wakeup_queue_does_not_receive_message_content(monkeypatch):
    wakeups, _connection = build_wakeups(monkeypatch)
    payload = dws_message(content="  exact reply text  ")
    lines = [json.dumps(payload)]

    assert worker.dispatch_dws_review_event_lines(lines, wakeups, "cifra-dm") == 1
    assert wakeups.pending.get_nowait() == ("batch-12345678", "cifra-dm")


def test_default_stream_path_stays_default_and_reports_missing_sdk(capsys):
    args = SimpleNamespace(credentials_file="unused-credentials-file")

    with patch.object(worker, "_load_stream_sdk", return_value=None):
        assert asyncio.run(worker.run(args)) == 2

    assert json.loads(capsys.readouterr().out) == {"status": "stream_dependency_unavailable"}
