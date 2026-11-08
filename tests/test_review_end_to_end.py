from __future__ import annotations

import copy
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from unittest.mock import patch
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import dws_adapter
import review_auto_process
import review_auto_schedule
import review_dispatch_send
import review_handoff
import review_inbox_worker
import review_resolve_create
import review_schedule_dws
from review_gateway import ReviewGateway
from review_inbox import connect, ingest_reply, register_batch
from review_schedule_dws import ReviewScheduleDws


CHINA_TZ = ZoneInfo("Asia/Shanghai")
ACTION_ID = "701"
BATCH_ID = "review_e2e_0001"
BOT_ID = "cifra-bot"
CONVERSATION_ID = "owner-dm"
OWNER_ID = "owner-open-id"
OWNER_USER_ID = "owner-user-id"
OWNER_NAME = "测试负责人"
APP_ID = "cifra-app"
SOURCE_TEXT = "明天15:00前更新配电柜巡检记录"


class FakeWorkQueryAdapter:
    """Isolated source and action ledger used by both discovery and the resolver."""

    def __init__(self, updated_at: str, source_time: str):
        self.action = {
            "id": int(ACTION_ID),
            "action_key": "e2e-action",
            "status": "REVIEW",
            "created_at": updated_at,
            "updated_at": updated_at,
            "conversation_id": "source-conversation",
            "message_id": "source-message",
            "business_key": "source-business-key",
            "external_task_id": None,
            "effective_external_task_id": None,
            "duplicate_of_action_id": None,
        }
        self.source = {
            "conversation_id": "source-conversation",
            "message_id": "source-message",
            "group_name": "设备巡检群",
            "sender_name": "班组长",
            "message_time": source_time,
            "text": SOURCE_TEXT,
        }
        self.source_reads: list[tuple[str, str]] = []

    def query(self, request: dict) -> dict:
        if request.get("view") == "message":
            identity = (request.get("conversation_id"), request.get("message_id"))
            self.source_reads.append(identity)
            matches = [self.source] if identity == (
                self.source["conversation_id"], self.source["message_id"]
            ) else []
            return {
                "view": "message",
                "rows": copy.deepcopy(matches),
                "returned_count": len(matches),
                "matched_count": len(matches),
                "has_more": False,
            }

        if request.get("view") != "actions":
            raise AssertionError(f"unexpected isolated adapter query: {request!r}")

        rows = [self.action]
        if request.get("action_status") == "REVIEW":
            rows = [row for row in rows if row["status"] == "REVIEW"]
        if request.get("since") and request.get("until"):
            updated_day = self.action["updated_at"][:10]
            rows = [row for row in rows
                    if request["since"] <= updated_day < request["until"]]
        if request.get("conversation_id"):
            rows = [row for row in rows
                    if row["conversation_id"] == request["conversation_id"]]
        if request.get("message_id"):
            rows = [row for row in rows if row["message_id"] == request["message_id"]]
        rows = copy.deepcopy(rows)
        return {
            "view": "actions",
            "rows": rows,
            "returned_count": len(rows),
            "matched_count": len(rows),
            "has_more": False,
            "next_page_token": None,
        }


class FakeCifra:
    """Local Cifra boundary: accepts sends and exposes complete DM history."""

    def __init__(self):
        self.messages: list[dict] = []
        self.outbound: list[dict] = []

    def full_history(self) -> dict:
        return {
            "complete": True,
            "partial": False,
            "hasMore": False,
            "failures": [],
            "messages": copy.deepcopy(self.messages),
        }

    def add_owner_reply(self, text: str) -> dict:
        prompt = next(message for message in self.messages
                      if message.get("senderId") == BOT_ID
                      and message.get("text", "").startswith(f"复核编号：{BATCH_ID}"))
        prompt_time = datetime.strptime(prompt["createTime"], "%Y-%m-%d %H:%M:%S")
        reply = {
            "messageId": "owner-reply-1",
            "conversationId": CONVERSATION_ID,
            "senderId": OWNER_ID,
            "createTime": (prompt_time + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S"),
            "text": text,
        }
        self.messages.append(reply)
        return reply

    def dws(self, arguments: list[str]) -> dict:
        command = arguments[:2]
        if command == ["chat", "+conversation-info"]:
            return {
                "success": True,
                "result": {"conversationInfo": {
                    "singleChat": True,
                    "openConversationId": CONVERSATION_ID,
                }},
            }
        if command == ["dev", "app"]:
            return {"ok": True, "data": {
                "robotStatus": "ONLINE",
                "robotCode": "cifra-robot-code",
                "openDingTalkId": BOT_ID,
            }}
        if command == ["chat", "+messages-send"]:
            text = arguments[arguments.index("--text") + 1]
            if "--dry-run" in arguments:
                return {
                    "dry_run": True,
                    "executed": False,
                    "actions": [{
                        "identity": "bot",
                        "arguments": {
                            "robotCode": "cifra-robot-code",
                            "userIds": [OWNER_USER_ID],
                            "markdown": text,
                        },
                    }],
                }

            message_id = f"cifra-outbound-{len(self.outbound) + 1}"
            message = {
                "messageId": message_id,
                "conversationId": CONVERSATION_ID,
                "senderId": BOT_ID,
                "createTime": datetime.now(CHINA_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                "text": text,
            }
            self.outbound.append({"message_id": message_id, "text": text})
            self.messages.append(message)
            return {"ok": True, "result": {"success": True}}
        raise AssertionError(f"unexpected simulated Cifra command: {arguments!r}")

    def read_dws(self, arguments: list[str]) -> dict:
        command = arguments[:2]
        if command == ["chat", "+conversation-info"]:
            return self.dws(arguments)
        if command == ["chat", "+chat-messages"]:
            return self.full_history()
        if command == ["dev", "app"]:
            return self.dws(arguments)
        if command == ["chat", "+messages-send"]:
            return self.dws(arguments)
        if arguments[:2] == ["profile", "list"]:
            return {"profiles": [{
                "profile": "isolated-profile",
                "isOrgCurrent": True,
                "userId": OWNER_USER_ID,
                "userName": OWNER_NAME,
            }]}
        if arguments[:3] == ["contact", "user", "search"]:
            return {"success": True, "result": [{
                "name": OWNER_NAME,
                "userId": OWNER_USER_ID,
                "openDingTalkId": OWNER_ID,
            }]}
        raise AssertionError(f"unexpected simulated DWS read: {arguments!r}")


class FakeWorkWriter:
    def __init__(self, adapter: FakeWorkQueryAdapter):
        self.adapter = adapter
        self.calls: list[dict] = []

    def execute(self, request: dict) -> dict:
        self.calls.append(copy.deepcopy(request))
        operation = request["operation"]
        if operation == "claim_review_create":
            if request.get("dry_run") is True:
                return {"ok": True, "committed": False, "row": {"status": "CLAIMED"}}
            if self.adapter.action["status"] != "REVIEW":
                raise AssertionError("review claim should only affect the pending action")
            self.adapter.action["status"] = "CLAIMED"
            return {"ok": True, "committed": True, "action_id": ACTION_ID,
                    "row": {"id": int(ACTION_ID), "status": "CLAIMED"}}
        if operation == "link_external_task":
            self.adapter.action["external_task_id"] = request["external_task_id"]
            self.adapter.action["effective_external_task_id"] = request["external_task_id"]
            return {"ok": True, "committed": True}
        if operation == "save_action_result":
            self.adapter.action["status"] = request["status"]
            return {"ok": True, "committed": True}
        raise AssertionError(f"unexpected isolated writer operation: {request!r}")


class FakeDwsClient:
    def __init__(self, state: dict, **_kwargs):
        self.state = state

    def search_todos(self, _query: str) -> list[dict]:
        return []

    def run(self, arguments: list[str]) -> dict:
        title = arguments[arguments.index("--title") + 1]
        executor_ids = arguments[arguments.index("--executors") + 1].split(",")
        due_iso = arguments[arguments.index("--due") + 1] if "--due" in arguments else None
        priority = int(arguments[arguments.index("--priority") + 1])
        self.state["create_preview"] = {
            "title": title,
            "executor_ids": executor_ids,
            "due_iso": due_iso,
            "priority": priority,
        }
        return {
            "dry_run": True,
            "executed": False,
            "arguments": {"PersonalTodoCreateVO": {
                "subject": title,
                "executorIds": executor_ids,
                "priority": priority,
                "dueTime": dws_adapter._epoch_ms(due_iso),
            }},
        }

    def create_todo(self, plan: dict, priority: int = 20) -> dict:
        self.state["create_calls"] += 1
        if priority != 20:
            raise AssertionError("resolver should preserve its normal priority")
        self.state["create_plan"] = copy.deepcopy(plan)
        self.state["todo"] = {
            "taskId": "todo-e2e-701",
            "subject": plan["title"],
            "isDone": False,
            "executorInfos": [{"name": OWNER_NAME, "userId": OWNER_USER_ID}],
            "priority": priority,
            "dueTime": dws_adapter._epoch_ms(plan.get("due_iso")) or 0,
        }
        return {"task_id": "todo-e2e-701"}

    def get_todo(self, task_id: str) -> dict:
        todo = self.state.get("todo")
        if not todo or todo["taskId"] != str(task_id):
            raise AssertionError(f"unexpected todo readback: {task_id!r}")
        return copy.deepcopy(todo)

    def add_source_comment(self, task_id: str, content: str) -> dict:
        self.state["comments"].append((str(task_id), content))
        return {"ok": True}

    def verify_source_comment(self, task_id: str, content: str) -> None:
        if (str(task_id), content) not in self.state["comments"]:
            raise AssertionError("source comment readback did not match the write")
        self.state["verified_comments"].append((str(task_id), content))


@pytest.mark.parametrize('source_text,has_due', [
    ('明天15:00前更新配电柜巡检记录', True),
    ('巡查配电柜温度', False),
])
def test_review_pipeline_runs_from_discovery_to_verified_completion(tmp_path, monkeypatch, source_text, has_due):
    monkeypatch.setattr(sys.modules[__name__], 'SOURCE_TEXT', source_text)
    now = datetime.now(CHINA_TZ).replace(microsecond=0)
    now_text = now.strftime("%Y-%m-%d %H:%M:%S")
    source_datetime = now - timedelta(minutes=12)
    source_time = source_datetime.strftime("%Y-%m-%d %H:%M:%S")
    expected_due_iso = f"{(source_datetime.date() + timedelta(days=1)).isoformat()}T15:00:00+08:00" if has_due else None
    expected_due_ms = dws_adapter._epoch_ms(expected_due_iso) or 0
    adapter = FakeWorkQueryAdapter(now_text, source_time)
    cifra = FakeCifra()
    external = {"create_calls": 0, "todo": None, "comments": [], "verified_comments": []}
    writer = FakeWorkWriter(adapter)
    conn = connect(str(tmp_path / "review.sqlite3"))
    lock_path = str(tmp_path / "review-resolution.lock")
    review_auto_schedule.enable(conn, now - timedelta(days=1))

    schedule_dws = ReviewScheduleDws(
        conn,
        profile="isolated-profile",
        bot_open_id=BOT_ID,
        owner_open_id=OWNER_ID,
        app_id=APP_ID,
    )
    def advance_batch(_batch_id: str) -> dict:
        return review_inbox_worker.run(
            conn, "isolated-profile", BOT_ID, APP_ID, lock_path
        )

    def tick() -> dict:
        return review_auto_schedule.run_tick(
            conn,
            ReviewGateway(adapter),
            today=now.date(),
            now=now,
            conversation_id=CONVERSATION_ID,
            owner_id=OWNER_ID,
            bot_open_id=BOT_ID,
            batch_id_factory=lambda: BATCH_ID,
            preflight=schedule_dws.preflight,
            sender=schedule_dws.send,
            history_reader=schedule_dws.history,
            advance_batch=advance_batch,
        )

    with (
        patch.object(review_dispatch_send, "validate_owner", return_value=OWNER_USER_ID),
        patch.object(review_dispatch_send, "dws", side_effect=cifra.dws),
        patch.object(review_dispatch_send, "_load_work_query_adapter", return_value=adapter),
        patch.object(review_auto_process, "_verified_send_context",
                     return_value=(OWNER_USER_ID, "cifra-robot-code")),
        patch.object(review_auto_process, "read_dws", side_effect=cifra.read_dws),
        patch.object(review_inbox_worker, "validate_owner", return_value=OWNER_USER_ID),
        patch.object(review_inbox_worker, "read_dws", side_effect=cifra.read_dws),
        patch.object(review_handoff, "validate_owner", return_value=OWNER_USER_ID),
        patch.object(review_handoff, "read_dws", side_effect=cifra.read_dws),
        patch.object(review_handoff, "_load_work_query_adapter", return_value=adapter),
        patch.object(review_resolve_create, "validate_owner", return_value=OWNER_USER_ID),
        patch.object(review_resolve_create, "read_dws", side_effect=cifra.read_dws),
        patch.object(review_resolve_create, "DwsClient",
                     side_effect=lambda **kwargs: FakeDwsClient(external, **kwargs)),
        patch.object(review_resolve_create, "WorkWriter", return_value=writer),
        patch.object(review_resolve_create, "_load_work_query_adapter", return_value=adapter),
        patch.object(dws_adapter, "DwsClient",
                     side_effect=lambda **kwargs: FakeDwsClient(external, **kwargs)),
        patch.object(review_schedule_dws, "read_dws", side_effect=cifra.read_dws),
    ):
        discovered_and_delivered = tick()
        assert discovered_and_delivered == {"status": "dispatch_delivered", "batch": BATCH_ID}
        assert SOURCE_TEXT in cifra.outbound[0]["text"]
        assert conn.execute("SELECT status FROM dispatch WHERE id=?", (BATCH_ID,)).fetchone() == (
            "delivered",
        )
        assert conn.execute("SELECT state FROM batch WHERE id=?", (BATCH_ID,)).fetchone() == (
            "open",
        )

        waiting = tick()
        assert waiting["status"] == "open_batch_advanced"
        assert waiting["result"]["status"] == "waiting"
        assert sum(message["text"].startswith(f"复核编号：{BATCH_ID}")
                   for message in cifra.outbound) == 1

        cifra.add_owner_reply("创建待办提醒我，标注测试用")
        completed = tick()
        assert completed["status"] == "open_batch_advanced"
        assert completed["result"]["status"] == "complete"
        assert completed["result"]["poll_status"] == "recorded"

        replay = tick()
        assert replay["status"] == "idle"

    assert len(cifra.outbound) == 3
    assert cifra.outbound[0]["text"].startswith(f"复核编号：{BATCH_ID}")
    assert [message["text"] for message in cifra.outbound[1:]] == ["收到", "处理已完成"]
    assert conn.execute(
        "SELECT status FROM review_auto_terminal WHERE batch_id=?", (BATCH_ID,)
    ).fetchone() == ("complete",)
    reply_content, reply_decisions = conn.execute(
        "SELECT content,decisions FROM reply WHERE batch_id=?", (BATCH_ID,)
    ).fetchone()
    assert reply_content == "创建待办提醒我，标注测试用"
    assert json.loads(reply_decisions) == {"1": "create"}
    assert conn.execute(
        "SELECT decision,status,external_task_id FROM resolution WHERE batch_id=? AND ordinal=1",
        (BATCH_ID,),
    ).fetchone() == ("create", "applied", "todo-e2e-701")
    audit = json.loads(conn.execute(
        "SELECT request_json FROM resolution WHERE batch_id=? AND ordinal=1", (BATCH_ID,)
    ).fetchone()[0])
    assert audit["title"] == SOURCE_TEXT
    assert audit["due_iso"] == expected_due_iso
    assert audit["source_message_id"] == "source-message"
    assert external["create_calls"] == 1
    assert external["create_preview"] == {
        "title": SOURCE_TEXT,
        "executor_ids": [OWNER_USER_ID],
        "due_iso": expected_due_iso,
        "priority": 20,
    }
    assert external["create_plan"].get("due_iso") == expected_due_iso
    assert external["todo"]["dueTime"] == expected_due_ms
    assert len(external["comments"]) == 1
    assert external["comments"][0][0] == "todo-e2e-701"
    expected_comment = (
        "来源群：设备巡检群\n"
        "发送人：班组长\n"
        f"消息时间：{source_time}\n"
        "messageId：source-message\n"
        f"原文：\n{SOURCE_TEXT}"
    )
    assert external["comments"] == [("todo-e2e-701", expected_comment)]
    assert external["verified_comments"] == [("todo-e2e-701", expected_comment)]
    assert audit["source_comment_verified"] is True
    assert adapter.source_reads
    assert set(adapter.source_reads) == {("source-conversation", "source-message")}
    assert adapter.action["status"] == "SUCCESS"
    assert adapter.action["effective_external_task_id"] == "todo-e2e-701"
    conn.close()


def test_review_create_blocks_wrong_or_unrelated_deadline_before_writer(tmp_path):
    conn = connect(str(tmp_path / "review-deadline.sqlite3"))
    source_time = "2026-11-05 08:00:00"
    register_batch(conn, BATCH_ID, CONVERSATION_ID, OWNER_ID, "prompt-message",
                   "2026-11-05 08:30:00", {1: [ACTION_ID]})
    ingest_reply(conn, BATCH_ID, {
        "messageId": "owner-reply-1",
        "conversationId": CONVERSATION_ID,
        "senderId": OWNER_ID,
        "createTime": "2026-11-05 08:31:00",
        "text": "创建待办提醒我，标注测试用",
    })

    cases = (
        (SOURCE_TEXT, "2026-11-06T14:00:00+08:00"),
        ("明天更新配电柜巡检记录，15:00前提交", "2026-11-06T15:00:00+08:00"),
    )
    with patch.object(review_resolve_create, "resolve") as resolve:
        for source_text, guessed_due in cases:
            source = {
                "action_id": ACTION_ID,
                "status": "REVIEW",
                "conversation_id": "source-conversation",
                "message_id": "source-message",
                "updated_at": source_time,
                "business_key": "source-business-key",
                "source": {
                    "group_name": "设备巡检群",
                    "sender_name": "班组长",
                    "message_time": source_time,
                    "text": source_text,
                },
            }
            item = {
                "ordinal": 1,
                "user_decision": "create",
                "handoff_state": "ready_for_existing_worker_review",
                "sources": [source],
            }
            plan = {
                "owner_confirmed": True,
                "batch_id": BATCH_ID,
                "ordinal": 1,
                "action_id": ACTION_ID,
                "reply_message_id": "owner-reply-1",
                "title": source_text,
                "duplicate_query": source_text,
                "due_iso": guessed_due,
            }

            result = review_auto_process._apply_create(
                conn, BATCH_ID, item, "isolated-profile", BOT_ID,
                str(tmp_path / "review-resolution.lock"), lambda _item: plan,
                "owner-reply-1",
            )

            assert result == {
                "status": "pending",
                "ordinal": 1,
                "reason": "create_deadline_not_source_bound",
            }

    resolve.assert_not_called()
    assert conn.execute(
        "SELECT 1 FROM resolution WHERE batch_id=?", (BATCH_ID,)
    ).fetchone() is None
    conn.close()
