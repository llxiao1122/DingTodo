import json
import sys
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dingtalk_reader import (  # noqa: E402
    is_complete_ledger,
    normalize_message,
    start_from_cursor,
    DingTalkReader,
)
from event_worker import normalize_event  # noqa: E402


class ReaderTests(unittest.TestCase):
    def test_complete_ledger_requires_all_safety_fields(self):
        good = {"complete": True, "hasMore": False, "failedCount": 0, "partial": False, "truncated": False, "failures": []}
        self.assertTrue(is_complete_ledger(good))
        bad = {**good, "hasMore": True}
        self.assertFalse(is_complete_ledger(bad))

    def test_normalize_message_preserves_source_identity(self):
        row = normalize_message(
            {
                "messageId": "m1",
                "conversationId": "c1",
                "senderId": "u1",
                "sender": "李林骁",
                "createTime": "2026-09-21T09:00:00+08:00",
                "text": "请完成盘点",
            },
            group_name="铁炉西工班群",
        )
        self.assertEqual(row["message_id"], "m1")
        self.assertEqual(row["conversation_id"], "c1")
        self.assertEqual(row["group"], "铁炉西工班群")
        self.assertEqual(row["sender_id"], "u1")

    def test_normalize_message_accepts_official_open_dingtalk_sender_id_aliases(self):
        for key in ("senderOpenDingTalkId", "sender_open_dingtalk_id"):
            row = normalize_message(
                {
                    "messageId": "m1",
                    "conversationId": "c1",
                    key: "sender-open-id",
                    "createTime": "2026-09-21T09:00:00+08:00",
                    "text": "source",
                },
                group_name="g",
            )
            self.assertEqual(row["sender_id"], "sender-open-id")

    def test_normalize_message_preserves_explicit_source_relation_ids(self):
        row = normalize_message(
            {
                "messageId": "file-message",
                "conversationId": "group-1",
                "createTime": "2026-09-21T09:00:00+08:00",
                "text": "[文件]source.docx",
                "replyToMessageId": "notice-message",
                "quotedMessageId": "quoted-message",
                "relatedMessageId": "related-message",
            },
            group_name="g",
        )
        self.assertEqual(row["reply_to_message_id"], "notice-message")
        self.assertEqual(row["quoted_message_id"], "quoted-message")
        self.assertEqual(row["related_message_id"], "related-message")

    def test_complete_reader_page_attests_ascending_source_order(self):
        response = {
            "complete": True,
            "hasMore": False,
            "failedCount": 0,
            "messages": [{
                "messageId": "m1", "conversationId": "c1", "createTime": "2026-09-21 09:00:00",
                "text": "source",
            }],
        }
        completed = SimpleNamespace(returncode=0, stdout=json.dumps(response), stderr="")
        with patch("dingtalk_reader.subprocess.run", return_value=completed) as run:
            result = DingTalkReader().read_group(
                conversation_id="c1",
                group_name="g",
                last_msg_time=None,
                end=datetime(2026, 9, 21, 10, 0, tzinfo=timezone(timedelta(hours=8))),
            )
        self.assertEqual(result["source_order_provenance"], "complete_dws_chronological_page")
        page_call = next(call.args[0] for call in run.call_args_list if "+chat-messages" in call.args[0])
        self.assertIn("--order", page_call)
        self.assertEqual(page_call[page_call.index("--order") + 1], "asc")
        self.assertNotIn("--no-threads", page_call)
        self.assertNotIn("--with-threads", page_call)
        mget_call = next(call.args[0] for call in run.call_args_list if "+messages-mget" in call.args[0])
        self.assertIn("--no-threads", mget_call)

    def test_reader_does_not_attest_a_page_with_decreasing_timestamps(self):
        response = {
            "complete": True,
            "hasMore": False,
            "failedCount": 0,
            "messages": [
                {"messageId": "later", "conversationId": "c1", "createTime": "2026-09-21 09:01:00", "text": "a"},
                {"messageId": "earlier", "conversationId": "c1", "createTime": "2026-09-21 09:00:00", "text": "b"},
            ],
        }
        completed = SimpleNamespace(returncode=0, stdout=json.dumps(response), stderr="")
        with patch("dingtalk_reader.subprocess.run", return_value=completed):
            result = DingTalkReader().read_group(
                conversation_id="c1",
                group_name="g",
                last_msg_time=None,
                end=datetime(2026, 9, 21, 10, 0, tzinfo=timezone(timedelta(hours=8))),
            )
        self.assertEqual(result["source_order_provenance"], "unverified_input_order")

    def test_cursor_start_uses_one_minute_overlap(self):
        start = start_from_cursor("2026-09-21T09:00:00", overlap_seconds=60)
        self.assertEqual(start.isoformat(), "2026-09-21T08:59:00+08:00")

    def test_read_group_mgets_in_bounded_batches_and_preserves_child_source_identity(self):
        root = {
            "messageId": "synthetic-root", "conversationId": "synthetic-group",
            "createTime": "2026-09-21 09:00:00", "text": "通知：完成资产盘点权限确认。",
            "openConvThreadId": "synthetic-thread",
        }
        child = {
            "messageId": "synthetic-child", "conversationId": "synthetic-group",
            "createTime": "2026-09-21 09:01:00", "text": "我负责标签核验。",
            "replyToMessageId": "synthetic-root",
        }
        topic_reply = {
            "messageId": "synthetic-topic-reply", "conversationId": "synthetic-group",
            "createTime": "2026-09-21 09:02:00", "text": "线上流程需单独确认。",
            "replyToMessageId": "synthetic-root",
        }
        page = {"complete": True, "hasMore": False, "failedCount": 0, "messages": [root, child]}
        mget = {"ok": True, "outcome": "success", "data": {
            "complete": True, "hasMore": False, "failedCount": 0, "messages": [root, child],
        }}
        thread = {"ok": True, "outcome": "success", "data": {
            "complete": True, "hasMore": False, "failedCount": 0, "messages": [topic_reply],
        }}

        def completed(payload):
            return SimpleNamespace(returncode=0, stdout=json.dumps(payload), stderr="")

        def run(command, **_kwargs):
            if "+chat-messages" in command:
                return completed(page)
            if "+messages-mget" in command:
                return completed(mget)
            if "+thread-replies" in command:
                return completed(thread)
            raise AssertionError("unexpected command")

        with patch("dingtalk_reader.subprocess.run", side_effect=run) as subprocess_run:
            result = DingTalkReader().read_group(
                conversation_id="synthetic-group", group_name="合成工班群", last_msg_time=None,
                end=datetime(2026, 9, 21, 10, 0, tzinfo=timezone(timedelta(hours=8))),
            )

        self.assertTrue(result["complete"])
        by_id = {row["message_id"]: row for row in result["messages"]}
        self.assertEqual(set(by_id), {"synthetic-root", "synthetic-child", "synthetic-topic-reply"})
        self.assertEqual(by_id["synthetic-child"]["conversation_id"], "synthetic-group")
        self.assertEqual(by_id["synthetic-child"]["reply_to_message_id"], "synthetic-root")
        self.assertEqual(by_id["synthetic-child"]["related_contexts"][0]["message_id"], "synthetic-root")
        self.assertEqual(by_id["synthetic-child"]["related_contexts"][0]["text"], root["text"])
        normalized_child = normalize_event(by_id["synthetic-child"])
        self.assertEqual(normalized_child["message_id"], "synthetic-child")
        self.assertEqual(normalized_child["conversation_id"], "synthetic-group")
        self.assertEqual(by_id["synthetic-topic-reply"]["message_id"], "synthetic-topic-reply")
        mget_call = next(call.args[0] for call in subprocess_run.call_args_list if "+messages-mget" in call.args[0])
        self.assertIn("--no-reactions", mget_call)
        self.assertIn("--no-threads", mget_call)
        thread_call = next(call.args[0] for call in subprocess_run.call_args_list if "+thread-replies" in call.args[0])
        self.assertIn("--group", thread_call)
        self.assertIn("synthetic-group", thread_call)
        self.assertIn("--thread-id", thread_call)

    def test_partial_topic_reply_read_is_scoped_to_root_source_and_keeps_group_page(self):
        page = {
            "complete": True, "hasMore": False, "failedCount": 0,
            "messages": [{
                "messageId": "synthetic-root", "conversationId": "synthetic-group",
                "createTime": "2026-09-21 09:00:00", "text": "话题根消息",
                "openConvThreadId": "synthetic-thread",
            }],
        }
        mget = {"ok": True, "outcome": "success", "data": {
            "complete": True, "hasMore": False, "failedCount": 0, "messages": page["messages"],
        }}
        partial_thread = {
            "ok": True, "outcome": "success", "data": {
                "complete": False, "hasMore": True, "failedCount": 0,
                "messages": [{
                    "messageId": "synthetic-reply", "conversationId": "synthetic-group",
                    "createTime": "2026-09-21 09:01:00", "text": "部分回复",
                }],
            },
        }

        def completed(payload):
            return SimpleNamespace(returncode=0, stdout=json.dumps(payload), stderr="")

        def run(command, **_kwargs):
            if "+chat-messages" in command:
                return completed(page)
            if "+messages-mget" in command:
                return completed(mget)
            if "+thread-replies" in command:
                return completed(partial_thread)
            raise AssertionError("unexpected command")

        with patch("dingtalk_reader.subprocess.run", side_effect=run):
            result = DingTalkReader().read_group(
                conversation_id="synthetic-group", group_name="合成工班群", last_msg_time=None,
                end=datetime(2026, 9, 21, 10, 0, tzinfo=timezone(timedelta(hours=8))),
            )

        self.assertTrue(result["complete"])
        self.assertEqual(len(result["messages"]), 2)
        self.assertEqual(result["context_status"], "partial")
        by_id = {row["message_id"]: row for row in result["messages"]}
        self.assertEqual(set(by_id), {"synthetic-root", "synthetic-reply"})
        self.assertEqual(by_id["synthetic-root"]["thread_context_status"], "partial")
        self.assertEqual(by_id["synthetic-reply"]["context_status"], "complete")
        self.assertEqual(by_id["synthetic-reply"]["thread_context_status"], "partial")

    def test_mget_splits_more_than_fifty_sources_and_requires_complete_identity_match(self):
        page_rows = [{
            "messageId": f"synthetic-{index}", "conversationId": "synthetic-group",
            "createTime": f"2026-09-21 09:{index // 60:02d}:{index % 60:02d}",
            "text": f"合成正文{index}",
        } for index in range(51)]

        def envelope(rows):
            return {"ok": True, "outcome": "success", "data": {
                "complete": True, "hasMore": False, "failedCount": 0, "messages": rows,
            }}

        def completed(payload):
            return SimpleNamespace(returncode=0, stdout=json.dumps(payload), stderr="")

        def run(command, **_kwargs):
            if "+chat-messages" in command:
                return completed({
                    "complete": True, "hasMore": False, "failedCount": 0,
                    "messages": page_rows,
                })
            if "+messages-mget" in command:
                start = command.index("--msg-ids") + 1
                ids = command[start].split(",")
                rows = [page_rows[int(value.rsplit("-", 1)[1])] for value in ids]
                return completed(envelope(rows))
            raise AssertionError("unexpected command")

        with patch("dingtalk_reader.subprocess.run", side_effect=run) as subprocess_run:
            result = DingTalkReader().read_group(
                conversation_id="synthetic-group", group_name="合成工班群", last_msg_time=None,
                end=datetime(2026, 9, 21, 10, 0, tzinfo=timezone(timedelta(hours=8))),
            )

        self.assertTrue(result["complete"])
        self.assertEqual(len(result["messages"]), 51)
        mget_calls = [call.args[0] for call in subprocess_run.call_args_list if "+messages-mget" in call.args[0]]
        self.assertEqual(len(mget_calls), 2)
        self.assertEqual([
            len(call[call.index("--msg-ids") + 1].split(",")) for call in mget_calls
        ], [50, 1])

    def test_mget_packs_two_message_ids_as_one_cli_array_argument(self):
        rows = [
            {"messageId": message_id, "conversationId": "synthetic-group",
             "createTime": f"2026-09-21 09:00:0{index}", "text": f"正文{index}"}
            for index, message_id in enumerate(("synthetic-one", "synthetic-two"))
        ]
        response = {"complete": True, "hasMore": False, "failedCount": 0, "messages": rows}
        completed = SimpleNamespace(returncode=0, stdout=json.dumps(response), stderr="")

        with patch("dingtalk_reader.subprocess.run", return_value=completed) as subprocess_run:
            result = DingTalkReader()._mget(
                ["synthetic-one", "synthetic-two"], conversation_id="synthetic-group",
            )

        self.assertEqual([row["messageId"] for row in result], ["synthetic-one", "synthetic-two"])
        mget_call = subprocess_run.call_args.args[0]
        msg_ids_start = mget_call.index("--msg-ids") + 1
        msg_ids_end = mget_call.index("--no-reactions")
        self.assertEqual(mget_call[msg_ids_start:msg_ids_end], ["synthetic-one,synthetic-two"])

    def test_missing_same_group_parent_context_preserves_source_for_review(self):
        child = {
            "messageId": "synthetic-child", "conversationId": "synthetic-group",
            "createTime": "2026-09-21 09:01:00", "text": "附件见上条通知",
            "replyToMessageId": "synthetic-parent-missing",
        }

        def envelope(rows):
            return {"ok": True, "outcome": "success", "data": {
                "complete": True, "hasMore": False, "failedCount": 0, "messages": rows,
            }}

        def completed(payload):
            return SimpleNamespace(returncode=0, stdout=json.dumps(payload), stderr="")

        def run(command, **_kwargs):
            if "+chat-messages" in command:
                return completed({
                    "complete": True, "hasMore": False, "failedCount": 0, "messages": [child],
                })
            if "+messages-mget" in command:
                start = command.index("--msg-ids") + 1
                ids = command[start].split(",")
                rows = [child] if ids == ["synthetic-child"] else []
                return completed(envelope(rows))
            raise AssertionError("unexpected command")

        with patch("dingtalk_reader.subprocess.run", side_effect=run):
            result = DingTalkReader().read_group(
                conversation_id="synthetic-group", group_name="合成工班群", last_msg_time=None,
                end=datetime(2026, 9, 21, 10, 0, tzinfo=timezone(timedelta(hours=8))),
            )

        self.assertTrue(result["complete"])
        self.assertEqual(len(result["messages"]), 1)
        self.assertEqual(result["context_status"], "partial")
        self.assertEqual(result["messages"][0]["message_id"], "synthetic-child")
        self.assertEqual(result["messages"][0]["context_status"], "partial")
        self.assertEqual(result["messages"][0]["context_missing_message_ids"], ["synthetic-parent-missing"])

    def test_unrelated_topic_failure_does_not_drop_other_source_messages(self):
        topic = {
            "messageId": "synthetic-topic-root", "conversationId": "synthetic-group",
            "createTime": "2026-09-21 09:00:00", "text": "普通话题",
            "openConvThreadId": "synthetic-unrelated-thread",
        }
        regular = {
            "messageId": "synthetic-regular", "conversationId": "synthetic-group",
            "createTime": "2026-09-21 09:01:00", "text": "请完成核对",
        }

        def complete(rows):
            return {"ok": True, "outcome": "success", "data": {
                "complete": True, "hasMore": False, "failedCount": 0, "messages": rows,
            }}

        def completed(payload):
            return SimpleNamespace(returncode=0, stdout=json.dumps(payload), stderr="")

        def run(command, **_kwargs):
            if "+chat-messages" in command:
                return completed({"complete": True, "hasMore": False, "failedCount": 0,
                                 "messages": [topic, regular]})
            if "+messages-mget" in command:
                return completed(complete([topic, regular]))
            if "+thread-replies" in command:
                return completed({"ok": True, "outcome": "success", "data": {
                    "complete": False, "hasMore": True, "failedCount": 0, "messages": [],
                }})
            raise AssertionError("unexpected command")

        with patch("dingtalk_reader.subprocess.run", side_effect=run):
            result = DingTalkReader().read_group(
                conversation_id="synthetic-group", group_name="合成工班群", last_msg_time=None,
                end=datetime(2026, 9, 21, 10, 0, tzinfo=timezone(timedelta(hours=8))),
            )

        assert result["complete"] is True
        rows = {row["message_id"]: row for row in result["messages"]}
        assert set(rows) == {"synthetic-topic-root", "synthetic-regular"}
        assert rows["synthetic-topic-root"]["thread_context_status"] == "partial"
        assert rows["synthetic-regular"]["context_status"] == "complete"


if __name__ == "__main__":
    unittest.main()
