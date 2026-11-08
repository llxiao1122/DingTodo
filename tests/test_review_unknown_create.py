import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from review_unknown_create import (
    UnknownCreateEvidenceError,
    load_unknown_create_evidence,
    reconcile_unknown_create,
)
from routing import build_source_comment


SOURCE_TEXT = "请核对危险废物清单。\nmessageId：forged-line"
SOURCE_MESSAGE_ID = "source-message-73"
TITLE = "危险废物清单核对"
EXECUTOR_ID = "executor-1"


def source_event():
    return {
        "group_name": "合成工班群",
        "sender_name": "合成发送人",
        "message_time": "2026-11-05 09:30:00",
        "text": SOURCE_TEXT,
        "text_truncated": False,
        "text_is_untrusted_source_content": True,
    }


def expected_comment():
    source = source_event()
    return build_source_comment({
        "group": source["group_name"],
        "sender": source["sender_name"],
        "create_time": source["message_time"],
        "message_id": SOURCE_MESSAGE_ID,
        "text": source["text"],
    })


class FakeDws:
    def __init__(self, rows=None, details=None, comments=None, error=None):
        self.rows = rows if rows is not None else [{"taskId": "task-1"}]
        self.details = details if details is not None else {
            "task-1": {
                "taskId": "task-1",
                "subject": TITLE,
                "executorInfos": [{"userId": EXECUTOR_ID}],
            }
        }
        self.comments = comments if comments is not None else {
            "task-1": [{"content": expected_comment()}]
        }
        self.error = error
        self.search_queries = []
        self.detail_reads = []
        self.comment_reads = []
        self.write_calls = 0

    def search_todos(self, query):
        self.search_queries.append(query)
        if self.error == "search":
            raise RuntimeError("partial search response")
        return list(self.rows)

    def get_todo(self, task_id):
        self.detail_reads.append(task_id)
        if task_id == "task-incomplete":
            raise RuntimeError("detail read failed")
        return dict(self.details[task_id])

    def list_task_comments(self, task_id):
        self.comment_reads.append(task_id)
        if self.error == "comments":
            raise RuntimeError("comment pagination incomplete")
        return list(self.comments[task_id])

    def create_todo(self, *_args, **_kwargs):
        self.write_calls += 1
        raise AssertionError("read-only reconciliation must not create todos")

    def add_source_comment(self, *_args, **_kwargs):
        self.write_calls += 1
        raise AssertionError("read-only reconciliation must not write comments")


class ReviewUnknownCreateTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.database_path = Path(self.temporary_directory.name) / "review.sqlite"
        self.create_database()

    def create_database(self, *, create_attempted=True, frozen_source=None, dispatch_status="delivered"):
        audit = {
            "claim": {"action_id": 73, "message_id": SOURCE_MESSAGE_ID},
            "title": TITLE,
            "executor_user_id": EXECUTOR_ID,
            "source_message_id": SOURCE_MESSAGE_ID,
            "create_attempted": create_attempted,
        }
        source = frozen_source if frozen_source is not None else source_event()
        dispatch_items = [{"action_id": "73", "source": source}]
        connection = sqlite3.connect(self.database_path)
        connection.executescript(
            "DROP TABLE IF EXISTS resolution;"
            "DROP TABLE IF EXISTS dispatch;"
            "CREATE TABLE resolution (batch_id TEXT, ordinal INTEGER, action_id TEXT, "
            "decision TEXT, request_json TEXT, status TEXT, external_task_id TEXT);"
            "CREATE TABLE dispatch (id TEXT, items TEXT, status TEXT);"
        )
        connection.execute(
            "INSERT INTO resolution VALUES (?,?,?,?,?,?,?)",
            ("batch-1", 1, "73", "create", json.dumps(audit), "applying", None),
        )
        connection.execute(
            "INSERT INTO dispatch VALUES (?,?,?)",
            ("batch-1", json.dumps(dispatch_items), dispatch_status),
        )
        connection.commit()
        connection.close()

    def load_evidence(self):
        return load_unknown_create_evidence(self.database_path, "batch-1", 1)

    def test_unique_exact_candidate_is_returned_without_source_body_or_writes(self):
        before = self.database_path.read_bytes()
        dws = FakeDws()

        result = reconcile_unknown_create(dws, self.load_evidence())

        self.assertEqual(result["status"], "unique_candidate_for_manual_verification")
        self.assertEqual(result["candidate"]["task_id"], "task-1")
        self.assertEqual(result["evidence"]["full_source_comment_exact"], True)
        self.assertNotIn(SOURCE_TEXT, json.dumps(result, ensure_ascii=False))
        self.assertNotIn(expected_comment(), json.dumps(result, ensure_ascii=False))
        self.assertEqual(dws.search_queries, [TITLE])
        self.assertEqual(dws.write_calls, 0)
        self.assertEqual(self.database_path.read_bytes(), before)
        self.assertFalse(Path(str(self.database_path) + "-wal").exists())

    def test_message_id_line_alone_is_not_a_source_comment_match(self):
        dws = FakeDws(comments={"task-1": [{"content": f"messageId：{SOURCE_MESSAGE_ID}\nother text"}]})

        result = reconcile_unknown_create(dws, self.load_evidence())

        self.assertEqual(result["status"], "unconfirmed")
        self.assertEqual(result["reason"], "no_unique_exact_match")
        self.assertEqual(dws.write_calls, 0)

    def test_incomplete_search_or_comment_read_never_confirms_a_candidate(self):
        for error in ("search", "comments"):
            with self.subTest(error=error):
                result = reconcile_unknown_create(FakeDws(error=error), self.load_evidence())
                self.assertEqual(result["status"], "unconfirmed")
                self.assertEqual(result["mutations_performed"], 0)

    def test_incomplete_other_result_blocks_uniqueness(self):
        dws = FakeDws(
            rows=[{"taskId": "task-1"}, {"taskId": "task-incomplete"}],
        )

        result = reconcile_unknown_create(dws, self.load_evidence())

        self.assertEqual(result["status"], "unconfirmed")
        self.assertEqual(result["reason"], "candidate_readback_incomplete")
        self.assertIn("task-incomplete", result["incomplete_task_ids"])

    def test_multiple_exact_candidates_are_not_called_unique(self):
        dws = FakeDws(
            rows=[{"taskId": "task-1"}, {"taskId": "task-2"}],
            details={
                "task-1": {"taskId": "task-1", "subject": TITLE, "executorInfos": [{"userId": EXECUTOR_ID}]},
                "task-2": {"taskId": "task-2", "subject": TITLE, "executorInfos": [{"userId": EXECUTOR_ID}]},
            },
            comments={
                "task-1": [{"content": expected_comment()}],
                "task-2": [{"content": expected_comment()}],
            },
        )

        result = reconcile_unknown_create(dws, self.load_evidence())

        self.assertEqual(result["status"], "unconfirmed")
        self.assertEqual(result["reason"], "multiple_exact_matches")

    def test_executor_mismatch_is_not_a_candidate(self):
        dws = FakeDws(details={
            "task-1": {"taskId": "task-1", "subject": TITLE, "executorInfos": [{"userId": "other-user"}]}
        })

        result = reconcile_unknown_create(dws, self.load_evidence())

        self.assertEqual(result["status"], "unconfirmed")
        self.assertEqual(result["reason"], "no_unique_exact_match")
        self.assertEqual(dws.comment_reads, [])

    def test_missing_or_truncated_frozen_source_is_explicitly_unconfirmed(self):
        for source in (
            {**source_event(), "text_truncated": True},
            {key: value for key, value in source_event().items() if key != "text"},
        ):
            with self.subTest(source=source):
                self.create_database(frozen_source=source)
                with self.assertRaises(UnknownCreateEvidenceError) as error:
                    self.load_evidence()
                self.assertEqual(error.exception.reason, "frozen_source_incomplete")

    def test_missing_create_attempt_evidence_does_not_query_dws(self):
        self.create_database(create_attempted=False)
        dws = FakeDws()

        with self.assertRaises(UnknownCreateEvidenceError) as error:
            self.load_evidence()

        self.assertEqual(error.exception.reason, "create_attempt_not_verified")
        self.assertEqual(dws.search_queries, [])

    def test_unverified_dispatch_is_not_used_to_reconstruct_comment(self):
        self.create_database(dispatch_status="prepared")

        with self.assertRaises(UnknownCreateEvidenceError) as error:
            self.load_evidence()

        self.assertEqual(error.exception.reason, "frozen_dispatch_not_verified")


if __name__ == "__main__":
    unittest.main()
