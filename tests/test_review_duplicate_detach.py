from __future__ import annotations

import copy
from datetime import datetime
import sys
import types
import unittest

sys.path.insert(0, "/root/.hermes/polardb_runtime")

# Keep this package's optional DB driver importable without installing or using it.
try:
    import pymysql  # noqa: F401
except ModuleNotFoundError:
    pymysql_stub = types.ModuleType("pymysql")
    pymysql_stub.IntegrityError = type("IntegrityError", (Exception,), {})
    pymysql_stub.OperationalError = type("OperationalError", (Exception,), {})
    sys.modules["pymysql"] = pymysql_stub

from workflows.polardb_work_writer.policy import InvalidWriteRequest, WRITE_OPERATIONS
from workflows.polardb_work_writer.mcp_server import TOOL_SCHEMA
from workflows.polardb_work_writer.writer import WorkWriter, WriteError


SNAPSHOT_FIELDS = (
    "action_id", "conversation_id", "message_id", "action_key", "status", "business_key",
    "external_task_id", "todo_id", "duplicate_of_action_id", "parent_action_id", "updated_at",
)


def row(action_id: int, *, duplicate_of: int | None, suffix: str, external_task_id=None, todo_id=None):
    return {
        "id": action_id,
        "conversation_id": f"synthetic-conversation-{suffix}",
        "message_id": f"synthetic-message-{suffix}",
        "action_key": "todo",
        "status": "REVIEW",
        "business_key": f"synthetic-business-{suffix}",
        "external_task_id": external_task_id,
        "todo_id": todo_id,
        "duplicate_of_action_id": duplicate_of,
        "parent_action_id": None,
        "updated_at": datetime.fromisoformat("2026-11-03T12:15:30.123456"),
    }


def snapshot(value):
    return {
        "action_id": value["id"],
        **{field: value[field] for field in SNAPSHOT_FIELDS if field != "action_id"},
        "updated_at": value["updated_at"].isoformat(),
    }


def make_request(rows, *, dry_run=False):
    return {
        "operation": "detach_review_duplicate_action",
        "dry_run": dry_run,
        "target_action_id": 2202,
        "expected_canonical_action_id": 2201,
        "expected_target": snapshot(rows[2202]),
        "expected_canonical": snapshot(rows[2201]),
    }


class SyntheticCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rowcount = 0
        self.result = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=()):
        compact = " ".join(sql.split()).lower()
        self.connection.statements.append((compact, tuple(params)))
        if compact.startswith("select ") and " from message_action where id=%s" in compact:
            action_id = int(params[0])
            self.connection.select_locks.append(action_id) if "for update" in compact else None
            self.result = copy.deepcopy(self.connection.working_rows.get(action_id))
            self.rowcount = 1 if self.result else 0
            return
        if compact.startswith("update message_action set duplicate_of_action_id=null"):
            (action_id, conversation_id, message_id, action_key, status, old_duplicate,
             business_key, updated_at) = params
            current = self.connection.working_rows.get(int(action_id))
            matches = bool(
                current
                and current["conversation_id"] == conversation_id
                and current["message_id"] == message_id
                and current["action_key"] == action_key
                and current["status"] == status == "REVIEW"
                and current["duplicate_of_action_id"] == old_duplicate
                and current["parent_action_id"] is None
                and current["external_task_id"] in (None, "")
                and current["todo_id"] is None
                and current["business_key"] == business_key
                and current["updated_at"] == updated_at
            )
            if matches:
                current["duplicate_of_action_id"] = None
            self.rowcount = 1 if matches else 0
            self.result = None
            return
        raise AssertionError(f"unexpected SQL: {sql}")

    def fetchone(self):
        return self.result


class SyntheticConnection:
    def __init__(self, rows):
        self.rows = rows
        self.working_rows = copy.deepcopy(rows)
        self.statements = []
        self.select_locks = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return SyntheticCursor(self)

    def commit(self):
        self.rows.clear()
        self.rows.update(copy.deepcopy(self.working_rows))
        self.commits += 1

    def rollback(self):
        self.working_rows = copy.deepcopy(self.rows)
        self.rollbacks += 1

    def close(self):
        self.closed = True


class DetachReviewDuplicateActionTests(unittest.TestCase):
    def setUp(self):
        self.rows = {
            2201: row(
                2201, duplicate_of=None, suffix="canonical",
                external_task_id="synthetic-canonical-task", todo_id=8201,
            ),
            2202: row(2202, duplicate_of=2201, suffix="target"),
        }
        self.connection = SyntheticConnection(self.rows)
        self.connect_calls = 0

        def connect():
            self.connect_calls += 1
            return self.connection

        self.writer = WorkWriter(connect_factory=connect)
        self.writer._schema_contract_cache = {"schema_supported": True}

    def test_exactly_one_target_pointer_is_detached(self):
        before = copy.deepcopy(self.rows)
        result = self.writer.execute(make_request(self.rows))

        self.assertTrue(result["committed"])
        self.assertEqual(self.connection.commits, 1)
        self.assertEqual(self.connection.rollbacks, 0)
        self.assertEqual(self.connection.select_locks, [2201, 2202])
        updates = [(sql, params) for sql, params in self.connection.statements if sql.startswith("update ")]
        self.assertEqual(len(updates), 1)
        update_sql, update_params = updates[0]
        self.assertEqual(
            update_sql.split(" where ")[0],
            "update message_action set duplicate_of_action_id=null, updated_at=updated_at",
        )
        for guard in (
            "id=%s", "conversation_id=%s", "message_id=%s", "action_key=%s", "status=%s",
            "duplicate_of_action_id=%s", "parent_action_id is null", "external_task_id",
            "todo_id", "business_key", "updated_at",
        ):
            self.assertIn(guard, update_sql)
        self.assertEqual(update_params[0], 2202)

        expected_target = copy.deepcopy(before[2202])
        expected_target["duplicate_of_action_id"] = None
        self.assertEqual(self.rows[2202], expected_target)
        self.assertEqual(self.rows[2201], before[2201])
        self.assertEqual(self.rows[2202]["status"], before[2202]["status"])
        self.assertEqual(self.rows[2202]["external_task_id"], before[2202]["external_task_id"])
        self.assertEqual(self.rows[2202]["todo_id"], before[2202]["todo_id"])
        self.assertEqual(self.rows[2202]["business_key"], before[2202]["business_key"])
        for field in ("conversation_id", "message_id", "action_key"):
            self.assertEqual(self.rows[2202][field], before[2202][field])

    def test_stale_snapshot_rejects_without_commit(self):
        stale = make_request(self.rows)
        self.connection.working_rows[2202]["updated_at"] = datetime.fromisoformat("2026-11-03T12:16:00")
        before_execute = copy.deepcopy(self.rows)

        with self.assertRaises(WriteError):
            self.writer.execute(stale)

        self.assertEqual(self.rows, before_execute)
        self.assertEqual(self.connection.commits, 0)
        self.assertEqual(self.connection.rollbacks, 1)
        self.assertFalse(any(sql.startswith("update ") for sql, _ in self.connection.statements))

    def test_target_task_or_todo_linkage_rejects_without_commit(self):
        for field, value in (("external_task_id", "synthetic-target-task"), ("todo_id", 8202)):
            with self.subTest(field=field):
                self.setUp()
                request = make_request(self.rows)
                self.connection.working_rows[2202][field] = value
                before_execute = copy.deepcopy(self.rows)

                with self.assertRaises(WriteError):
                    self.writer.execute(request)

                self.assertEqual(self.rows, before_execute)
                self.assertEqual(self.connection.commits, 0)
                self.assertEqual(self.connection.rollbacks, 1)
                self.assertFalse(any(sql.startswith("update ") for sql, _ in self.connection.statements))

    def test_wrong_canonical_relationship_is_rejected(self):
        request = make_request(self.rows)
        self.connection.working_rows[2202]["duplicate_of_action_id"] = 2299

        with self.assertRaises(WriteError):
            self.writer.execute(request)

        self.assertEqual(self.connect_calls, 1)
        self.assertEqual(self.connection.commits, 0)
        self.assertEqual(self.connection.rollbacks, 1)

        self.setUp()
        request = make_request(self.rows)
        request["expected_target"]["duplicate_of_action_id"] = 2299

        with self.assertRaises(InvalidWriteRequest):
            self.writer.execute(request)

        self.assertEqual(self.connect_calls, 0)
        self.assertEqual(self.connection.commits, 0)
        self.assertEqual(self.connection.rollbacks, 0)

    def test_operation_registration_and_strict_mcp_snapshot_schemas(self):
        self.assertIn("detach_review_duplicate_action", WRITE_OPERATIONS)
        self.assertIn("detach_review_duplicate_action", WorkWriter.REQUIRED_OPERATIONS)
        schema = TOOL_SCHEMA["inputSchema"]
        self.assertIn("detach_review_duplicate_action", schema["properties"]["operation"]["enum"])
        for field in ("expected_target", "expected_canonical"):
            nested = schema["properties"][field]
            self.assertFalse(nested["additionalProperties"])
            self.assertEqual(set(nested["properties"]), set(SNAPSHOT_FIELDS))
            self.assertEqual(set(nested["required"]), set(SNAPSHOT_FIELDS))

    def test_malformed_or_extra_snapshot_fields_are_rejected_before_connection(self):
        invalid_requests = []
        extra_top = make_request(self.rows)
        extra_top["unexpected"] = "synthetic"
        invalid_requests.append(extra_top)
        extra_nested = make_request(self.rows)
        extra_nested["expected_target"]["unexpected"] = "synthetic"
        invalid_requests.append(extra_nested)
        non_ascii = make_request(self.rows)
        non_ascii["expected_target"]["conversation_id"] = "synthetic-会话"
        invalid_requests.append(non_ascii)
        malformed_datetime = make_request(self.rows)
        malformed_datetime["expected_canonical"]["updated_at"] = "not-a-local-datetime"
        invalid_requests.append(malformed_datetime)
        date_only = make_request(self.rows)
        date_only["expected_target"]["updated_at"] = "2026-11-03"
        invalid_requests.append(date_only)
        mismatched_id = make_request(self.rows)
        mismatched_id["expected_canonical"]["action_id"] = 2299
        invalid_requests.append(mismatched_id)

        for request in invalid_requests:
            with self.subTest(request=request):
                with self.assertRaises(InvalidWriteRequest):
                    self.writer.execute(request)

        self.assertEqual(self.connect_calls, 0)
        self.assertEqual(self.connection.commits, 0)
        self.assertEqual(self.connection.rollbacks, 0)

    def test_dry_run_reads_back_then_rolls_back_detach(self):
        before = copy.deepcopy(self.rows)
        result = self.writer.execute(make_request(self.rows, dry_run=True))

        self.assertFalse(result["committed"])
        self.assertTrue(result["dry_run"])
        self.assertEqual(self.connection.commits, 0)
        self.assertEqual(self.connection.rollbacks, 1)
        self.assertEqual(
            sum(sql.startswith("update ") for sql, _ in self.connection.statements), 1,
        )
        self.assertEqual(self.rows, before)
        self.assertEqual(self.rows[2201], before[2201])
        self.assertEqual(self.rows[2202]["duplicate_of_action_id"], 2201)


if __name__ == "__main__":
    unittest.main()
