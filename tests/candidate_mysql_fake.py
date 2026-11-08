from __future__ import annotations

import importlib.util
import re
from pathlib import Path
import sys
import threading
import types
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
WRITER_ROOT = ROOT / "writer_candidate" / "polardb_work_writer"


class IntegrityError(Exception):
    pass


class OperationalError(Exception):
    pass


def load_candidate_writer():
    """Import only copied source files; do not import or read the host DB configuration."""
    names = (
        "workflows", "workflows.polardb_work_writer",
        "workflows.polardb_work_writer.policy", "workflows.polardb_work_writer.db",
        "pymysql",
    )
    old = {name: sys.modules.get(name) for name in names}
    workflows = types.ModuleType("workflows")
    workflows.__path__ = []
    package = types.ModuleType("workflows.polardb_work_writer")
    package.__path__ = []
    pymysql = types.ModuleType("pymysql")
    pymysql.IntegrityError = IntegrityError
    pymysql.OperationalError = OperationalError
    sys.modules["workflows"] = workflows
    sys.modules["workflows.polardb_work_writer"] = package
    sys.modules["pymysql"] = pymysql
    try:
        policy_spec = importlib.util.spec_from_file_location(
            "workflows.polardb_work_writer.policy", WRITER_ROOT / "policy.py"
        )
        policy = importlib.util.module_from_spec(policy_spec)
        assert policy_spec and policy_spec.loader
        sys.modules["workflows.polardb_work_writer.policy"] = policy
        policy_spec.loader.exec_module(policy)
        db_module = types.ModuleType("workflows.polardb_work_writer.db")
        db_module.connect_writer = lambda: None
        sys.modules["workflows.polardb_work_writer.db"] = db_module
        writer_spec = importlib.util.spec_from_file_location(
            "phase3_candidate_writer", WRITER_ROOT / "writer.py"
        )
        writer_module = importlib.util.module_from_spec(writer_spec)
        assert writer_spec and writer_spec.loader
        writer_spec.loader.exec_module(writer_module)
        server_spec = importlib.util.spec_from_file_location(
            "phase3_candidate_writer_server", WRITER_ROOT / "mcp_server.py"
        )
        server = importlib.util.module_from_spec(server_spec)
        # The server imports package modules through workflows.*; its bounded schema is inspected separately.
        return writer_module.WorkWriter, policy, pymysql
    finally:
        for name, value in old.items():
            if value is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = value


class FakeMySQLDatabase:
    """Minimal transactional store that enforces the real unique/FK semantics used here."""

    def __init__(self, integrity_error: type[Exception] = IntegrityError):
        self.lock = threading.RLock()
        self.integrity_error = integrity_error
        self.messages: dict[tuple[str, str], dict[str, Any]] = {}
        self.actions: dict[int, dict[str, Any]] = {}
        self.scan_states: dict[str, dict[str, Any]] = {}
        self.next_action_id = 1
        self.business_select_barrier: threading.Barrier | None = None
        self.business_barrier_remaining = 0
        self.schema_columns = {"parent_action_id", "rule_audit"}
        self.schema_column_types = {
            "parent_action_id": ("bigint unsigned", "bigint", "YES"),
            "rule_audit": ("json", "json", "YES"),
        }
        self.parent_foreign_key = True
        self.parent_index = True
        self.query_log: list[str] = []

    def connect(self):
        return FakeConnection(self)

    def _duplicate(self, message: str):
        raise self.integrity_error(1062, message)

    def _insert_action(self, row: dict[str, Any]) -> int:
        source = (row["conversation_id"], row["message_id"], row["action_key"])
        if any(
            (current["conversation_id"], current["message_id"], current["action_key"]) == source
            for current in self.actions.values()
        ):
            self._duplicate("uq_message_action_source")
        business_key = row.get("business_key")
        if business_key and any(current.get("business_key") == business_key for current in self.actions.values()):
            self._duplicate("uq_message_action_business_key")
        external_task_id = row.get("external_task_id")
        if external_task_id and any(current.get("external_task_id") == external_task_id for current in self.actions.values()):
            self._duplicate("uq_message_action_external_task_id")
        parent_action_id = row.get("parent_action_id")
        duplicate_of = row.get("duplicate_of_action_id")
        if parent_action_id not in (None, "") and (
            parent_action_id not in self.actions or parent_action_id == self.next_action_id
        ):
            self._duplicate("fk_message_action_parent_action_id")
        if duplicate_of not in (None, ""):
            canonical = self.actions.get(int(duplicate_of))
            if not canonical or canonical.get("duplicate_of_action_id") not in (None, ""):
                self._duplicate("fk_message_action_duplicate_of_action_id")
            if row.get("business_key") is not None:
                self._duplicate("uq_message_action_business_key")
        action_id = self.next_action_id
        self.next_action_id += 1
        self.actions[action_id] = {
            "id": action_id,
            "conversation_id": row["conversation_id"],
            "message_id": row["message_id"],
            "action_key": row["action_key"],
            "business_key": row.get("business_key"),
            "status": row["status"],
            "external_task_id": row.get("external_task_id"),
            "todo_id": row.get("todo_id"),
            "duplicate_of_action_id": row.get("duplicate_of_action_id"),
            "parent_action_id": row.get("parent_action_id"),
            "attempt_count": row.get("attempt_count", 0) or 0,
            "last_error": row.get("last_error"),
            "rule_audit": row.get("rule_audit"),
        }
        return action_id


class FakeConnection:
    def __init__(self, database: FakeMySQLDatabase):
        self.database = database
        self.closed = False

    def cursor(self):
        return FakeCursor(self.database)

    def commit(self):
        return None

    def rollback(self):
        return None

    def close(self):
        self.closed = True


class FakeCursor:
    def __init__(self, database: FakeMySQLDatabase):
        self.database = database
        self.rowcount = 0
        self.lastrowid = 0
        self._rows: list[dict[str, Any]] = []
        self._index = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def _set(self, rows: list[dict[str, Any]], rowcount: int | None = None):
        self._rows = [dict(row) for row in rows]
        self._index = 0
        self.rowcount = len(rows) if rowcount is None else rowcount

    def execute(self, sql: str, params: tuple[Any, ...] | None = None):
        params = tuple(params or ())
        query = re.sub(r"\s+", " ", sql.strip()).lower()
        with self.database.lock:
            self.database.query_log.append(query)
        if "from information_schema.columns" in query:
            self._set([
                {
                    "COLUMN_NAME": value,
                    "COLUMN_TYPE": self.database.schema_column_types[value][0],
                    "DATA_TYPE": self.database.schema_column_types[value][1],
                    "IS_NULLABLE": self.database.schema_column_types[value][2],
                }
                for value in sorted(self.database.schema_columns)
            ])
            return self.rowcount
        if "from information_schema.statistics" in query:
            rows = []
            for name, fields, non_unique in (
                ("uq_message_action_source", ("conversation_id", "message_id", "action_key"), 0),
                ("uq_message_action_external_task_id", ("external_task_id",), 0),
                ("uq_message_action_business_key", ("business_key",), 0),
                ("idx_message_action_parent_action_id", ("parent_action_id",), 1),
            ):
                if name == "idx_message_action_parent_action_id" and not self.database.parent_index:
                    continue
                rows.extend({
                    "INDEX_NAME": name, "NON_UNIQUE": non_unique,
                    "SEQ_IN_INDEX": index, "COLUMN_NAME": field,
                } for index, field in enumerate(fields, start=1))
            self._set(rows)
            return self.rowcount
        if "from information_schema.key_column_usage" in query:
            self._set([
                {"REFERENCED_TABLE_NAME": "message_action", "REFERENCED_COLUMN_NAME": "id"}
            ] if self.database.parent_foreign_key else [])
            return self.rowcount
        if query.startswith("insert into message_inbox"):
            key = (str(params[0]), str(params[1]))
            with self.database.lock:
                if key in self.database.messages:
                    self._set([], rowcount=0)
                else:
                    self.database.messages[key] = {
                        "id": len(self.database.messages) + 1,
                        "conversation_id": params[0], "message_id": params[1],
                        "group_name": params[2], "sender_id": params[3], "sender_name": params[4],
                        "message_time": params[5], "text": params[6], "msg_type": params[7],
                        "resource_refs": params[8], "quoted_message": params[9],
                        "raw_payload": params[10], "source": params[11],
                    }
                    self._set([], rowcount=1)
            return self.rowcount
        if query.startswith("select") and "from message_inbox" in query:
            key = (str(params[0]), str(params[1]))
            with self.database.lock:
                row = self.database.messages.get(key)
                self._set([row] if row else [])
            return self.rowcount
        if query.startswith("select") and "from message_action" in query:
            if "where conversation_id=%s and message_id=%s and action_key=%s" in query:
                if self.database.business_select_barrier is not None:
                    # Only the business-key lookup has two parameters and no source tuple.
                    pass
                key = (str(params[0]), str(params[1]), str(params[2]))
                with self.database.lock:
                    matches = [row for row in self.database.actions.values() if
                               (row["conversation_id"], row["message_id"], row["action_key"]) == key]
                    self._set(matches[:1])
                return self.rowcount
            if "where business_key=%s" in query:
                with self.database.lock:
                    barrier = (
                        self.database.business_select_barrier
                        if self.database.business_barrier_remaining > 0 else None
                    )
                    if barrier is not None:
                        self.database.business_barrier_remaining -= 1
                if barrier is not None:
                    barrier.wait(timeout=5)
                key = params[0]
                with self.database.lock:
                    matches = [row for row in self.database.actions.values() if row.get("business_key") == key]
                    self._set(matches[:1])
                return self.rowcount
            if "where id=%s" in query:
                action_id = int(params[0])
                with self.database.lock:
                    row = self.database.actions.get(action_id)
                    self._set([row] if row else [])
                return self.rowcount
        if query.startswith("insert into message_action"):
            with self.database.lock:
                if "duplicate_of_action_id" in query:
                    row = {
                        "conversation_id": params[0], "message_id": params[1], "action_key": params[2],
                        "business_key": None, "status": params[3], "duplicate_of_action_id": int(params[4]),
                        "parent_action_id": None,
                    }
                elif "parent_action_id" in query:
                    row = {
                        "conversation_id": params[0], "message_id": params[1], "action_key": params[2],
                        "business_key": None, "status": params[3], "parent_action_id": int(params[4]),
                        "duplicate_of_action_id": None,
                    }
                else:
                    row = {
                        "conversation_id": params[0], "message_id": params[1], "action_key": params[2],
                        "business_key": params[3], "status": params[4],
                        "parent_action_id": None, "duplicate_of_action_id": None,
                    }
                self.lastrowid = self.database._insert_action(row)
                self._set([], rowcount=1)
            return self.rowcount
        if query.startswith("update message_action set status=%s, last_error=null"):
            status, action_id = params
            with self.database.lock:
                row = self.database.actions.get(int(action_id))
                if row:
                    row["status"] = status
                    row["last_error"] = None
                    self._set([], rowcount=1)
                else:
                    self._set([], rowcount=0)
            return self.rowcount
        if query.startswith("update message_action set status=%s, external_task_id=coalesce"):
            status, external_id, todo_id, duplicate_of, last_error, attempt_count, action_id = params
            with self.database.lock:
                row = self.database.actions.get(int(action_id))
                if not row:
                    self._set([], rowcount=0)
                    return self.rowcount
                if external_id:
                    for other_id, other in self.database.actions.items():
                        if other_id != int(action_id) and other.get("external_task_id") == external_id:
                            self.database._duplicate("uq_message_action_external_task_id")
                    row["external_task_id"] = external_id
                if todo_id is not None:
                    row["todo_id"] = todo_id
                if duplicate_of is not None:
                    row["duplicate_of_action_id"] = duplicate_of
                row["status"] = status
                row["last_error"] = last_error
                if attempt_count is not None:
                    row["attempt_count"] = attempt_count
                self._set([], rowcount=1)
            return self.rowcount
        if query.startswith("update message_action set rule_audit=%s"):
            encoded, action_id = params
            with self.database.lock:
                row = self.database.actions.get(int(action_id))
                if row:
                    row["rule_audit"] = encoded
                    self._set([], rowcount=1)
                else:
                    self._set([], rowcount=0)
            return self.rowcount
        if query.startswith("select") and "from scan_state" in query:
            group = params[0]
            with self.database.lock:
                row = self.database.scan_states.get(group)
                self._set([row] if row else [])
            return self.rowcount
        if query.startswith("update scan_state set"):
            conversation_id, last_time, last_id, group = params
            with self.database.lock:
                row = self.database.scan_states[group]
                row["conversation_id"] = conversation_id or row.get("conversation_id")
                row["last_msg_time"] = last_time
                row["last_msg_id"] = last_id
                self._set([], rowcount=1)
            return self.rowcount
        if query.startswith("insert into scan_state"):
            group, conversation_id, last_time, last_id = params
            with self.database.lock:
                self.database.scan_states[group] = {
                    "group_name": group, "conversation_id": conversation_id,
                    "last_msg_time": last_time, "last_msg_id": last_id,
                }
                self._set([], rowcount=1)
            return self.rowcount
        raise AssertionError(f"unhandled candidate SQL: {query}")

    def fetchone(self):
        if self._index >= len(self._rows):
            return None
        value = self._rows[self._index]
        self._index += 1
        return dict(value)

    def fetchall(self):
        rows = self._rows[self._index:]
        self._index = len(self._rows)
        return [dict(row) for row in rows]
