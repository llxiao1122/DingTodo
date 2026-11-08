"""Transactional, allowlisted writes for the agentic PolarDB work ledger."""

from __future__ import annotations

import json
from typing import Any, Callable

import pymysql

from workflows.polardb_work_writer.db import connect_writer
from workflows.polardb_work_writer.policy import (
    InvalidWriteRequest,
    cursor_is_not_behind,
    local_datetime,
    normalize_request,
    transition_allowed,
)


class WriteError(RuntimeError):
    pass


def _json(value: Any) -> str | None:
    return None if value is None else json.dumps(value, ensure_ascii=False, separators=(",", ":"))


class WorkWriter:
    CONTRACT_VERSION = "polardb-work-writer/0.2.0"
    is_durable_work_writer = True
    REQUIRED_OPERATIONS = frozenset({
        "record_message", "claim_action", "claim_child_action", "save_action_result",
        "save_assignment_audit", "link_external_task", "advance_scan_state",
        "resolve_review_ignore", "claim_review_create", "repair_review_duplicate_family",
        "detach_review_duplicate_action",
    })

    def __init__(self, connect_factory: Callable[[], Any] = connect_writer):
        self.connect_factory = connect_factory
        self._schema_contract_cache: dict[str, Any] | None = None

    def contract_info(self) -> dict[str, Any]:
        """Read-only startup probe for the additive child/audit schema contract."""
        conn = self.connect_factory()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COLUMN_NAME, COLUMN_TYPE, DATA_TYPE, IS_NULLABLE "
                    "FROM information_schema.columns "
                    "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s "
                    "AND COLUMN_NAME IN (%s,%s)",
                    ("message_action", "parent_action_id", "rule_audit"),
                )
                column_rows = {str(row["COLUMN_NAME"]): row for row in cur.fetchall()}
                cur.execute(
                    "SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME "
                    "FROM information_schema.statistics "
                    "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s "
                    "ORDER BY INDEX_NAME, SEQ_IN_INDEX",
                    ("message_action",),
                )
                index_rows = cur.fetchall()
                signatures: set[tuple[str, ...]] = set()
                all_signatures: set[tuple[str, ...]] = set()
                by_index: dict[str, list[dict[str, Any]]] = {}
                for row in index_rows:
                    by_index.setdefault(str(row["INDEX_NAME"]), []).append(row)
                for rows in by_index.values():
                    all_signatures.add(tuple(str(row["COLUMN_NAME"]) for row in rows))
                    if rows and all(int(row["NON_UNIQUE"]) == 0 for row in rows):
                        signatures.add(tuple(str(row["COLUMN_NAME"]) for row in rows))
                cur.execute(
                    "SELECT REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME "
                    "FROM information_schema.key_column_usage "
                    "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s "
                    "AND COLUMN_NAME=%s AND REFERENCED_TABLE_NAME IS NOT NULL",
                    ("message_action", "parent_action_id"),
                )
                foreign_keys = cur.fetchall()
        finally:
            conn.close()
        expected_unique = {
            ("conversation_id", "message_id", "action_key"),
            ("external_task_id",),
            ("business_key",),
        }
        fk_ready = any(
            row.get("REFERENCED_TABLE_NAME") == "message_action"
            and row.get("REFERENCED_COLUMN_NAME") == "id"
            for row in foreign_keys
        )
        required_column_types = {
            "parent_action_id": ("bigint unsigned", "bigint", "YES"),
            "rule_audit": ("json", "json", "YES"),
        }
        missing = sorted(set(required_column_types) - set(column_rows))
        invalid_columns = sorted(
            name for name, expected in required_column_types.items()
            if name in column_rows and (
                str(column_rows[name].get("COLUMN_TYPE") or "").lower(),
                str(column_rows[name].get("DATA_TYPE") or "").lower(),
                str(column_rows[name].get("IS_NULLABLE") or "").upper(),
            ) != expected
        )
        missing_unique = sorted(expected_unique - signatures)
        parent_index_ready = ("parent_action_id",) in all_signatures
        supported = not missing and not invalid_columns and not missing_unique and fk_ready and parent_index_ready
        return {
            "contract_version": self.CONTRACT_VERSION,
            "operations": sorted(self.REQUIRED_OPERATIONS),
            "schema_supported": supported,
            "missing_columns": missing,
            "invalid_columns": invalid_columns,
            "missing_unique_indexes": [list(row) for row in missing_unique],
            "parent_foreign_key_supported": fk_ready,
            "parent_index_supported": parent_index_ready,
        }

    def execute(self, request: dict[str, Any]) -> dict[str, Any]:
        normalized = normalize_request(request)
        if self._schema_contract_cache is None:
            try:
                self._schema_contract_cache = self.contract_info()
            except Exception as exc:
                raise WriteError("writer_schema_probe_failed") from exc
        if self._schema_contract_cache.get("schema_supported") is not True:
            raise WriteError("writer_schema_contract_unsupported")
        conn = self.connect_factory()
        try:
            result = self._dispatch(conn, normalized)
            if normalized["dry_run"]:
                conn.rollback()
                result.update(committed=False, dry_run=True)
            else:
                conn.commit()
                result.update(committed=True, dry_run=False)
            return {"ok": True, "operation": normalized["operation"], **result}
        except WriteError:
            conn.rollback()
            raise
        except InvalidWriteRequest:
            conn.rollback()
            raise
        except pymysql.IntegrityError as exc:
            conn.rollback()
            raise WriteError("constraint_violation") from exc
        except pymysql.OperationalError as exc:
            conn.rollback()
            if exc.args and exc.args[0] == 1142:
                raise WriteError("database_write_permission_denied") from exc
            raise WriteError("database_write_failed") from exc
        finally:
            conn.close()

    def _dispatch(self, conn: Any, request: dict[str, Any]) -> dict[str, Any]:
        operation = request["operation"]
        if operation == "record_message":
            return self._record_message(conn, request)
        if operation == "claim_action":
            return self._claim_action(conn, request)
        if operation == "claim_child_action":
            return self._claim_child_action(conn, request)
        if operation in {"save_action_result", "link_external_task"}:
            return self._save_action_result(conn, request)
        if operation == "save_assignment_audit":
            return self._save_assignment_audit(conn, request)
        if operation == "review_area_duty":
            return self._review_area_duty(conn, request)
        if operation == "resolve_review_ignore":
            return self._resolve_review_action(conn, request, "IGNORED")
        if operation == "claim_review_create":
            return self._resolve_review_action(conn, request, "CLAIMED")
        if operation == "repair_review_duplicate_family":
            return self._repair_review_duplicate_family(conn, request)
        if operation == "detach_review_duplicate_action":
            return self._detach_review_duplicate_action(conn, request)
        return self._advance_scan_state(conn, request)

    @staticmethod
    def _resolve_review_ignore(conn: Any, request: dict[str, Any]) -> dict[str, Any]:
        return WorkWriter._resolve_review_action(conn, request, "IGNORED")

    @staticmethod
    def _resolve_review_action(conn: Any, request: dict[str, Any], target: str) -> dict[str, Any]:
        """Atomically transition one unchanged, unlinked REVIEW action."""
        if target not in {"IGNORED", "CLAIMED"}:
            raise WriteError("review_target_invalid")
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, conversation_id, message_id, action_key, status, business_key, "
                "external_task_id, duplicate_of_action_id, parent_action_id, updated_at "
                "FROM message_action WHERE id=%s FOR UPDATE", (request["action_id"],),
            )
            row = cur.fetchone()
            if not row:
                raise WriteError("action_not_found")
            if (row.get("conversation_id") != request["conversation_id"]
                    or row.get("message_id") != request["message_id"]
                    or row.get("action_key") != "todo" or row.get("status") != "REVIEW"
                    or row.get("business_key") != request["expected_business_key"]
                    or row.get("updated_at") != request["expected_updated_at"]
                    or row.get("external_task_id") not in (None, "")
                    or row.get("duplicate_of_action_id") not in (None, "")
                    or row.get("parent_action_id") not in (None, "")):
                raise WriteError("review_action_changed_or_linked")
            cur.execute("UPDATE message_action SET status=%s, last_error=NULL WHERE id=%s AND status=%s",
                        (target, request["action_id"], "REVIEW"))
            if cur.rowcount != 1:
                raise WriteError("review_action_update_not_unique")
            cur.execute("SELECT id,status,external_task_id,duplicate_of_action_id FROM message_action WHERE id=%s",
                        (request["action_id"],))
            result = cur.fetchone()
        if not result or result.get("status") != target:
            raise WriteError("review_action_readback_failed")
        return {"action_id": request["action_id"], "row": result}

    @staticmethod
    def _repair_review_duplicate_family(conn: Any, request: dict[str, Any]) -> dict[str, Any]:
        """Atomically detach a bad canonical pointer and reparent its verified siblings."""
        canonical_id = request["canonical_action_id"]
        old_canonical_id = request["old_canonical_action_id"]
        duplicates = request["duplicate_action_ids"]
        expected = {row["action_id"]: row for row in request["expected_rows"]}
        if set(expected) != {canonical_id, old_canonical_id, *duplicates}:
            raise WriteError("review_duplicate_expected_rows_mismatch")
        actual: dict[int, dict[str, Any]] = {}
        with conn.cursor() as cur:
            for action_id in sorted(expected):
                cur.execute(
                    "SELECT id, conversation_id, message_id, action_key, status, business_key, "
                    "external_task_id, duplicate_of_action_id, parent_action_id, updated_at "
                    "FROM message_action WHERE id=%s FOR UPDATE", (action_id,),
                )
                row = cur.fetchone()
                if not row:
                    raise WriteError("review_duplicate_action_not_found")
                snapshot = expected[action_id]
                for field in ("conversation_id", "message_id", "action_key", "status", "business_key",
                              "external_task_id", "duplicate_of_action_id", "parent_action_id"):
                    if row.get(field) != snapshot[field]:
                        raise WriteError("review_duplicate_snapshot_mismatch")
                current_updated = row.get("updated_at")
                if hasattr(current_updated, "replace") and hasattr(current_updated, "year"):
                    current_updated = current_updated.replace(tzinfo=None)
                if current_updated != snapshot["updated_at"]:
                    raise WriteError("review_duplicate_snapshot_mismatch")
                if row.get("status") != "REVIEW" or row.get("action_key") != "todo":
                    raise WriteError("review_duplicate_action_not_review")
                if row.get("external_task_id") not in (None, "") or row.get("parent_action_id") not in (None, ""):
                    raise WriteError("review_duplicate_action_linked")
                actual[action_id] = row

        canonical = actual[canonical_id]
        if canonical.get("duplicate_of_action_id") != old_canonical_id:
            raise WriteError("review_duplicate_canonical_pointer_changed")
        old_canonical = actual[old_canonical_id]
        if old_canonical.get("duplicate_of_action_id") not in (None, ""):
            raise WriteError("review_old_canonical_is_not_canonical")
        for action_id in duplicates:
            if actual[action_id].get("duplicate_of_action_id") != old_canonical_id:
                raise WriteError("review_duplicate_pointer_changed")

        with conn.cursor() as cur:
            reparented: list[int] = []
            for action_id in [canonical_id, *duplicates]:
                target = None if action_id == canonical_id else canonical_id
                cur.execute(
                    "UPDATE message_action SET duplicate_of_action_id=%s "
                    "WHERE id=%s AND duplicate_of_action_id=%s AND status=%s "
                    "AND external_task_id IS NULL AND parent_action_id IS NULL",
                    (target, action_id, old_canonical_id, "REVIEW"),
                )
                if cur.rowcount != 1:
                    raise WriteError("review_duplicate_reparent_not_unique")
                reparented.append(action_id)
            for action_id in [canonical_id, *duplicates]:
                cur.execute(
                    "SELECT id, status, external_task_id, duplicate_of_action_id "
                    "FROM message_action WHERE id=%s", (action_id,),
                )
                row = cur.fetchone()
                expected_parent = None if action_id == canonical_id else canonical_id
                if (not row or row.get("status") != "REVIEW"
                        or row.get("external_task_id") not in (None, "")
                        or row.get("duplicate_of_action_id") != expected_parent):
                    raise WriteError("review_duplicate_reparent_readback_failed")
        return {"canonical_action_id": canonical_id, "old_canonical_action_id": old_canonical_id,
                "reparented_action_ids": reparented,
                "duplicate_action_ids": duplicates}

    @staticmethod
    def _detach_review_duplicate_action(conn: Any, request: dict[str, Any]) -> dict[str, Any]:
        """Detach one fully verified, unlinked REVIEW duplicate without changing its source."""
        target_id = request["target_action_id"]
        canonical_id = request["expected_canonical_action_id"]
        expected = {
            target_id: request["expected_target"],
            canonical_id: request["expected_canonical"],
        }
        select_sql = (
            "SELECT id, conversation_id, message_id, action_key, status, business_key, "
            "external_task_id, todo_id, duplicate_of_action_id, parent_action_id, updated_at "
            "FROM message_action WHERE id=%s"
        )

        def snapshot_for(row: dict[str, Any]) -> dict[str, Any]:
            try:
                return {
                    "action_id": row.get("id"),
                    "conversation_id": row.get("conversation_id"),
                    "message_id": row.get("message_id"),
                    "action_key": row.get("action_key"),
                    "status": row.get("status"),
                    "business_key": None if row.get("business_key") in (None, "") else row.get("business_key"),
                    "external_task_id": None if row.get("external_task_id") in (None, "") else row.get("external_task_id"),
                    "todo_id": None if row.get("todo_id") in (None, "") else row.get("todo_id"),
                    "duplicate_of_action_id": None if row.get("duplicate_of_action_id") in (None, "") else row.get("duplicate_of_action_id"),
                    "parent_action_id": None if row.get("parent_action_id") in (None, "") else row.get("parent_action_id"),
                    "updated_at": local_datetime(row.get("updated_at"), "updated_at"),
                }
            except (InvalidWriteRequest, AttributeError, TypeError, ValueError) as exc:
                raise WriteError("review_duplicate_row_invalid") from exc

        def raw_fields_equal(left: dict[str, Any], right: dict[str, Any], *, skip_duplicate: bool = False) -> bool:
            fields = (
                "id", "conversation_id", "message_id", "action_key", "status", "business_key",
                "external_task_id", "todo_id", "duplicate_of_action_id", "parent_action_id", "updated_at",
            )
            return all(
                left.get(field) == right.get(field)
                for field in fields if not (skip_duplicate and field == "duplicate_of_action_id")
            )

        before: dict[int, dict[str, Any]] = {}
        with conn.cursor() as cur:
            for action_id in sorted(expected):
                cur.execute(select_sql + " FOR UPDATE", (action_id,))
                row = cur.fetchone()
                if not row:
                    raise WriteError("review_duplicate_action_not_found")
                if snapshot_for(row) != expected[action_id]:
                    raise WriteError("review_duplicate_snapshot_mismatch")
                before[action_id] = dict(row)

            target_before = snapshot_for(before[target_id])
            canonical_before = snapshot_for(before[canonical_id])
            if (target_before["duplicate_of_action_id"] != canonical_id
                    or canonical_before["duplicate_of_action_id"] is not None):
                raise WriteError("review_duplicate_relationship_changed")
            if (target_before["action_key"] != "todo" or target_before["status"] != "REVIEW"
                    or target_before["parent_action_id"] is not None
                    or target_before["external_task_id"] is not None
                    or target_before["todo_id"] is not None):
                raise WriteError("review_duplicate_target_changed_or_linked")
            if (canonical_before["action_key"] != "todo" or canonical_before["status"] != "REVIEW"
                    or canonical_before["parent_action_id"] is not None):
                raise WriteError("review_duplicate_canonical_changed")

            cur.execute(
                "UPDATE message_action SET duplicate_of_action_id=NULL, updated_at=updated_at "
                "WHERE id=%s AND conversation_id=%s AND message_id=%s AND action_key=%s AND status=%s "
                "AND duplicate_of_action_id=%s AND parent_action_id IS NULL "
                "AND (external_task_id IS NULL OR external_task_id='') AND todo_id IS NULL "
                "AND COALESCE(business_key, '') <=> COALESCE(%s, '') AND updated_at=%s",
                (
                    target_id, target_before["conversation_id"], target_before["message_id"],
                    "todo", "REVIEW", canonical_id, target_before["business_key"], target_before["updated_at"],
                ),
            )
            if cur.rowcount != 1:
                raise WriteError("review_duplicate_detach_guard_mismatch")

            after: dict[int, dict[str, Any]] = {}
            for action_id in (target_id, canonical_id):
                cur.execute(select_sql, (action_id,))
                row = cur.fetchone()
                if not row:
                    raise WriteError("review_duplicate_readback_failed")
                after[action_id] = dict(row)

        target_expected_after = dict(expected[target_id])
        target_expected_after["duplicate_of_action_id"] = None
        if (snapshot_for(after[target_id]) != target_expected_after
                or not raw_fields_equal(before[target_id], after[target_id], skip_duplicate=True)
                or after[target_id].get("duplicate_of_action_id") is not None):
            raise WriteError("review_duplicate_target_readback_mismatch")
        if (snapshot_for(after[canonical_id]) != expected[canonical_id]
                or not raw_fields_equal(before[canonical_id], after[canonical_id])):
            raise WriteError("review_duplicate_canonical_readback_mismatch")

        return {
            "target_action_id": target_id,
            "expected_canonical_action_id": canonical_id,
            "detached": True,
        }

    @staticmethod
    def _record_message(conn: Any, request: dict[str, Any]) -> dict[str, Any]:
        sql = (
            "INSERT INTO message_inbox "
            "(conversation_id, message_id, group_name, sender_id, sender_name, message_time, "
            "text, msg_type, resource_refs, quoted_message, raw_payload, source) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON DUPLICATE KEY UPDATE id=id"
        )
        params = (
            request["conversation_id"], request["message_id"], request["group_name"],
            request["sender_id"], request["sender_name"], request["message_time"],
            request["text"], request["msg_type"], _json(request["resource_refs"]),
            _json(request["quoted_message"]), _json(request["raw_payload"]), request["source"],
        )
        with conn.cursor() as cur:
            cur.execute(sql, params)
            created = cur.rowcount == 1
            cur.execute(
                "SELECT id, conversation_id, message_id FROM message_inbox "
                "WHERE conversation_id=%s AND message_id=%s",
                (request["conversation_id"], request["message_id"]),
            )
            row = cur.fetchone()
        if not row:
            raise WriteError("message_write_not_readable")
        return {"record_id": row["id"], "created": created, "row": row}

    @staticmethod
    def _claim_action(conn: Any, request: dict[str, Any]) -> dict[str, Any]:
        select_source = (
            "SELECT id, conversation_id, message_id, action_key, status, business_key, "
            "external_task_id, duplicate_of_action_id, parent_action_id, rule_audit, last_error "
            "FROM message_action WHERE conversation_id=%s AND message_id=%s AND action_key=%s FOR UPDATE"
        )
        select_business = (
            "SELECT id, conversation_id, message_id, action_key, status, business_key, "
            "external_task_id, duplicate_of_action_id, parent_action_id "
            "FROM message_action WHERE business_key=%s FOR UPDATE"
        )

        def get_source(cur: Any) -> dict[str, Any] | None:
            cur.execute(select_source, (request["conversation_id"], request["message_id"], request["action_key"]))
            return cur.fetchone()

        def result_for(cur: Any, row: dict[str, Any], *, created: bool, reason: str) -> dict[str, Any]:
            duplicate_of = row.get("duplicate_of_action_id")
            canonical = row
            if duplicate_of not in (None, ""):
                cur.execute(
                    "SELECT id, external_task_id, business_key, duplicate_of_action_id "
                    "FROM message_action WHERE id=%s FOR UPDATE",
                    (duplicate_of,),
                )
                canonical = cur.fetchone()
                if not canonical or canonical.get("duplicate_of_action_id") not in (None, ""):
                    raise WriteError("duplicate_canonical_action_invalid")
            return {
                "action_id": row["id"],
                "created": created,
                "reason": reason,
                "row": row,
                "external_task_id": row.get("external_task_id"),
                "duplicate_of_action_id": duplicate_of,
                "canonical_action_id": duplicate_of or row["id"],
                "canonical_external_task_id": canonical.get("external_task_id"),
                "effective_external_task_id": row.get("external_task_id") or canonical.get("external_task_id"),
            }

        def insert_duplicate_source(cur: Any, canonical_id: int) -> dict[str, Any]:
            try:
                cur.execute(
                    "INSERT INTO message_action "
                    "(conversation_id, message_id, action_key, business_key, status, duplicate_of_action_id) "
                    "VALUES (%s,%s,%s,NULL,%s,%s)",
                    (request["conversation_id"], request["message_id"], request["action_key"],
                     request["status"], canonical_id),
                )
            except pymysql.IntegrityError as exc:
                if not exc.args or exc.args[0] != 1062:
                    raise
                existing_source = get_source(cur)
                if existing_source:
                    return result_for(cur, existing_source, created=False, reason="source_exists")
                raise
            action_id = cur.lastrowid
            cur.execute("SELECT * FROM message_action WHERE id=%s FOR UPDATE", (action_id,))
            row = cur.fetchone()
            if not row:
                raise WriteError("duplicate_source_action_not_readable")
            return result_for(cur, row, created=True, reason="business_exists")

        with conn.cursor() as cur:
            existing = get_source(cur)
            if existing:
                # A prior REVIEW may become CREATE after deterministic rules are
                # corrected (for example, an explicit @current-user assignment).
                # Reclaim only that source when no external task is linked; all
                # other existing claims remain idempotent no-ops.
                if existing.get("status") == "IGNORED" and request["status"] == "REVIEW":
                    cur.execute(
                        "UPDATE message_action SET status=%s, last_error=NULL WHERE id=%s",
                        ("REVIEW", existing["id"]),
                    )
                    cur.execute(
                        "SELECT id, status, business_key, external_task_id FROM message_action WHERE id=%s",
                        (existing["id"],),
                    )
                    row = cur.fetchone()
                    return {
                        "action_id": existing["id"],
                        "created": False,
                        "reason": "ignored_reclassified_review",
                        "row": row,
                    }
                if (
                    existing.get("status") == "REVIEW"
                    and request["status"] == "CLAIMED"
                    and not existing.get("external_task_id")
                ):
                    cur.execute(
                        "UPDATE message_action SET status=%s, last_error=NULL WHERE id=%s",
                        ("CLAIMED", existing["id"]),
                    )
                    cur.execute(
                        "SELECT id, status, business_key, external_task_id FROM message_action WHERE id=%s",
                        (existing["id"],),
                    )
                    row = cur.fetchone()
                    return {
                        "action_id": existing["id"],
                        "created": True,
                        "reason": "review_reclaimed",
                        "row": row,
                    }
                return result_for(cur, existing, created=False, reason="source_exists")
            if request["business_key"]:
                cur.execute(select_business, (request["business_key"],))
                existing = cur.fetchone()
                if existing:
                    if existing.get("duplicate_of_action_id") not in (None, ""):
                        raise WriteError("business_key_owner_is_not_canonical")
                    return insert_duplicate_source(cur, int(existing["id"]))
            try:
                cur.execute(
                    "INSERT INTO message_action "
                    "(conversation_id, message_id, action_key, business_key, status) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (
                        request["conversation_id"], request["message_id"], request["action_key"],
                        request["business_key"], request["status"],
                    ),
                )
            except pymysql.IntegrityError as exc:
                if not exc.args or exc.args[0] != 1062:
                    raise
                existing_source = get_source(cur)
                if existing_source:
                    return result_for(cur, existing_source, created=False, reason="source_exists")
                if request["business_key"]:
                    cur.execute(select_business, (request["business_key"],))
                    canonical = cur.fetchone()
                    if canonical and canonical.get("duplicate_of_action_id") in (None, ""):
                        return insert_duplicate_source(cur, int(canonical["id"]))
                raise WriteError("claim_action_unique_conflict_unresolved") from exc
            action_id = cur.lastrowid
            cur.execute("SELECT * FROM message_action WHERE id=%s FOR UPDATE", (action_id,))
            row = cur.fetchone()
            if not row:
                raise WriteError("claimed_action_not_readable")
            return result_for(cur, row, created=True, reason="claimed")

    @staticmethod
    def _claim_child_action(conn: Any, request: dict[str, Any]) -> dict[str, Any]:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, conversation_id, message_id, action_key, business_key, status, "
                "external_task_id, parent_action_id, duplicate_of_action_id "
                "FROM message_action WHERE id=%s FOR UPDATE",
                (request["parent_action_id"],),
            )
            parent = cur.fetchone()
            if not parent:
                raise WriteError("child_parent_action_not_found")
            if (
                parent.get("conversation_id") != request["conversation_id"]
                or parent.get("message_id") != request["message_id"]
                or parent.get("business_key") != request["business_scope"]
                or parent.get("action_key") != "todo"
                or parent.get("parent_action_id") not in (None, "")
                or parent.get("duplicate_of_action_id") not in (None, "")
                or parent.get("external_task_id") != request["parent_task_id"]
            ):
                raise WriteError("child_parent_link_or_source_mismatch")
            cur.execute(
                "SELECT id, conversation_id, message_id, action_key, status, business_key, "
                "external_task_id, parent_action_id, duplicate_of_action_id, last_error "
                "FROM message_action WHERE conversation_id=%s AND message_id=%s AND action_key=%s FOR UPDATE",
                (request["conversation_id"], request["message_id"], request["action_key"]),
            )
            existing = cur.fetchone()
            if existing:
                if (
                    existing.get("parent_action_id") != request["parent_action_id"]
                    or existing.get("duplicate_of_action_id") not in (None, "")
                    or existing.get("business_key") not in (None, "")
                ):
                    raise WriteError("child_action_parent_conflict")
                return {
                    "action_id": existing["id"], "created": False,
                    "external_task_id": existing.get("external_task_id"),
                    "parent_action_id": existing.get("parent_action_id"),
                    "last_error": existing.get("last_error"), "status": existing.get("status"),
                    "row": existing,
                }
            cur.execute(
                "INSERT INTO message_action "
                "(conversation_id, message_id, action_key, business_key, status, parent_action_id) "
                "VALUES (%s,%s,%s,NULL,%s,%s)",
                (request["conversation_id"], request["message_id"], request["action_key"],
                 request["status"], request["parent_action_id"]),
            )
            action_id = cur.lastrowid
            cur.execute(
                "SELECT id, conversation_id, message_id, action_key, status, business_key, "
                "external_task_id, parent_action_id, duplicate_of_action_id, last_error "
                "FROM message_action WHERE id=%s FOR UPDATE",
                (action_id,),
            )
            row = cur.fetchone()
        if not row:
            raise WriteError("child_action_not_readable")
        return {
            "action_id": action_id, "created": True, "external_task_id": None,
            "parent_action_id": request["parent_action_id"], "last_error": None,
            "status": request["status"], "row": row,
        }

    @staticmethod
    def _save_action_result(conn: Any, request: dict[str, Any]) -> dict[str, Any]:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, status, external_task_id, duplicate_of_action_id FROM message_action WHERE id=%s FOR UPDATE",
                (request["action_id"],),
            )
            current = cur.fetchone()
            if not current:
                raise WriteError("action_not_found")
            if current.get("duplicate_of_action_id") not in (None, "") and request.get("external_task_id"):
                raise WriteError("duplicate_external_task_must_stay_on_canonical_action")
            if (
                request.get("duplicate_of_action_id") not in (None, "")
                and request.get("duplicate_of_action_id") != current.get("duplicate_of_action_id")
            ):
                raise WriteError("duplicate_pointer_must_be_claimed_atomically")
            if not transition_allowed(current["status"], request["status"]):
                raise WriteError("invalid_status_transition")
            if request["status"] in {"EXTERNAL_CREATED", "SUCCESS"} and not (
                request["external_task_id"] or current["external_task_id"]
            ):
                raise WriteError("missing_external_task_id")
            if (
                current["external_task_id"]
                and request["external_task_id"]
                and current["external_task_id"] != request["external_task_id"]
            ):
                raise WriteError("external_task_id_conflict")
            cur.execute(
                "UPDATE message_action SET status=%s, "
                "external_task_id=COALESCE(%s, external_task_id), "
                "todo_id=COALESCE(%s, todo_id), "
                "duplicate_of_action_id=COALESCE(%s, duplicate_of_action_id), "
                "last_error=%s, attempt_count=COALESCE(%s, attempt_count) WHERE id=%s",
                (
                    request["status"], request["external_task_id"], request["todo_id"],
                    request["duplicate_of_action_id"], request["last_error"], request["attempt_count"],
                    request["action_id"],
                ),
            )
            cur.execute(
                "SELECT id, status, external_task_id, todo_id, attempt_count, last_error "
                "FROM message_action WHERE id=%s",
                (request["action_id"],),
            )
            row = cur.fetchone()
        return {"action_id": request["action_id"], "row": row}

    @staticmethod
    def _save_assignment_audit(conn: Any, request: dict[str, Any]) -> dict[str, Any]:
        encoded = _json(request["assignment_audit"])
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, rule_audit FROM message_action WHERE id=%s FOR UPDATE",
                (request["action_id"],),
            )
            current = cur.fetchone()
            if not current:
                raise WriteError("action_not_found")
            old = current.get("rule_audit")
            if isinstance(old, (bytes, bytearray)):
                old = old.decode("utf-8")
            if isinstance(old, str) and old:
                try:
                    old = json.loads(old)
                except json.JSONDecodeError as exc:
                    raise WriteError("assignment_audit_existing_value_invalid") from exc
            if old is not None:
                if old != request["assignment_audit"]:
                    raise WriteError("assignment_audit_conflict")
                return {"action_id": request["action_id"], "changed": False, "rule_audit": old}
            cur.execute(
                "UPDATE message_action SET rule_audit=%s WHERE id=%s",
                (encoded, request["action_id"]),
            )
            cur.execute("SELECT id, rule_audit FROM message_action WHERE id=%s", (request["action_id"],))
            row = cur.fetchone()
        if not row or row.get("rule_audit") in (None, ""):
            raise WriteError("assignment_audit_not_readable")
        return {"action_id": request["action_id"], "changed": True, "row": row}

    @staticmethod
    def _review_area_duty(conn: Any, request: dict[str, Any]) -> dict[str, Any]:
        """Mark one exact, owner-confirmed area-duty row as reviewed.

        The snapshot fields are part of the write contract so a stale request
        cannot review a row that changed underneath it.  The SQL is fixed and
        parameterized; this operation cannot update arbitrary configuration.
        """
        select_sql = (
            "SELECT id, area_name, staff_name, cycle_start, cycle_end, reviewed "
            "FROM config_area_duty WHERE id=%s FOR UPDATE"
        )
        with conn.cursor() as cur:
            cur.execute(select_sql, (request["duty_id"],))
            current = cur.fetchone()
            if not current:
                raise WriteError("area_duty_not_found")

            def date_text(value: Any) -> str:
                return value.isoformat() if hasattr(value, "isoformat") else str(value)

            snapshot_matches = (
                str(current["area_name"]) == request["area_name"]
                and str(current["staff_name"]) == request["staff_name"]
                and date_text(current["cycle_start"]) == request["cycle_start"].isoformat()
                and date_text(current["cycle_end"]) == request["cycle_end"].isoformat()
            )
            if not snapshot_matches:
                raise WriteError("area_duty_snapshot_mismatch")

            changed = False
            if not bool(current["reviewed"]):
                if not request["dry_run"]:
                    cur.execute(
                        "UPDATE config_area_duty SET reviewed=1 WHERE id=%s AND reviewed=0",
                        (request["duty_id"],),
                    )
                    changed = cur.rowcount == 1
                else:
                    changed = True

            cur.execute(
                "SELECT id, area_name, staff_name, cycle_start, cycle_end, reviewed "
                "FROM config_area_duty WHERE id=%s",
                (request["duty_id"],),
            )
            row = cur.fetchone()
        if not row or (not request["dry_run"] and not bool(row["reviewed"])):
            raise WriteError("area_duty_review_not_readable")
        return {"duty_id": request["duty_id"], "changed": changed, "row": row}

    @staticmethod
    def _advance_scan_state(conn: Any, request: dict[str, Any]) -> dict[str, Any]:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT group_name, conversation_id, last_msg_time, last_msg_id "
                "FROM scan_state WHERE group_name=%s FOR UPDATE",
                (request["group_name"],),
            )
            old = cur.fetchone()
            if old and not cursor_is_not_behind(
                old["last_msg_time"], old["last_msg_id"], request["last_msg_time"], request["last_msg_id"]
            ):
                raise WriteError("scan_cursor_would_move_backwards")
            if old and old["last_msg_time"] == request["last_msg_time"] and old["last_msg_id"] == request["last_msg_id"]:
                return {"group_name": request["group_name"], "changed": False, "row": old}
            if old:
                cur.execute(
                    "UPDATE scan_state SET conversation_id=COALESCE(%s, conversation_id), "
                    "last_msg_time=%s, last_msg_id=%s WHERE group_name=%s",
                    (
                        request["conversation_id"], request["last_msg_time"], request["last_msg_id"],
                        request["group_name"],
                    ),
                )
            else:
                cur.execute(
                    "INSERT INTO scan_state "
                    "(group_name, conversation_id, last_msg_time, last_msg_id) VALUES (%s,%s,%s,%s)",
                    (
                        request["group_name"], request["conversation_id"], request["last_msg_time"],
                        request["last_msg_id"],
                    ),
                )
            cur.execute(
                "SELECT group_name, conversation_id, last_msg_time, last_msg_id "
                "FROM scan_state WHERE group_name=%s",
                (request["group_name"],),
            )
            row = cur.fetchone()
        return {"group_name": request["group_name"], "changed": True, "row": row}
