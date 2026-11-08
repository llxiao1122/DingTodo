"""Read-only reconciliation for a todo create with an unknown, unlinked result."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import sqlite3
import sys
from typing import Any

from dws_adapter import DwsClient
from routing import build_source_comment


MAX_SEARCH_ROWS = 50


class UnknownCreateEvidenceError(ValueError):
    """Local evidence is insufficient to reconcile an unknown create."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class CreateEvidence:
    batch_id: str
    ordinal: int
    action_id: str
    source_message_id: str
    title: str
    executor_user_id: str
    source_comment: str


def _open_readonly_database(path: str | Path) -> sqlite3.Connection:
    try:
        uri = Path(path).resolve(strict=True).as_uri() + "?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=5)
        connection.execute("PRAGMA query_only=ON")
        return connection
    except (OSError, sqlite3.Error, ValueError) as exc:
        raise UnknownCreateEvidenceError("local_database_unavailable") from exc


def _text(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    return None


def _frozen_source_comment(
    connection: sqlite3.Connection,
    *,
    batch_id: str,
    action_id: str,
    source_message_id: str,
) -> str:
    try:
        rows = connection.execute(
            "SELECT items,status FROM dispatch WHERE id=?", (batch_id,)
        ).fetchall()
    except sqlite3.Error as exc:
        raise UnknownCreateEvidenceError("frozen_dispatch_unavailable") from exc
    if len(rows) != 1 or rows[0][1] != "delivered":
        raise UnknownCreateEvidenceError("frozen_dispatch_not_verified")
    try:
        items = json.loads(rows[0][0])
    except (TypeError, json.JSONDecodeError) as exc:
        raise UnknownCreateEvidenceError("frozen_dispatch_invalid") from exc
    if not isinstance(items, list):
        raise UnknownCreateEvidenceError("frozen_dispatch_invalid")
    matches = [item for item in items if isinstance(item, dict)
               and str(item.get("action_id") or "") == action_id]
    if len(matches) != 1 or not isinstance(matches[0].get("source"), dict):
        raise UnknownCreateEvidenceError("frozen_source_not_unique")

    source = matches[0]["source"]
    group = source.get("group_name")
    sender = source.get("sender_name")
    message_time = source.get("message_time")
    text = source.get("text")
    if (not isinstance(group, str) or not group.strip()
            or not isinstance(sender, str) or not sender.strip()
            or not isinstance(message_time, str) or not message_time.strip()
            or not isinstance(text, str) or not text.strip()
            or source.get("text_truncated") is not False or len(text) > 1600):
        raise UnknownCreateEvidenceError("frozen_source_incomplete")
    frozen_message_id = _text(source.get("message_id") or source.get("messageId"))
    if frozen_message_id is not None and frozen_message_id != source_message_id:
        raise UnknownCreateEvidenceError("frozen_source_message_mismatch")

    return build_source_comment({
        "group": group,
        "sender": sender,
        "create_time": message_time,
        "message_id": source_message_id,
        "text": text,
    })


def load_unknown_create_evidence(
    database_path: str | Path,
    batch_id: str,
    ordinal: int,
) -> CreateEvidence:
    """Load a pending create audit and frozen source without modifying SQLite."""
    if not isinstance(batch_id, str) or not batch_id.strip():
        raise UnknownCreateEvidenceError("batch_id_missing")
    if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal < 1:
        raise UnknownCreateEvidenceError("ordinal_invalid")

    connection = _open_readonly_database(database_path)
    try:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(resolution)")}
        required = {"action_id", "decision", "request_json", "status", "external_task_id"}
        if not required.issubset(columns):
            raise UnknownCreateEvidenceError("resolution_schema_incomplete")
        rows = connection.execute(
            "SELECT action_id,decision,request_json,status,external_task_id "
            "FROM resolution WHERE batch_id=? AND ordinal=?",
            (batch_id, ordinal),
        ).fetchall()
        if len(rows) != 1:
            raise UnknownCreateEvidenceError("resolution_not_unique")
        action_id_value, decision, request_json, status, linked_task_id = rows[0]
        if decision != "create" or status != "applying":
            raise UnknownCreateEvidenceError("create_request_not_pending")
        if linked_task_id not in (None, ""):
            raise UnknownCreateEvidenceError("task_id_already_recorded")
        try:
            audit = json.loads(request_json)
        except (TypeError, json.JSONDecodeError) as exc:
            raise UnknownCreateEvidenceError("create_audit_invalid") from exc
        if not isinstance(audit, dict) or audit.get("create_attempted") is not True:
            raise UnknownCreateEvidenceError("create_attempt_not_verified")

        action_id = _text(action_id_value)
        claim = audit.get("claim")
        source_message_id = _text(audit.get("source_message_id"))
        title = _text(audit.get("title"))
        executor_user_id = _text(audit.get("executor_user_id"))
        if (action_id is None or not isinstance(claim, dict)
                or _text(claim.get("action_id")) != action_id
                or source_message_id is None
                or _text(claim.get("message_id")) != source_message_id
                or title is None or executor_user_id is None):
            raise UnknownCreateEvidenceError("create_identity_incomplete")

        source_comment = _frozen_source_comment(
            connection,
            batch_id=batch_id,
            action_id=action_id,
            source_message_id=source_message_id,
        )
        return CreateEvidence(
            batch_id=batch_id,
            ordinal=ordinal,
            action_id=action_id,
            source_message_id=source_message_id,
            title=title,
            executor_user_id=executor_user_id,
            source_comment=source_comment,
        )
    except sqlite3.Error as exc:
        raise UnknownCreateEvidenceError("local_evidence_read_failed") from exc
    finally:
        connection.close()


def _executor_ids(detail: dict[str, Any]) -> list[str] | None:
    rows = detail.get("executorInfos")
    if not isinstance(rows, list) or not rows:
        return None
    result: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            return None
        nested = row.get("orgEmployeeModel")
        nested = nested if isinstance(nested, dict) else {}
        user_id = row.get("userId") or row.get("user_id") or nested.get("userId")
        normalized = _text(user_id)
        if normalized is None:
            return None
        result.append(normalized)
    if len(result) != len(set(result)):
        return None
    return result


def _unconfirmed(reason: str, **fields: Any) -> dict[str, Any]:
    return {
        "status": "unconfirmed",
        "reason": reason,
        "mutations_performed": 0,
        "next_step": "不要据此重建；由人工在待办详情核对，证据确认前保持未确认。",
        **fields,
    }


def reconcile_unknown_create(dws: Any, evidence: CreateEvidence) -> dict[str, Any]:
    """Return a candidate only when exact title, assignee, and full comment are unique."""
    try:
        rows = dws.search_todos(evidence.title)
    except Exception:
        return _unconfirmed("todo_search_incomplete")
    if not isinstance(rows, list):
        return _unconfirmed("todo_search_invalid")
    if len(rows) > MAX_SEARCH_ROWS:
        return _unconfirmed("todo_search_too_large")

    exact_matches: list[str] = []
    incomplete_ids: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            incomplete_ids.append("unidentified")
            continue
        task_id = _text(row.get("taskId") or row.get("todoTaskId"))
        if task_id is None:
            incomplete_ids.append("unidentified")
            continue
        try:
            detail = dws.get_todo(task_id)
        except Exception:
            incomplete_ids.append(task_id)
            continue
        if not isinstance(detail, dict):
            incomplete_ids.append(task_id)
            continue
        returned_id = _text(detail.get("taskId") or detail.get("todoTaskId"))
        subject = detail.get("subject")
        if returned_id != task_id or not isinstance(subject, str):
            incomplete_ids.append(task_id)
            continue
        if subject != evidence.title:
            continue

        executor_ids = _executor_ids(detail)
        if executor_ids is None:
            incomplete_ids.append(task_id)
            continue
        if executor_ids != [evidence.executor_user_id]:
            continue

        try:
            comments = dws.list_task_comments(task_id)
        except Exception:
            incomplete_ids.append(task_id)
            continue
        if (not isinstance(comments, list)
                or any(not isinstance(comment, dict)
                       or not isinstance(comment.get("content"), str)
                       for comment in comments)):
            incomplete_ids.append(task_id)
            continue
        exact_comment_count = sum(
            1 for comment in comments if comment.get("content") == evidence.source_comment
        )
        if exact_comment_count > 1:
            incomplete_ids.append(task_id)
        elif exact_comment_count == 1:
            exact_matches.append(task_id)

    if incomplete_ids:
        return _unconfirmed(
            "candidate_readback_incomplete",
            incomplete_task_ids=sorted(set(incomplete_ids)),
        )
    if len(exact_matches) > 1:
        return _unconfirmed(
            "multiple_exact_matches",
            matching_task_ids=sorted(set(exact_matches)),
        )
    if not exact_matches:
        return _unconfirmed(
            "no_unique_exact_match",
            next_step="未找到唯一的完整证据；这不表示任务未创建。不要重建，先人工在待办中核查。",
        )

    return {
        "status": "unique_candidate_for_manual_verification",
        "candidate": {
            "task_id": exact_matches[0],
        },
        "evidence": {
            "exact_title": True,
            "exact_executor_user_id": True,
            "full_source_comment_exact": True,
            "unique_match": True,
        },
        "mutations_performed": 0,
        "next_step": "人工打开该任务核验详情并登记真实 task id；本工具未更改线上或本地状态。",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Read-only reconciliation of an unknown todo create; never retries or writes."
    )
    parser.add_argument("--db", required=True)
    parser.add_argument("--batch", required=True)
    parser.add_argument("--ordinal", required=True, type=int)
    args = parser.parse_args(argv)

    try:
        evidence = load_unknown_create_evidence(args.db, args.batch, args.ordinal)
    except UnknownCreateEvidenceError as exc:
        print(json.dumps(_unconfirmed(exc.reason), ensure_ascii=False))
        return 0
    result = reconcile_unknown_create(
        DwsClient(confirmed_official_writes=False), evidence
    )
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
