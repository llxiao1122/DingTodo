"""Conservative reconciliation of DWS search candidates before task creation."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import re
import unicodedata
from typing import Any


class BusinessCandidateError(RuntimeError):
    """A candidate could not be safely reconciled from complete readbacks."""


def normalize_business_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    return re.sub(r"\s+", " ", text).strip()


def source_context_from_comment(comment: Any) -> dict[str, str] | None:
    if not isinstance(comment, str):
        return None
    marker = "\n原文：\n"
    if marker not in comment:
        return None
    header, original = comment.split(marker, 1)
    group_match = re.search(r"(?:^|\n)来源群：([^\n]*)", header)
    message_match = re.search(r"(?:^|\n)messageId：([^\n]*)", header)
    group = group_match.group(1).strip() if group_match else ""
    message_id = message_match.group(1).strip() if message_match else ""
    if not group or not message_id or not original.strip():
        return None
    return {"group": group, "message_id": message_id, "text": original}


def _timestamp_ms(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            numeric = int(value)
            return numeric if numeric > 10_000_000_000 else numeric * 1000
        text = str(value).strip()
        if text.isdigit():
            numeric = int(text)
            return numeric if numeric > 10_000_000_000 else numeric * 1000
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return None
        return int(parsed.timestamp() * 1000)
    except (TypeError, ValueError, OverflowError):
        return None


def _executor_rows(detail: dict[str, Any]) -> list[tuple[str, str | None]]:
    rows = detail.get("executorInfos")
    if not isinstance(rows, list):
        raise BusinessCandidateError("business_candidate_executor_readback_incomplete")
    result: list[tuple[str, str | None]] = []
    for row in rows:
        if not isinstance(row, dict):
            raise BusinessCandidateError("business_candidate_executor_readback_incomplete")
        nested = row.get("orgEmployeeModel")
        nested = nested if isinstance(nested, dict) else {}
        name = str(
            row.get("name") or row.get("orgUserName")
            or nested.get("orgUserName") or nested.get("name") or ""
        ).strip()
        user_id = row.get("userId") or row.get("user_id") or nested.get("userId")
        if not name:
            raise BusinessCandidateError("business_candidate_executor_readback_incomplete")
        result.append((name, str(user_id).strip() if user_id not in (None, "") else None))
    if not result or len({name for name, _ in result}) != len(result):
        raise BusinessCandidateError("business_candidate_executor_readback_ambiguous")
    return result


def candidate_matches_business(
    *,
    task_id: str,
    detail: Any,
    comments: Any,
    plan: dict[str, Any],
    event: dict[str, Any],
    today: date,
) -> bool:
    """Match only an exact, active task with exact scope-bearing source text.

    A title match alone never suppresses creation. The task must also have the
    same deadline, verified executor set, source group, and exact original text
    from a complete source-comment readback.
    """
    if not isinstance(detail, dict):
        raise BusinessCandidateError("business_candidate_detail_incomplete")
    returned_id = detail.get("taskId") or detail.get("todoTaskId")
    if not isinstance(returned_id, str) or returned_id.strip() != task_id:
        raise BusinessCandidateError("business_candidate_task_id_mismatch")
    subject = detail.get("subject")
    if not isinstance(subject, str):
        raise BusinessCandidateError("business_candidate_title_readback_incomplete")
    if normalize_business_text(subject) != normalize_business_text(plan.get("title")):
        return False

    is_done = detail.get("isDone")
    if not isinstance(is_done, bool):
        raise BusinessCandidateError("business_candidate_status_readback_incomplete")
    if is_done:
        return False

    expected_due = _timestamp_ms(plan.get("due_iso"))
    due_value = detail.get("dueTime") or detail.get("due")
    actual_due = _timestamp_ms(due_value)
    if plan.get("due_iso") and expected_due is None:
        raise BusinessCandidateError("business_candidate_expected_deadline_invalid")
    if due_value not in (None, "") and actual_due is None:
        raise BusinessCandidateError("business_candidate_deadline_readback_invalid")
    if expected_due != actual_due:
        return False
    if actual_due is not None:
        deadline_date = datetime.fromtimestamp(
            actual_due / 1000, tz=timezone(timedelta(hours=8))
        ).date()
        if deadline_date < today:
            return False

    expected_names = [str(value).strip() for value in (plan.get("executor_names") or [])]
    expected_ids = [str(value).strip() for value in (plan.get("executor_ids") or [])]
    if not expected_names or len(expected_names) != len(expected_ids):
        raise BusinessCandidateError("business_candidate_expected_executor_set_invalid")
    actual_rows = _executor_rows(detail)
    actual_by_name = {name: user_id for name, user_id in actual_rows}
    if set(actual_by_name) != set(expected_names):
        return False
    if len(set(expected_ids)) != len(expected_ids) or any(not value for value in expected_ids):
        raise BusinessCandidateError("business_candidate_expected_executor_set_invalid")
    actual_ids = [actual_by_name[name] for name in expected_names]
    if actual_ids != expected_ids:
        remote_ids = [value for value in actual_ids if value]
        controlled_fallback = (
            plan.get("allow_executor_namespace_fallback") is True
            and len(remote_ids) == len(actual_ids)
            and len(set(remote_ids)) == len(remote_ids)
        )
        if not controlled_fallback:
            return False

    if not isinstance(comments, list):
        raise BusinessCandidateError("business_candidate_source_comments_incomplete")
    parsed_contexts = [
        context
        for row in comments
        if isinstance(row, dict)
        for context in [source_context_from_comment(row.get("content"))]
        if context is not None
    ]
    if not parsed_contexts:
        raise BusinessCandidateError("business_candidate_source_context_unverified")
    expected_group = normalize_business_text(event.get("group"))
    expected_text = normalize_business_text(event.get("text") or event.get("content"))
    return any(
        normalize_business_text(context["group"]) == expected_group
        and normalize_business_text(context["text"]) == expected_text
        for context in parsed_contexts
    )
