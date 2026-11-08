"""Shared idempotency boundary for claimed DingTalk todo writes."""
from __future__ import annotations

from collections.abc import Callable
from typing import Any


class TodoExecutionError(RuntimeError):
    """A claimed todo write cannot safely continue or be retried."""

    def __init__(self, message: str, *, task_id: str | None = None) -> None:
        super().__init__(message)
        self.task_id = task_id


def _stable_task_id(value: Any) -> str:
    if isinstance(value, dict):
        value = value.get("task_id") or value.get("taskId") or value.get("todoTaskId")
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise TodoExecutionError("create_response_task_id_missing")
    task_id = str(value).strip()
    if not task_id or len(task_id) > 128:
        raise TodoExecutionError("create_response_task_id_invalid")
    return task_id


def create_claimed_task(
    *,
    action_id: Any | None,
    claim_acquired: bool,
    create: Callable[[], Any],
    link: Callable[[str, str], None] | None,
    test_only: bool = False,
) -> str:
    """Create once after a durable claim and link the returned ID immediately."""
    if test_only is True:
        if action_id is not None or claim_acquired is not False or link is not None:
            raise TodoExecutionError("test_todo_creation_must_not_claim_or_link")
    else:
        if isinstance(action_id, bool) or action_id in (None, "") or not str(action_id).strip():
            raise TodoExecutionError("durable_create_claim_action_id_missing")
        if claim_acquired is not True:
            raise TodoExecutionError("durable_create_claim_not_acquired")
        if not callable(link):
            raise TodoExecutionError("durable_create_claim_link_required")

    task_id = _stable_task_id(create())
    if not test_only:
        try:
            link(str(action_id), task_id)
        except Exception as exc:
            raise TodoExecutionError("create_link_outcome_unknown", task_id=task_id) from exc
    return task_id


def add_verified_source_comment(
    dws: Any,
    task_id: str,
    content: str,
    *,
    already_attempted: bool = False,
    check_existing: bool = True,
    on_attempt: Callable[[], None] | None = None,
    on_verified: Callable[[], None] | None = None,
) -> bool:
    """Write one source comment at most once, reconciling exact existing content."""
    normalized_task_id = str(task_id or "").strip()
    if not normalized_task_id or normalized_task_id != str(task_id):
        raise TodoExecutionError("source_comment_task_id_invalid")
    if not isinstance(content, str) or not content.strip():
        raise TodoExecutionError("source_comment_content_missing")

    if check_existing:
        rows = dws.list_task_comments(normalized_task_id)
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise TodoExecutionError("source_comment_readback_invalid")
        if any(row.get("content") == content for row in rows):
            dws.verify_source_comment(normalized_task_id, content)
            if on_verified:
                on_verified()
            return False

    if already_attempted:
        raise TodoExecutionError("source_comment_outcome_unknown")
    if on_attempt:
        on_attempt()
    dws.add_source_comment(normalized_task_id, content)
    dws.verify_source_comment(normalized_task_id, content)
    if on_verified:
        on_verified()
    return True
