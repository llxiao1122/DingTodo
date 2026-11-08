"""Strict loader for one-source, local human deadline evidence."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import json
from pathlib import Path
import re
from typing import Any


_SOURCE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
_UTC_PLUS_8 = timedelta(hours=8)


@dataclass(frozen=True)
class DeadlineClarification:
    source_message_id: str
    source_time: str
    source_evidence: str
    user_confirmation: str
    confirmed_due_date: str
    confirmed_due_iso: str

    def matches(self, event: dict[str, Any]) -> bool:
        """Join only the exact source identity/time and quoted source text."""
        return (
            str(event.get("message_id") or "") == self.source_message_id
            and str(event.get("create_time") or "") == self.source_time
            and self.source_evidence in str(event.get("text") or event.get("content") or "")
        )

    def public_evidence(self) -> dict[str, str]:
        """Return local provenance without copying the external message ID."""
        return {
            "provenance": "user_confirmation",
            "source_time": self.source_time,
            "source_evidence": self.source_evidence,
            "user_confirmation": self.user_confirmation,
            "confirmed_due_date": self.confirmed_due_date,
            "confirmed_due_iso": self.confirmed_due_iso,
        }


def _require_text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("deadline_clarification_" + name + "_missing")
    if value != value.strip():
        raise ValueError("deadline_clarification_" + name + "_invalid")
    return value


def _has_430_completion_consequence(quote: str) -> bool:
    return bool(
        re.search(
            r"4点30分.{0,16}仍未更新的工班.{0,16}纳入本月考核项",
            quote,
        )
    )


def _validate_confirmed_datetime(raw_date: Any, raw_iso: Any, source_time: str) -> tuple[str, str]:
    due_date_text = _require_text(raw_date, "due_date")
    due_iso = _require_text(raw_iso, "due_iso")
    try:
        due_date = date.fromisoformat(due_date_text)
    except ValueError as exc:
        raise ValueError("deadline_clarification_due_date_invalid") from exc
    if due_date.isoformat() != due_date_text:
        raise ValueError("deadline_clarification_due_date_invalid")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?\+08:00", due_iso):
        raise ValueError("deadline_clarification_due_iso_must_be_utc_plus_8")
    try:
        due_at = datetime.fromisoformat(due_iso)
        source_at = datetime.strptime(source_time, _SOURCE_TIME_FORMAT)
    except ValueError as exc:
        raise ValueError("deadline_clarification_datetime_invalid") from exc
    if due_at.utcoffset() != _UTC_PLUS_8:
        raise ValueError("deadline_clarification_due_iso_must_be_utc_plus_8")
    if due_at.date() != due_date or due_date != source_at.date():
        raise ValueError("deadline_clarification_local_date_mismatch")
    if (due_at.hour, due_at.minute, due_at.second, due_at.microsecond) != (16, 30, 0, 0):
        raise ValueError("deadline_clarification_due_time_mismatch")
    return due_date.isoformat(), due_iso


def load_deadline_clarification(path: str | Path | None) -> DeadlineClarification | None:
    """Load the single bounded human record; reject malformed or widened input."""
    if path is None:
        return None
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("deadline_clarification_file_invalid") from exc
    if not isinstance(raw, dict):
        raise ValueError("deadline_clarification_record_invalid")
    if raw.get("kind") != "human_deadline_clarification":
        raise ValueError("deadline_clarification_kind_invalid")
    if raw.get("scope") != "one source obligation only; local evaluation evidence, not production activation or generalized message-ID policy":
        raise ValueError("deadline_clarification_scope_invalid")

    source_message_id = _require_text(raw.get("source_message_id"), "source_message_id")
    source_time = _require_text(raw.get("source_time"), "source_time")
    source_evidence = _require_text(raw.get("source_evidence"), "source_evidence")
    confirmation = _require_text(raw.get("user_confirmation"), "user_confirmation")
    try:
        datetime.strptime(source_time, _SOURCE_TIME_FORMAT)
    except ValueError as exc:
        raise ValueError("deadline_clarification_source_time_invalid") from exc
    if not _has_430_completion_consequence(source_evidence):
        raise ValueError("deadline_clarification_source_evidence_invalid")
    normalized_confirmation = confirmation.replace("：", ":")
    if not re.search(r"16\s*:\s*30|16点30分|16时30分", normalized_confirmation) or not re.search(
        r"完成(?:截止|期限)|作为.{0,8}截止",
        confirmation,
    ):
        raise ValueError("deadline_clarification_confirmation_invalid")
    due_date, due_iso = _validate_confirmed_datetime(
        raw.get("confirmed_due_date"), raw.get("confirmed_due_iso"), source_time
    )
    return DeadlineClarification(
        source_message_id=source_message_id,
        source_time=source_time,
        source_evidence=source_evidence,
        user_confirmation=confirmation,
        confirmed_due_date=due_date,
        confirmed_due_iso=due_iso,
    )
