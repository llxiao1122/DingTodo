"""Conservative deadline extraction for an owner-approved source message."""
from __future__ import annotations

import re
from datetime import datetime
from zoneinfo import ZoneInfo

from routing import _DATE_CANDIDATE_RE, extract_due_candidates, has_competing_assessment_cutoff


_CLOCK = re.compile(r'(?<!\d)(?:[01]\d|2[0-3])[:：][0-5]\d(?!\d)')
_UNSUPPORTED = re.compile(r'周|星期|今晚|下班|季度|年底|年前|\d{1,2}\s*(?:点|时)')


def source_deadline(text: str, message_time: str) -> str | None:
    """Return a single explicit deadline; never infer a missing clock or date role."""
    if not isinstance(text, str) or not isinstance(message_time, str):
        return None
    if _UNSUPPORTED.search(text) or has_competing_assessment_cutoff(text):
        return None
    clocks = list(_CLOCK.finditer(text))
    dates = list(_DATE_CANDIDATE_RE.finditer(text))
    if len(clocks) != 1 or len(dates) != 1:
        return None
    between = text[dates[0].end():clocks[0].start()]
    if (clocks[0].start() < dates[0].end() or len(between) > 24
            or re.search(r'[，,；;。！？\n]', between)):
        return None
    try:
        sent = datetime.fromisoformat(message_time.replace('Z', '+00:00'))
        if sent.tzinfo is not None:
            sent = sent.astimezone(ZoneInfo('Asia/Shanghai'))
        candidates = extract_due_candidates(text, sent.date(), include_context=True)
    except (ValueError, TypeError):
        return None
    if len(candidates) != 1 or candidates[0].get('role') != 'deadline':
        return None
    due = candidates[0].get('due_iso')
    clock = clocks[0].group().replace('：', ':')
    if not isinstance(due, str) or due[11:16] != clock:
        return None
    return due
