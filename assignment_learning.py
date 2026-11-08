"""Read-only comparison for explicitly injected, sanitized assignment samples.

This module does not read DWS, the database, messages, or the historical task archive,
and it has no writer or label-update path.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable

from assignment_policy import load_contract


_SAMPLE_FIELDS = frozenset({
    "task_id", "split", "confirmed_rule_id", "predicted_rule_id",
    "confirmed_primary_owner", "predicted_primary_owner",
})
_SPLITS = frozenset({"injected", "holdout"})


class InvalidAssignmentSample(ValueError):
    pass


def compare_sanitized_samples(samples: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Compare labels supplied by a host reviewer without returning sample IDs or writing labels."""
    rows = list(samples)
    if len(rows) > 500:
        raise InvalidAssignmentSample("sample_limit_exceeded")
    members = set(load_contract()["members"])
    rule_ids = {"A", "B", "C", "D", "E", "F", "G"}
    grouped: dict[str, dict[str, int]] = defaultdict(lambda: {
        "samples": 0, "rule_agreements": 0, "owner_agreements": 0,
    })
    for row in rows:
        if not isinstance(row, dict) or set(row) != _SAMPLE_FIELDS:
            raise InvalidAssignmentSample("sample_fields_invalid")
        task_id = row.get("task_id")
        if not isinstance(task_id, str) or not task_id.strip() or len(task_id) > 128:
            raise InvalidAssignmentSample("task_id_invalid")
        split = row.get("split")
        if split not in _SPLITS:
            raise InvalidAssignmentSample("split_invalid")
        for field in ("confirmed_rule_id", "predicted_rule_id"):
            value = row.get(field)
            if value is not None and value not in rule_ids:
                raise InvalidAssignmentSample(f"{field}_invalid")
        for field in ("confirmed_primary_owner", "predicted_primary_owner"):
            value = row.get(field)
            if value is not None and value not in members:
                raise InvalidAssignmentSample(f"{field}_invalid")
        bucket = grouped[split]
        bucket["samples"] += 1
        bucket["rule_agreements"] += int(row["confirmed_rule_id"] == row["predicted_rule_id"])
        bucket["owner_agreements"] += int(row["confirmed_primary_owner"] == row["predicted_primary_owner"])
    return {
        "mode": "read_only_comparison",
        "writes_labels": False,
        "sample_count": len(rows),
        "splits": {key: grouped[key] for key in sorted(grouped)},
    }
