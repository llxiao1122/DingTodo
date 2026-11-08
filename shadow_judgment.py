"""Ephemeral side-by-side evaluation for the JEV and limited GPT resolvers."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from typing import Any, Protocol


_LIMITED_FIELDS = frozenset({
    "applicability", "followup", "title_span", "due_key", "same_work", "evidence_spans",
})
_JEV_FIELDS = frozenset({
    "applies_to_workgroup", "needs_follow_up", "prospective_action",
    "source_clause_choice", "source_clause_confidence", "source_clause_margin",
    "source_clause_overflow", "source_clause", "due_date_choice",
    "due_date_confidence", "due_date_margin", "date_role_judgments",
})
_FORBIDDEN_AUTHORITY_FIELDS = frozenset({
    "decision", "route", "route_module", "route_owner", "owner", "executor",
    "executor_id", "task_id", "create_todo", "write_authorized",
})


class MainJudgmentResolver(Protocol):
    def resolve(
        self,
        message: dict[str, Any],
        plan: dict[str, Any],
        *,
        date_candidates: list[dict[str, Any]],
        business_candidates: list[dict[str, Any]],
    ) -> dict[str, Any]: ...


class LimitedJudgmentResolver(Protocol):
    def resolve_context(
        self,
        message: dict[str, Any],
        plan: dict[str, Any],
        *,
        date_candidates: list[dict[str, Any]],
        business_candidates: list[dict[str, Any]],
    ) -> dict[str, Any]: ...


def _validate_limited_judgment(
    value: Any,
    *,
    date_candidates: list[dict[str, Any]],
    business_candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != _LIMITED_FIELDS:
        raise ValueError("limited_judgment_schema_invalid")
    if not isinstance(value["applicability"], str) or value["applicability"] not in {
        "applicable", "not_applicable", "unknown",
    }:
        raise ValueError("limited_judgment_applicability_invalid")
    if not isinstance(value["followup"], str) or value["followup"] not in {
        "required", "not_required", "unknown",
    }:
        raise ValueError("limited_judgment_followup_invalid")
    if value["title_span"] is not None and not isinstance(value["title_span"], str):
        raise ValueError("limited_judgment_title_invalid")
    date_keys = {str(candidate.get("key")) for candidate in date_candidates}
    if not isinstance(value["due_key"], str) or value["due_key"] not in date_keys | {"none", "unknown"}:
        raise ValueError("limited_judgment_due_invalid")
    business_keys = {str(candidate.get("key")) for candidate in business_candidates}
    same_work = value["same_work"]
    if (
        not isinstance(same_work, dict)
        or set(same_work) != business_keys
        or any(
            not isinstance(answer, str) or answer not in {"same", "different", "unknown"}
            for answer in same_work.values()
        )
    ):
        raise ValueError("limited_judgment_match_invalid")
    evidence_spans = value["evidence_spans"]
    if (
        not isinstance(evidence_spans, list)
        or not evidence_spans
        or any(not isinstance(span, str) or not span for span in evidence_spans)
    ):
        raise ValueError("limited_judgment_evidence_invalid")
    return deepcopy(value)


def _validate_jev_judgment(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("jev_judgment_schema_invalid")
    if set(value).intersection(_FORBIDDEN_AUTHORITY_FIELDS):
        raise ValueError("jev_judgment_authority_invalid")
    unknown = {
        key for key in value
        if key not in _JEV_FIELDS and not (isinstance(key, str) and key.startswith("same_work_business_"))
    }
    if unknown:
        raise ValueError("jev_judgment_schema_invalid")
    return deepcopy(value)


def _capture_branch(call: Any, validator: Any) -> dict[str, Any]:
    try:
        judgment = validator(call())
    except ValueError:
        return {"status": "invalid", "reason": "judgment_schema_invalid"}
    except Exception:
        return {"status": "error", "reason": "resolver_failed"}
    return {"status": "ok", "judgment": judgment}


def evaluate_limited_judgments_in_shadow(
    message: dict[str, Any],
    plan: dict[str, Any],
    *,
    main_resolver: MainJudgmentResolver,
    limited_resolver: LimitedJudgmentResolver,
    date_candidates: list[dict[str, Any]] | None = None,
    business_candidates: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Call both judgment interfaces on the same isolated snapshot without persistence.

    This helper deliberately has no DWS, SQL, scheduler, or message-send dependency.
    It returns field judgments only and rejects action authority fields from either model.
    """
    if not isinstance(message, dict) or not isinstance(plan, dict):
        raise TypeError("message_and_plan_must_be_objects")
    dates = deepcopy(date_candidates or [])
    businesses = deepcopy(business_candidates or [])
    main_message, limited_message = deepcopy(message), deepcopy(message)
    main_plan, limited_plan = deepcopy(plan), deepcopy(plan)

    def call_main() -> dict[str, Any]:
        return main_resolver.resolve(
            main_message,
            main_plan,
            date_candidates=deepcopy(dates),
            business_candidates=deepcopy(businesses),
        )

    def call_limited() -> dict[str, Any]:
        return limited_resolver.resolve_context(
            limited_message,
            limited_plan,
            date_candidates=deepcopy(dates),
            business_candidates=deepcopy(businesses),
        )

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="judgment-shadow") as executor:
        main_future = executor.submit(
            _capture_branch,
            call_main,
            _validate_jev_judgment,
        )
        limited_future = executor.submit(
            _capture_branch,
            call_limited,
            lambda value: _validate_limited_judgment(
                value,
                date_candidates=dates,
                business_candidates=businesses,
            ),
        )
        main_result = main_future.result()
        limited_result = limited_future.result()

    return {
        "status": "complete" if main_result["status"] == limited_result["status"] == "ok" else "partial",
        "jev_main": main_result,
        "gpt_limited": limited_result,
    }
