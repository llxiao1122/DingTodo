"""Causal, read-only per-day event replay primitives."""
from __future__ import annotations

import copy
import hashlib
import json
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Callable

from event_worker import COMPLETE_DWS_PAGE_ORDER, _local_datetime


def review_retry_allowed(action: dict[str, Any], evidence_fingerprint: str, *, max_attempts: int = 2) -> bool:
    """Allow bounded in-memory rejudgment only for a REVIEW with new evidence."""
    if str(action.get("status", "")).upper() not in {"REVIEW", "NEEDS_REVIEW"}:
        return False
    if action.get("external_task_id") or str(action.get("status", "")).upper() in {"CLAIMED", "EXTERNAL_CREATED", "FAILED"}:
        return False
    try:
        attempts = int(action.get("review_attempts", 0))
    except (TypeError, ValueError):
        return False
    return attempts < max(0, int(max_attempts)) and bool(evidence_fingerprint) and evidence_fingerprint != action.get("review_evidence_fingerprint")


def _has_known_initial_timestamp(action: dict[str, Any]) -> bool:
    initial = action.get("created_at") or action.get("message_time") or action.get("create_time")
    if not initial:
        return False
    try:
        _local_datetime(initial)
        return True
    except (TypeError, ValueError):
        return False


def _action_is_visible_on_day(action: dict[str, Any], source_day: date) -> bool:
    """Only include actions with known initial state strictly before source-day midnight."""
    initial = action.get("created_at") or action.get("message_time") or action.get("create_time")
    if not initial:
        return False
    try:
        return _local_datetime(initial) < datetime.combine(source_day, time.min)
    except (TypeError, ValueError):
        return False


def replay_day(payload: dict[str, Any], *, resolver: Callable[[dict[str, Any], list[dict[str, Any]]], dict[str, Any]]) -> dict[str, Any]:
    """Replay source_day messages by timestamp, preserving input order for ties."""
    source_day = date.fromisoformat(str(payload["source_day"]))
    initial_state_incomplete = bool(payload.get("initial_state_incomplete")) or any(
        isinstance(action, dict)
        and not _has_known_initial_timestamp(action)
        for action in (payload.get("existing_actions") or [])
    )
    actions = copy.deepcopy(payload.get("existing_actions") or [])
    actions = [
        action for action in actions
        if not isinstance(action, dict) or _action_is_visible_on_day(action, source_day)
    ]
    valid = []
    for raw in payload.get("messages") or []:
        if not isinstance(raw, dict) or not raw.get("create_time") or not raw.get("message_id"):
            continue
        if _local_datetime(raw["create_time"]).date() == source_day:
            valid.append(copy.deepcopy(raw))
    # Python's sort is stable: equal timestamps retain the collector's order.
    # Message IDs and conversation IDs do not establish arrival sequence.
    valid.sort(key=lambda m: _local_datetime(m["create_time"]))
    plans = []
    retry_state: dict[tuple[str, str], dict[str, Any]] = {}
    for message in valid:
        resolver_actions = copy.deepcopy(actions)
        linked_id = next((str(message.get(key)) for key in ("reply_to_message_id", "quoted_message_id", "related_message_id") if message.get(key)), None)
        if linked_id and message.get("conversation_id"):
            identity = (str(message["conversation_id"]), linked_id)
            matched = next((a for a in resolver_actions if isinstance(a, dict) and str(a.get("conversation_id", "")) == identity[0] and str(a.get("message_id", "")) == identity[1]), None)
            if matched and str(matched.get("status", "")).upper() in {"REVIEW", "NEEDS_REVIEW"}:
                # The hash is computed from arrived, explicitly linked source evidence, never model output.
                evidence = sorted((m for m in valid if str(m.get("conversation_id", "")) == identity[0]
                                   and str(next((m.get(k) for k in ("reply_to_message_id", "quoted_message_id", "related_message_id") if m.get(k)), "")) == identity[1]
                                   and _local_datetime(m["create_time"]) <= _local_datetime(message["create_time"])),
                                  key=lambda m: (_local_datetime(m["create_time"]), str(m["message_id"])))
                encoded = json.dumps(evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode()
                fingerprint = hashlib.sha256(encoded).hexdigest()
                state = retry_state.setdefault(identity, copy.deepcopy(matched))
                if review_retry_allowed(state, fingerprint):
                    state["review_attempts"] = int(state.get("review_attempts", 0)) + 1
                    state["review_evidence_fingerprint"] = fingerprint
                    resolver_actions = [a for a in resolver_actions if not (isinstance(a, dict) and str(a.get("conversation_id", "")) == identity[0] and str(a.get("message_id", "")) == identity[1])]
        result = resolver(copy.deepcopy(message), resolver_actions)
        plan = {"message_id": message["message_id"], **(result if isinstance(result, dict) else {"decision": "REVIEW"})}
        plans.append(plan)
        if plan.get("decision") == "CREATE":
            actions.append({
                "conversation_id": message.get("conversation_id"),
                "message_id": message["message_id"],
                "source_key": plan.get("source_key"),
                "business_key": plan.get("business_key"),
                "status": "SUCCESS",
                "simulation_id": "simulated:" + str(message["message_id"]),
                "source_text": message.get("text"),
                "message_time": message.get("create_time"),
                "route_module": plan.get("route_module"),
                "route_owner": plan.get("route_owner"),
                "simulated": True,
            })
            actions[-1]["conversation_id"] = message.get("conversation_id")
    provenance = payload.get("source_order_provenance")
    provenance = provenance if provenance == COMPLETE_DWS_PAGE_ORDER else "unverified_input_order"
    return {"source_day": source_day.isoformat(), "plans": plans, "review_retry_state": list(retry_state.values()), "simulated_ledger": [a for a in actions if a.get("simulated")], "summary": {"messages": len(valid), "create": sum(p.get("decision") == "CREATE" for p in plans), "review": sum(p.get("decision") == "REVIEW" for p in plans), "ignore": sum(p.get("decision") == "IGNORE" for p in plans), "initial_state_incomplete": initial_state_incomplete, "arrival_order_provenance": provenance, "writes_performed": 0, "external_todos_created": 0, "group_messages_sent": 0}}


def replay_event_worker_day(payload: dict[str, Any], *, resolver: Any = None, semantic_fallback: Any = None, date_role_fallback_resolver: Any = None, title_resolver: Any = None, dws: Any = None, writer: Any = None, deadline_clarifications_path: str | Path | None = None) -> dict[str, Any]:
    """Invoke production planner/resolver logic in guarded, memory-only shadow mode."""
    from event_worker import EventProcessor
    processor = EventProcessor(payload.get("config") or {}, payload.get("self_user") or {}, mode="shadow", dws=dws, writer=writer, write_ledger=False, allow_external_writes=False, llm_resolver=resolver, semantic_fallback=semantic_fallback, date_role_fallback_resolver=date_role_fallback_resolver, title_resolver=title_resolver, allow_simulated_candidates=True, existing_actions=copy.deepcopy(payload.get("existing_actions") or []), business_candidate_query_complete=payload.get("business_candidate_query_complete"), source_order_provenance=payload.get("source_order_provenance"), deadline_clarifications_path=deadline_clarifications_path)
    def resolve_with_processor(msg: dict[str, Any], acts: list[dict[str, Any]]) -> dict[str, Any]:
        processor.existing_actions = copy.deepcopy(acts)
        return processor.process_batch([msg])[0]


    result = replay_day(
        {**payload, "existing_actions": processor.existing_actions},
        resolver=resolve_with_processor,
    )
    result["simulated_ledger"] = [{k: v for k, v in a.items() if k != "external_task_id"} for a in result.get("simulated_ledger", [])]
    result["summary"].update({
        "llm_calls": processor.stats["llm_calls"],
        "semantic_fallback_calls": processor.stats["semantic_fallback_calls"],
        "semantic_fallback_succeeded": processor.stats["semantic_fallback_succeeded"],
        "semantic_fallback_failed": processor.stats["semantic_fallback_failed"],
        "semantic_fallback_latency_ms_total": processor.stats["semantic_fallback_latency_ms_total"],
        "date_role_fallback_calls": processor.stats["date_role_fallback_calls"],
        "date_role_fallback_succeeded": processor.stats["date_role_fallback_succeeded"],
        "date_role_fallback_failed": processor.stats["date_role_fallback_failed"],
        "date_role_fallback_latency_ms_total": processor.stats["date_role_fallback_latency_ms_total"],
        "title_field_calls": processor.stats["title_field_calls"],
        "title_field_succeeded": processor.stats["title_field_succeeded"],
        "title_field_failed": processor.stats["title_field_failed"],
        "arrived_context_messages": processor.stats["arrived_context_messages"],
        "self_authored_pure_mention_ignores": processor.stats["self_authored_pure_mention_ignores"],
        "self_notice_artifact_ignores": processor.stats["self_notice_artifact_ignores"],
        "human_clarified_sources": processor.stats["human_clarified_sources"],
        "arrival_order_provenance": processor.stats["arrival_order_provenance"],
        "writes_performed": 0,
        "ledger_writes": 0,
        "external_todos_created": 0,
        "group_messages_sent": 0,
    })
    return result
