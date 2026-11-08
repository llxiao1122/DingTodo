#!/usr/bin/env python3
"""Local Hermes auto-todo planner and guarded DWS writer.

Default mode is a read-only test plan.  The test path never calls DWS writes
and always places only the current user in the actual executor list.  A future
production invocation must explicitly pass --mode production --write; even
then this module has no group-send operation.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import json
import sys
from typing import Any

from dws_adapter import DwsClient, DwsError, verify_todo
from routing import plan_message


SHANGHAI = timezone(timedelta(hours=8))


def normalize_config(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize PolarDB adapter output without trusting its ID namespace."""
    if isinstance(raw.get("staff"), dict):
        normalized: dict[str, Any] = {"staff": raw["staff"]}
        for key in ("active_routing_loaded", "responsibility_rules", "handover_evidence", "assign_to_self_only"):
            if key in raw:
                normalized[key] = raw[key]
        return normalized
    staff: dict[str, dict[str, Any]] = {}
    rows = raw.get("config_staff") or []
    for row in rows:
        if isinstance(row, dict):
            name = str(row.get("name") or row.get("staff_name") or "").strip()
            user_id = str(row.get("user_id") or "").strip()
            role = str(row.get("role") or "").strip()
        else:
            name = str(row[1]).strip() if len(row) > 1 else ""
            user_id = str(row[2]).strip() if len(row) > 2 else ""
            role = str(row[3]).strip() if len(row) > 3 else ""
        if name:
            staff[name] = {"name": name, "user_id": user_id, "role": role}
    normalized: dict[str, Any] = {"staff": staff}
    for key in ("active_routing_loaded", "responsibility_rules", "handover_evidence", "assign_to_self_only"):
        if key in raw:
            normalized[key] = raw[key]
    return normalized


def _parse_run_date(value: str | None) -> date:
    if value:
        return date.fromisoformat(value)
    return datetime.now(SHANGHAI).date()


def plan_batch(
    payload: dict[str, Any],
    *,
    mode: str = "test",
    run_date: date | None = None,
    live_ids: dict[str, str] | None = None,
) -> dict[str, Any]:
    run_date = run_date or datetime.now(SHANGHAI).date()
    config = normalize_config(payload.get("config") or {})
    self_user = payload.get("self_user") or {}
    messages = payload.get("messages") or []
    existing_actions = list(payload.get("existing_actions") or [])
    plans: list[dict[str, Any]] = []
    seen_actions = list(existing_actions)

    for message in messages:
        plan = plan_message(
            message,
            config,
            self_user,
            run_date=run_date,
            mode=mode,
            live_ids=live_ids,
            existing_actions=seen_actions,
            allow_group_send=False,
        )
        plans.append(plan)
        if plan.get("decision") == "CREATE":
            seen_actions.append(
                {
                    "source_key": plan.get("source_key"),
                    "business_key": plan.get("business_key"),
                }
            )

    summary = {
        "messages": len(messages),
        "create": sum(p.get("decision") == "CREATE" for p in plans),
        "review": sum(p.get("decision") == "REVIEW" for p in plans),
        "ignore": sum(p.get("decision") == "IGNORE" for p in plans),
        "writes_performed": 0,
        "group_messages_sent": 0,
    }
    llm_warnings = {"title_needs_llm", "semantic_intent_needs_llm"}
    llm_queue = [
        {
            "source_key": p.get("source_key"),
            "message_id": p.get("message_id"),
            "reason": next(
                warning
                for warning in (p.get("warnings") or [])
                if warning in llm_warnings
            ),
        }
        for p in plans
        if any(warning in llm_warnings for warning in (p.get("warnings") or []))
    ]
    return {
        "router_version": "iron-west-responsibility-v1",
        "mode": mode,
        "run_date": run_date.isoformat(),
        "plans": plans,
        "llm_queue": llm_queue,
        "summary": summary,
    }


def _production_write(payload: dict[str, Any], run_date: date) -> dict[str, Any]:
    """Fail closed: this legacy entry point has no durable pre-create ledger handoff."""
    result = plan_batch(payload, mode="test", run_date=run_date)
    blocked = []
    for plan in result["plans"]:
        if plan.get("decision") == "CREATE":
            plan["decision"] = "REVIEW"
            plan["reason"] = "durable_create_ledger_unavailable"
            plan["warnings"] = list(plan.get("warnings") or []) + ["production_write_blocked_no_precreate_ledger"]
            blocked.append(plan.get("source_key"))
    result["summary"]["create"] = sum(plan.get("decision") == "CREATE" for plan in result["plans"])
    result["summary"]["review"] = sum(plan.get("decision") == "REVIEW" for plan in result["plans"])
    result["summary"]["writes_performed"] = 0
    result["summary"]["group_messages_sent"] = 0
    result["errors"] = [
        {"source_key": key, "error": "production_write_blocked_no_precreate_ledger"}
        for key in blocked
    ]
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("test", "production"), default="test")
    parser.add_argument("--write", action="store_true", help="explicitly enable production DWS writes")
    parser.add_argument("--run-date")
    args = parser.parse_args(argv)

    payload = json.load(sys.stdin)
    run_date = _parse_run_date(args.run_date)
    if args.mode == "production" and args.write:
        output = _production_write(payload, run_date)
    else:
        output = plan_batch(payload, mode="test", run_date=run_date)
        output["execution_note"] = "test mode: no DWS or group-message writes"
    json.dump(output, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
