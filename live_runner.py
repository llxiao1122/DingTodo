#!/usr/bin/env python3
"""Read-config/read-DWS/plan/ledger runner for the pre-production phase.

Default execution is read-only.  ``--write-ledger`` enables only the bounded
PolarDB evidence/action/cursor writes; it never creates DingTalk todos or sends
group messages.  A later production writer must be a separate explicit phase.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys
from typing import Any

POLARDB_ROOT = Path(os.environ.get("HERMES_POLARDB_RUNTIME", "/root/.hermes/polardb_runtime"))
if str(POLARDB_ROOT) not in sys.path:
    sys.path.insert(0, str(POLARDB_ROOT))

from workflows.polardb_work_adapter.query import WorkQueryAdapter  # noqa: E402
from workflows.polardb_work_writer.writer import WorkWriter  # noqa: E402

from dingtalk_reader import DingTalkReader, DwsReadError  # noqa: E402
from dws_adapter import DwsClient, DwsError  # noqa: E402
from runner import plan_batch  # noqa: E402
from routing import derive_active_routing  # noqa: E402


CHINA_TZ = timezone(timedelta(hours=8))

def _local_date(value: Any) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return str(value or "")[:10]


def _local_naive(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=CHINA_TZ)
    return parsed.astimezone(CHINA_TZ).replace(tzinfo=None)


def calendar_gate(config: dict[str, Any], run_date: date) -> dict[str, Any]:
    rows = [row for row in config.get("config_calendar", []) if _local_date(row.get("dt")) == run_date.isoformat()]
    if len(rows) != 1:
        return {"allowed": False, "reason": "calendar_missing_or_ambiguous", "rows": rows}
    day_type = str(rows[0].get("day_type") or "").strip().lower()
    allowed_types = {"workday", "holiday_shift", "调休", "工作日"}
    return {"allowed": day_type in allowed_types, "day_type": day_type, "row": rows[0]}


def build_planner_config(config: dict[str, Any], run_date: date | None = None) -> dict[str, Any]:
    run_date = run_date or datetime.now(CHINA_TZ).date()
    staff: dict[str, dict[str, Any]] = {}
    for row in config.get("config_staff", []):
        if isinstance(row, dict):
            name = str(row.get("name") or "").strip()
            user_id = str(row.get("user_id") or "").strip()
            role = str(row.get("role") or "").strip()
        else:
            name = str(row[1]).strip() if len(row) > 1 else ""
            user_id = str(row[2]).strip() if len(row) > 2 else ""
            role = str(row[3]).strip() if len(row) > 3 else ""
        if name:
            staff[name] = {"name": name, "user_id": user_id, "role": role}
    assign_to_self_only = os.environ.get("DINGTODO_ASSIGN_SELF_ONLY") == "1"
    if assign_to_self_only:
        derived_active, derived_rules, routing_warnings = False, {}, []
    else:
        derived_active, derived_rules, routing_warnings = derive_active_routing(
            config.get("config_area_duty") or [], run_date
        )
    if "active_routing_loaded" in config:
        active_routing_loaded = bool(config.get("active_routing_loaded"))
        responsibility_rules = dict(config.get("responsibility_rules") or {})
    else:
        active_routing_loaded = derived_active
        responsibility_rules = {}
    if derived_active:
        responsibility_rules.update(derived_rules)
    return {
        "staff": staff,
        "assign_to_self_only": assign_to_self_only,
        "active_routing_loaded": active_routing_loaded,
        "responsibility_rules": responsibility_rules,
        "routing_warnings": routing_warnings,
        "handover_evidence": list(config.get("handover_evidence") or []),
    }


def max_message_cursor(messages: list[dict[str, Any]]) -> tuple[str, str] | None:
    usable = [m for m in messages if m.get("create_time") and m.get("message_id")]
    if not usable:
        return None
    chosen = max(usable, key=lambda m: (_local_naive(m["create_time"]), str(m["message_id"])))
    return _local_naive(chosen["create_time"]).isoformat(sep=" "), str(chosen["message_id"])


def read_all(adapter: WorkQueryAdapter, view: str, **filters: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    page_token = None
    while True:
        request = {"view": view, "limit": 100, **filters}
        if page_token:
            request["page_token"] = page_token
        result = adapter.query(request)
        rows.extend(result.get("rows") or [])
        if not result.get("has_more"):
            return rows
        page_token = result.get("next_page_token")
        if not page_token:
            raise RuntimeError(f"{view}_missing_next_page_token")


def group_specs(config: dict[str, Any], scan_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    state_by_conv = {row.get("conversation_id"): row for row in scan_rows if row.get("conversation_id")}
    specs: list[dict[str, Any]] = []
    for row in config.get("config_group", []):
        if str(row.get("purpose") or "") != "todo_scan":
            continue
        conversation_id = str(row.get("conversation_id") or "")
        state = state_by_conv.get(conversation_id)
        configured_name = str(row.get("group_name") or "")
        effective_name = str((state or {}).get("group_name") or configured_name)
        warnings = []
        if state and str(state.get("group_name") or "") != configured_name:
            warnings.append("group_name_config_mismatch")
        specs.append(
            {
                "group_name": effective_name,
                "configured_group_name": configured_name,
                "state_group_name": str((state or {}).get("group_name") or ""),
                "conversation_id": conversation_id,
                "last_msg_time": (state or {}).get("last_msg_time"),
                "last_msg_id": (state or {}).get("last_msg_id"),
                "warnings": warnings,
            }
        )
    return specs


def _source_key(plan: dict[str, Any]) -> str:
    return str(plan.get("source_key") or "")


def run_cycle(*, run_date: date, write_ledger: bool, lookback_hours: int = 24) -> dict[str, Any]:
    adapter = WorkQueryAdapter()
    runtime = adapter.query({"view": "runtime_config"})
    config = runtime.get("config") or {}
    gate = calendar_gate(config, run_date)
    if not gate["allowed"]:
        return {
            "status": "SKIPPED",
            "reason": gate["reason"],
            "calendar": gate,
            "groups_attempted": 0,
            "messages": 0,
            "writes": 0,
            "external_todos_created": 0,
            "group_messages_sent": 0,
        }

    dws = DwsClient()
    self_user = dws.get_self()
    scan_rows = read_all(adapter, "scan_state")
    existing_actions = read_all(adapter, "actions")
    specs = group_specs(config, scan_rows)
    reader = DingTalkReader()
    all_messages: list[dict[str, Any]] = []
    group_reports: list[dict[str, Any]] = []
    complete_specs: list[dict[str, Any]] = []
    end = datetime.now(CHINA_TZ)

    for spec in specs:
        try:
            read = reader.read_group(
                conversation_id=spec["conversation_id"],
                group_name=spec["group_name"],
                last_msg_time=spec.get("last_msg_time"),
                end=end,
            )
        except (DwsReadError, OSError, TimeoutError) as exc:
            group_reports.append({**spec, "complete": False, "error": str(exc)})
            continue
        report = {
            **spec,
            "complete": bool(read.get("complete")),
            "message_count": len(read.get("messages") or []),
            "read_start": read.get("start"),
            "read_end": read.get("end"),
        }
        group_reports.append(report)
        if not read.get("complete"):
            continue
        complete_specs.append(spec)
        all_messages.extend(read.get("messages") or [])

    unique: dict[tuple[str, str], dict[str, Any]] = {}
    for message in all_messages:
        key = (message.get("conversation_id", ""), message.get("message_id", ""))
        if all(key):
            unique[key] = message
    messages = list(unique.values())
    payload = {
        "config": build_planner_config(config, run_date=run_date),
        "self_user": self_user,
        "existing_actions": existing_actions,
        "messages": messages,
    }
    planner_result = plan_batch(payload, mode="test", run_date=run_date)

    for plan in planner_result["plans"]:
        spec = next((s for s in specs if s["conversation_id"] == plan.get("conversation_id")), None)
        if spec and spec.get("warnings"):
            plan["warnings"] = list(plan.get("warnings") or []) + spec["warnings"]
            if plan.get("decision") == "CREATE":
                plan["decision"] = "REVIEW"

    writes = 0
    ledger_counts = {
        "message_inserted": 0,
        "message_already_present": 0,
        "actions_created": 0,
        "actions_already_present": 0,
        "cursors_changed": 0,
        "cursors_unchanged": 0,
    }
    write_errors: list[dict[str, Any]] = []
    if write_ledger:
        os.environ.setdefault("HERMES_WORK_WRITER_ENV", "/etc/cipher-work.env")
        writer = WorkWriter()
        plan_by_source = {_source_key(plan): plan for plan in planner_result["plans"]}
        for message in messages:
            try:
                message_result = writer.execute(
                    {
                        "operation": "record_message",
                        "conversation_id": message["conversation_id"],
                        "message_id": message["message_id"],
                        "group_name": message["group"],
                        "sender_id": message.get("sender_id"),
                        "sender_name": message.get("sender"),
                        "message_time": message["create_time"],
                        "text": message.get("text"),
                        "msg_type": message.get("msg_type"),
                        "resource_refs": message.get("resource_refs"),
                        "quoted_message": message.get("quoted_message"),
                        "raw_payload": message.get("raw_payload"),
                        "source": "hermes",
                    }
                )
                if message_result.get("created"):
                    ledger_counts["message_inserted"] += 1
                else:
                    ledger_counts["message_already_present"] += 1
                writes += 1
                plan = plan_by_source.get(f"{message['conversation_id']}:{message['message_id']}:todo")
                if plan and plan.get("business_key") and plan.get("decision") in {"CREATE", "REVIEW"}:
                    action_result = writer.execute(
                        {
                            "operation": "claim_action",
                            "conversation_id": message["conversation_id"],
                            "message_id": message["message_id"],
                            "action_key": "todo",
                            "business_key": plan["business_key"],
                            "status": "CLAIMED" if plan["decision"] == "CREATE" else "REVIEW",
                        }
                    )
                    if action_result.get("created"):
                        ledger_counts["actions_created"] += 1
                    else:
                        ledger_counts["actions_already_present"] += 1
                    writes += 1
            except Exception as exc:
                write_errors.append({"message_id": message.get("message_id"), "error": str(exc)})

        # Advance only groups whose read and all message writes completed.
        failed_messages = {item.get("message_id") for item in write_errors}
        for spec in complete_specs:
            if spec.get("warnings"):
                continue
            group_messages = [m for m in messages if m.get("conversation_id") == spec["conversation_id"]]
            if not group_messages or any(m.get("message_id") in failed_messages for m in group_messages):
                continue
            cursor = max_message_cursor(group_messages)
            if not cursor:
                continue
            try:
                cursor_result = writer.execute(
                    {
                        "operation": "advance_scan_state",
                        "group_name": spec["configured_group_name"],
                        "conversation_id": spec["conversation_id"],
                        "last_msg_time": cursor[0],
                        "last_msg_id": cursor[1],
                    }
                )
                if cursor_result.get("changed"):
                    ledger_counts["cursors_changed"] += 1
                else:
                    ledger_counts["cursors_unchanged"] += 1
                writes += 1
            except Exception as exc:
                write_errors.append({"group_name": spec["configured_group_name"], "error": str(exc)})

    return {
        "status": "COMPLETED" if not write_errors else "REVIEW",
        "calendar": gate,
        "groups_attempted": len(specs),
        "groups_complete": sum(bool(r.get("complete")) for r in group_reports),
        "messages": len(messages),
        "planner_summary": planner_result["summary"],
        "llm_queue": len(planner_result["llm_queue"]),
        "writes": writes,
        "ledger_counts": ledger_counts,
        "write_errors": write_errors,
        "external_todos_created": 0,
        "group_messages_sent": 0,
        "group_reports": group_reports,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default=datetime.now(CHINA_TZ).date().isoformat())
    parser.add_argument("--lookback-hours", type=int, default=24)
    parser.add_argument("--write-ledger", action="store_true", help="commit only PolarDB evidence/action/cursor writes")
    args = parser.parse_args(argv)
    result = run_cycle(
        run_date=date.fromisoformat(args.run_date),
        write_ledger=args.write_ledger,
        lookback_hours=args.lookback_hours,
    )
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2, default=str)
    sys.stdout.write("\n")
    return 0 if result.get("status") != "REVIEW" or not result.get("write_errors") else 2


if __name__ == "__main__":
    raise SystemExit(main())
