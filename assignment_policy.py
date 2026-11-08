"""Validated local contract for the user's confirmed A-G task routing rules."""

from __future__ import annotations

from datetime import date
import calendar
import json
from functools import lru_cache
from pathlib import Path
import re
from typing import Any


CONTRACT_PATH = Path(__file__).with_name("confirmed_assignment_rules.json")
EXPECTED_RULE_IDS = frozenset({"A", "B", "C", "D", "E", "F", "G"})
ALLOWED_EVIDENCE_SOURCES = frozenset({
    "dws_readback",
    "reviewed_area_duty",
    "user_confirmed_month_anchor",
})
NOTICE_TERMS = ("通知", "传达", "知会", "通报")
ASSIGNMENT_VERBS = ("承办", "办理", "执行", "负责", "责任人", "承办人", "执行人")


class AssignmentContractError(ValueError):
    """The local rule document is malformed or violates its fixed owner contract."""


def validate_contract(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise AssignmentContractError("contract_must_be_object")
    if payload.get("schema_version") != 1:
        raise AssignmentContractError("contract_schema_version_unsupported")
    if not isinstance(payload.get("contract_version"), str) or not payload["contract_version"].strip():
        raise AssignmentContractError("contract_version_missing")
    if payload.get("source") != "rule-driven-task-routing/references/confirmed-assignment-habits.md":
        raise AssignmentContractError("contract_source_mismatch")
    members = payload.get("members")
    if members != ["李林骁", "陈红洁", "张志斌", "杨梦卓", "苗笑天", "谭继衡"]:
        raise AssignmentContractError("contract_members_mismatch")
    rules = payload.get("rules")
    if not isinstance(rules, dict) or set(rules) != EXPECTED_RULE_IDS:
        raise AssignmentContractError("contract_rule_set_mismatch")
    expected_static_owners = {"A": "陈红洁", "C": "苗笑天"}
    for rule_id, expected_owner in expected_static_owners.items():
        rule = rules.get(rule_id)
        if not isinstance(rule, dict) or rule.get("primary_owner") != expected_owner:
            raise AssignmentContractError(f"contract_owner_mismatch:{rule_id}")
        if rule.get("collaborators") != ["李林骁"]:
            raise AssignmentContractError(f"contract_collaborator_mismatch:{rule_id}")
    if rules["B"].get("dynamic_area") != "材料棚及办公用房":
        raise AssignmentContractError("contract_area_mismatch:B")
    if rules["F"].get("dynamic_area") != "杂品库":
        raise AssignmentContractError("contract_area_mismatch:F")
    if rules["D"].get("awareness_names") != members:
        raise AssignmentContractError("contract_awareness_members_mismatch")
    if rules["D"].get("requires_local_scope") is not True:
        raise AssignmentContractError("contract_awareness_scope_gate_missing")
    if rules["E"].get("manual_assignment_required") is not True:
        raise AssignmentContractError("contract_manual_gate_missing:E")
    if rules["G"].get("manual_assignment_required") is not True:
        raise AssignmentContractError("contract_manual_gate_missing:G")
    anchors = payload.get("monthly_anchors")
    if not isinstance(anchors, dict) or anchors.get("2026-10") != {
        "杂品库": "苗笑天",
        "中小件区": "杨梦卓",
        "材料棚及办公用房": "张志斌",
        "大件库区": "谭继衡",
    }:
        raise AssignmentContractError("contract_october_anchor_mismatch")
    for rule in rules.values():
        if not isinstance(rule, dict) or not isinstance(rule.get("patterns"), list):
            raise AssignmentContractError("contract_patterns_invalid")
        if not rule["patterns"] or any(
            not isinstance(pattern, dict)
            or not isinstance(pattern.get("all"), list)
            or not pattern["all"]
            or any(not isinstance(term, str) or not term for term in pattern["all"])
            for pattern in rule["patterns"]
        ):
            raise AssignmentContractError("contract_pattern_invalid")
    return payload


@lru_cache(maxsize=1)
def load_contract() -> dict[str, Any]:
    try:
        payload = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        raise AssignmentContractError("contract_unavailable_or_invalid_json") from None
    return validate_contract(payload)


def _pattern_matches(text: str, patterns: list[dict[str, Any]]) -> bool:
    return any(all(term in text for term in pattern["all"]) for pattern in patterns)


def classify_rule(
    text: str,
    *,
    source_group: str,
    local_scope: bool,
) -> str | None:
    contract = load_contract()
    rules = contract["rules"]
    # Manual-allocation categories win over generic area/module keywords.
    for rule_id in ("E", "G", "A", "C", "B", "F", "D"):
        rule = rules[rule_id]
        if not _pattern_matches(text, rule["patterns"]):
            continue
        if rule_id == "D":
            if not local_scope or not any(term in text for term in rule["notice_terms"]):
                continue
            if not any(term in text for term in ("各工班", "工班全员", "工班成员", "鐵炉西工班", "铁炉西工班")) and source_group not in {
                "铁炉西工班群", "工班长交流群"
            }:
                continue
        return rule_id
    return None


def _named_assignments(text: str, members: list[str]) -> list[str]:
    hits: list[tuple[int, str]] = []
    for name in members:
        escaped = re.escape(name)
        patterns = (
            rf"(?:由|让|安排)?\s*{escaped}(?:同志)?\s*(?:(?:个人|本人)\s*)?(?:承办|办理|执行|负责|责任人|承办人|执行人)",
            rf"(?:责任人|承办人|执行人)\s*[：:为是]?\s*{escaped}",
        )
        offsets = [match.start() for pattern in patterns for match in re.finditer(pattern, text)]
        if offsets:
            hits.append((min(offsets), name))
    return [name for _, name in sorted(hits)]


def _verified_handover(
    *,
    rule_id: str,
    config: dict[str, Any],
    run_date: date,
) -> dict[str, Any]:
    contract = load_contract()
    rule = contract["rules"][rule_id]
    area = rule["dynamic_area"]
    period = run_date.strftime("%Y-%m")
    evidence_rows = [
        row for row in (config.get("handover_evidence") or [])
        if isinstance(row, dict)
        and str(row.get("period") or "").strip() == period
        and str(row.get("area_name") or "").strip() == area
    ]
    anchor = contract.get("monthly_anchors", {}).get(period, {}).get(area)
    last_day = calendar.monthrange(run_date.year, run_date.month)[1]
    if not evidence_rows and anchor and run_date.day < last_day:
        return {
            "verified": True,
            "owners": [anchor],
            "evidence_ids": [f"user-confirmed-anchor:{period}:{area}"],
            "source": "user_confirmed_month_anchor",
            "hygiene_review": "anchor",
        }
    if len(evidence_rows) != 1:
        return {"verified": False, "reason": "handover_evidence_missing_or_ambiguous"}

    row = evidence_rows[0]
    owners = row.get("active_owners")
    evidence_id = str(row.get("evidence_id") or "").strip()
    evidence_source = str(row.get("source") or "").strip()
    status = str(row.get("status") or "").strip()
    hygiene_review = str(row.get("hygiene_review") or "").strip()
    members = set(contract["members"])
    if (
        row.get("reviewed") is not True
        or evidence_source not in ALLOWED_EVIDENCE_SOURCES
        or not evidence_id
        or not isinstance(owners, list)
        or not owners
        or any(not isinstance(owner, str) or owner not in members for owner in owners)
        or len(set(owners)) != len(owners)
    ):
        return {"verified": False, "reason": "handover_evidence_invalid"}

    if evidence_source == "user_confirmed_month_anchor":
        if not anchor or owners != [anchor] or status != "accepted":
            return {"verified": False, "reason": "handover_anchor_conflict"}
        if hygiene_review not in {"anchor", "passed"}:
            return {"verified": False, "reason": "handover_anchor_review_invalid"}
    elif status == "accepted":
        if hygiene_review != "passed":
            return {"verified": False, "reason": "handover_hygiene_not_approved"}
    elif status == "overlap":
        outgoing = str(row.get("outgoing_owner") or "").strip()
        incoming = str(row.get("incoming_owner") or "").strip()
        if (
            not outgoing
            or not incoming
            or outgoing not in owners
            or incoming not in owners
            or hygiene_review not in {"pending", "failed"}
        ):
            return {"verified": False, "reason": "handover_overlap_evidence_invalid"}
    else:
        return {"verified": False, "reason": "handover_status_invalid"}

    return {
        "verified": True,
        "owners": list(owners),
        "evidence_ids": [evidence_id],
        "source": evidence_source,
        "hygiene_review": hygiene_review,
        "status": status,
    }


def resolve_confirmed_assignment(
    *,
    rule_id: str,
    text: str,
    config: dict[str, Any],
    run_date: date,
) -> dict[str, Any]:
    contract = load_contract()
    rule = contract["rules"][rule_id]
    source_explicit = _named_assignments(text, contract["members"])
    audit: dict[str, Any] = {
        "rule_version": contract["contract_version"],
        "source": contract["source"],
        "rule_id": rule_id,
        "primary_owner": None,
        "collaborators": [],
        "awareness_only": False,
        "awareness_names": [],
        "manual_assignment_required": bool(rule.get("manual_assignment_required")),
        "assignment_basis": "confirmed_rule_contract",
        "handover_evidence_ids": [],
        "handover_source": None,
        "database_conflict": None,
    }

    if rule_id == "D":
        audit["awareness_only"] = True
        audit["awareness_names"] = list(rule["awareness_names"])
        audit["assignment_basis"] = "confirmed_workgroup_awareness"
        return {
            "module": rule["module"],
            "primary_owner": None,
            "collaborators": [],
            "executor_names": list(rule["awareness_names"]),
            "audit": audit,
        }

    if rule.get("manual_assignment_required"):
        if not source_explicit:
            return {
                "module": rule["module"],
                "primary_owner": None,
                "collaborators": [],
                "executor_names": [],
                "manual_assignment_required": True,
                "audit": audit,
            }
        primary = source_explicit[0]
        collaborators = source_explicit[1:]
        audit.update(
            primary_owner=primary,
            collaborators=collaborators,
            manual_assignment_required=False,
            assignment_basis="explicit_named_assignment_in_source",
        )
        return {
            "module": rule["module"],
            "primary_owner": primary,
            "collaborators": collaborators,
            "executor_names": [primary, *collaborators],
            "audit": audit,
        }

    if rule_id in {"B", "F"}:
        if source_explicit:
            primary = source_explicit[0]
            collaborators = source_explicit[1:]
            if primary != "李林骁":
                collaborators = list(dict.fromkeys([*collaborators, *rule["collaborators"]]))
            configured_owner = str((config.get("responsibility_rules") or {}).get(rule["module"]) or "").strip()
            if configured_owner and configured_owner != primary:
                audit["database_conflict"] = {
                    "configured_owner": configured_owner,
                    "selected_owner": primary,
                }
                audit["conflict_resolution"] = "explicit_current_assignment_has_priority_over_legacy_area_config"
                audit["legacy_configuration_recheck_required"] = True
            audit.update(
                primary_owner=primary,
                collaborators=collaborators,
                assignment_basis="explicit_named_assignment_in_source",
                handover_source="not_required_explicit_assignment",
            )
            return {
                "module": rule["module"],
                "primary_owner": primary,
                "collaborators": collaborators,
                "executor_names": list(dict.fromkeys([primary, *collaborators])),
                "audit": audit,
            }
        handover = _verified_handover(rule_id=rule_id, config=config, run_date=run_date)
        if not handover.get("verified"):
            return {
                "module": rule["module"],
                "primary_owner": None,
                "collaborators": [],
                "executor_names": [],
                "reason": "confirmed_area_handover_unverified",
                "missing_evidence_reason": handover.get("reason"),
                "audit": audit,
            }
        owners = list(handover["owners"])
        configured_owner = str((config.get("responsibility_rules") or {}).get(rule["module"]) or "").strip()
        selected_owner = owners[0]
        if configured_owner and configured_owner not in owners:
            audit["database_conflict"] = {
                "configured_owner": configured_owner,
                "selected_owner": selected_owner,
            }
            audit["conflict_resolution"] = "verified_handover_evidence_has_priority_over_legacy_area_config"
            audit["legacy_configuration_recheck_required"] = True
        audit["handover_evidence_ids"] = list(handover["evidence_ids"])
        audit["handover_source"] = handover["source"]
        audit["hygiene_review"] = handover["hygiene_review"]
        audit["handover_status"] = handover.get("status")
        audit["primary_owner"] = selected_owner
        audit["collaborators"] = list(dict.fromkeys([*owners[1:], *rule["collaborators"]]))
        return {
            "module": rule["module"],
            "primary_owner": selected_owner,
            "collaborators": audit["collaborators"],
            "executor_names": list(dict.fromkeys([*owners, *rule["collaborators"]])),
            "audit": audit,
        }

    if source_explicit:
        primary = source_explicit[0]
        collaborators = source_explicit[1:]
        if primary != "李林骁":
            collaborators = list(dict.fromkeys([*collaborators, *(rule.get("collaborators") or [])]))
        audit.update(
            primary_owner=primary,
            collaborators=collaborators,
            assignment_basis="explicit_named_assignment_in_source",
        )
    else:
        primary = rule["primary_owner"]
        collaborators = list(rule.get("collaborators") or [])
        audit.update(
            primary_owner=primary,
            collaborators=collaborators,
            assignment_basis="confirmed_static_rule",
        )
    return {
        "module": rule["module"],
        "primary_owner": primary,
        "collaborators": collaborators,
        "executor_names": list(dict.fromkeys([primary, *collaborators])),
        "audit": audit,
    }


def persistable_assignment_audit(plan: dict[str, Any]) -> dict[str, Any]:
    """Return only bounded rule/assignment provenance fields for the writer JSON column."""
    raw = plan.get("assignment_audit")
    if not isinstance(raw, dict):
        names = plan.get("routed_executor_names") or plan.get("executor_names") or []
        names = [str(name).strip() for name in names if isinstance(name, str) and name.strip()]
        owner = str(plan.get("route_owner") or (names[0] if names else "")).strip() or None
        collaborators = [name for name in names if name != owner]
        raw = {
            "rule_version": str(plan.get("rule_contract_version") or "confirmed-assignment/unclassified"),
            "source": "event_worker/explicit-assignment-fallback",
            "rule_id": None,
            "primary_owner": owner,
            "collaborators": collaborators,
            "awareness_only": False,
            "awareness_names": [],
            "manual_assignment_required": bool(
                plan.get("manual_assignment_required") or plan.get("reason") == "manual_assignment_required"
            ),
            "assignment_basis": "processor_resolved_names",
            "handover_evidence_ids": [],
            "handover_source": None,
        }
    allowed = {
        "rule_version", "source", "rule_id", "primary_owner", "collaborators", "awareness_only",
        "awareness_names", "manual_assignment_required", "assignment_basis", "handover_evidence_ids",
        "handover_source", "handover_status", "hygiene_review", "database_conflict",
        "conflict_resolution", "legacy_configuration_recheck_required",
    }
    result = {key: raw[key] for key in allowed if key in raw}
    required_defaults = {
        "rule_id": None,
        "primary_owner": None,
        "collaborators": [],
        "awareness_only": False,
        "awareness_names": [],
        "manual_assignment_required": False,
        "handover_evidence_ids": [],
        "handover_source": None,
    }
    for key, value in required_defaults.items():
        result.setdefault(key, value)
    if result.get("source") not in {
        "rule-driven-task-routing/references/confirmed-assignment-habits.md",
        "event_worker/explicit-assignment-fallback",
    }:
        raise AssignmentContractError("assignment_audit_source_unsupported")
    if not isinstance(result.get("rule_version"), str) or not result["rule_version"].strip():
        raise AssignmentContractError("assignment_audit_version_missing")
    if not isinstance(result.get("assignment_basis"), str) or not result["assignment_basis"].strip():
        raise AssignmentContractError("assignment_audit_basis_missing")
    return result
