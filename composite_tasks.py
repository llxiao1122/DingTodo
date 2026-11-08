"""Conservative composite detection and durable parent/child action execution."""

from __future__ import annotations

import hashlib
import re
from datetime import date
from typing import Any, Iterable

from dws_adapter import DwsError, build_create_sub_args, verify_todo
from routing import parse_due_datetime
from todo_execution import add_verified_source_comment, create_claimed_task


CONTRACT_VERSION = "child-actions/1.0"
_DEADLINE_RE = re.compile(r"(?P<deadline>(?:\d{1,2}月)?\d{1,2}日(?:之前|前|内)?)")
_SCOPES = (
    ("asset_inventory_permissions", re.compile(r"资产.{0,12}盘点.{0,10}权限|盘点.{0,10}权限")),
    ("asset_tags", re.compile(r"资产.{0,12}标签|标签.{0,10}(?:核验|核对|确认)")),
    ("online_process", re.compile(r"线上流程|线上.{0,8}流程")),
)
_ACTION_RE = re.compile(r"完成|核对|核验|确认|测试|办理|提交|处理|盘点")
_CLAUSE_SPLIT_RE = re.compile(r"[；;\n]+")


def detect_composite_candidate(text: str) -> dict[str, Any] | None:
    """Recognize only complete, independently scoped clauses with one explicit date each."""
    if not isinstance(text, str) or not text.strip():
        return None
    components: list[dict[str, str]] = []
    for raw_clause in _CLAUSE_SPLIT_RE.split(text):
        clause = raw_clause.strip()
        if not clause or not _ACTION_RE.search(clause):
            continue
        matched_scopes = [key for key, pattern in _SCOPES if pattern.search(clause)]
        deadlines = list(_DEADLINE_RE.finditer(clause))
        if len(matched_scopes) != 1 or len(deadlines) != 1:
            return None
        components.append({
            "scope_key": matched_scopes[0],
            "source_span": clause,
            "deadline_text": deadlines[0].group("deadline"),
        })
    scope_keys = [row["scope_key"] for row in components]
    if len(components) < 2 or len(components) > len(_SCOPES) or len(set(scope_keys)) != len(components):
        return None
    return {
        "contract_version": CONTRACT_VERSION,
        "status": "deterministic_candidate",
        "source_spans": [row["source_span"] for row in components],
        "components": components,
    }


def prepare_child_components(candidate: dict[str, Any], run_date: date) -> list[dict[str, Any]]:
    """Resolve every date and title before the parent or any child is written."""
    if not isinstance(candidate, dict) or candidate.get("contract_version") != CONTRACT_VERSION:
        raise ValueError("composite_contract_version_invalid")
    rows = candidate.get("components")
    if not isinstance(rows, list) or len(rows) < 2 or len(rows) > len(_SCOPES):
        raise ValueError("composite_components_incomplete")
    prepared: list[dict[str, Any]] = []
    seen_scopes: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("composite_component_invalid")
        scope_key = str(row.get("scope_key") or "").strip()
        title = str(row.get("source_span") or "").strip()
        deadline_text = str(row.get("deadline_text") or "").strip()
        if scope_key not in {scope for scope, _ in _SCOPES} or scope_key in seen_scopes:
            raise ValueError("composite_scope_not_unique")
        if not title or len(title) > 80:
            raise ValueError("composite_child_title_incomplete")
        date_value, due_iso = parse_due_datetime(deadline_text, run_date)
        if not date_value or not due_iso:
            raise ValueError("composite_child_deadline_unresolved")
        if date.fromisoformat(date_value) < run_date:
            raise ValueError("composite_child_deadline_in_past")
        prepared.append({
            "scope_key": scope_key,
            "source_span": title,
            "deadline_text": deadline_text,
            "title": title,
            "run_date": run_date.isoformat(),
            "due_date": date_value,
            "due_iso": due_iso,
            "priority": 20,
        })
        seen_scopes.add(scope_key)
    if len({row["due_date"] for row in prepared}) != len(prepared):
        raise ValueError("composite_deadlines_not_distinct")
    return prepared


def propose_child_actions(
    *,
    parent_task_id: str,
    conversation_id: str,
    message_id: str,
    business_scope: str,
    components: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Derive stable source+business+component keys; titles and positions are excluded."""
    required = {
        "parent_task_id": parent_task_id,
        "conversation_id": conversation_id,
        "message_id": message_id,
        "business_scope": business_scope,
    }
    if not isinstance(parent_task_id, str) or not parent_task_id.strip():
        raise ValueError("parent_task_id_required")
    if any(not isinstance(value, str) or not value.strip() for key, value in required.items() if key != "parent_task_id"):
        raise ValueError("child_source_identity_required")
    if len(parent_task_id) > 128 or len(conversation_id) > 128 or len(message_id) > 128 or len(business_scope) > 64:
        raise ValueError("child_source_identity_too_long")
    rows = list(components)
    if not rows:
        raise ValueError("child_components_required")
    scopes: set[str] = set()
    result: list[dict[str, Any]] = []
    for component in rows:
        if not isinstance(component, dict):
            raise ValueError("child_component_invalid")
        scope_key = str(component.get("scope_key") or "").strip()
        source_span = str(component.get("source_span") or "").strip()
        deadline_text = str(component.get("deadline_text") or "").strip()
        if scope_key not in {scope for scope, _ in _SCOPES} or not source_span:
            raise ValueError("child_component_identity_missing")
        if not deadline_text or not _DEADLINE_RE.search(deadline_text):
            raise ValueError("explicit_deadline_required")
        matched_scopes = [key for key, pattern in _SCOPES if pattern.search(source_span)]
        span_deadlines = [match.group("deadline") for match in _DEADLINE_RE.finditer(source_span)]
        if matched_scopes != [scope_key] or span_deadlines != [deadline_text]:
            raise ValueError("child_scope_or_deadline_not_in_source_span")
        if scope_key in scopes:
            raise ValueError("child_scope_key_duplicate")
        scopes.add(scope_key)
        stable_identity = "\0".join((conversation_id, message_id, business_scope, scope_key))
        action_key = "child:" + hashlib.sha256(stable_identity.encode("utf-8")).hexdigest()[:24]
        result.append({
            "operation": "claim_child_action",
            "contract_version": CONTRACT_VERSION,
            "parent_task_id": parent_task_id,
            "conversation_id": conversation_id,
            "message_id": message_id,
            "business_scope": business_scope,
            "scope_key": scope_key,
            "action_key": action_key,
            "source_span": source_span,
            "deadline_text": deadline_text,
        })
    return result


def run_durable_child_actions(
    *,
    parent_action_id: Any,
    parent_task_id: str,
    conversation_id: str,
    message_id: str,
    business_scope: str,
    executor_ids: Iterable[str],
    executor_names: Iterable[str],
    components: Iterable[dict[str, Any]],
    writer: Any,
    dws: Any,
    allow_external_writes: bool = False,
) -> dict[str, Any]:
    """Claim, create, link, detail-read, and comment each child without retrying unknown writes."""
    if not allow_external_writes:
        return {"status": "REVIEW", "created": 0, "resumed": 0, "errors": ["child_execution_not_enabled"]}
    if (
        isinstance(parent_action_id, bool)
        or not isinstance(parent_action_id, int)
        or parent_action_id <= 0
        or not isinstance(parent_task_id, str)
        or not parent_task_id.strip()
        or parent_task_id != parent_task_id.strip()
        or len(parent_task_id) > 128
        or not isinstance(conversation_id, str) or not conversation_id.strip() or len(conversation_id) > 128
        or not isinstance(message_id, str) or not message_id.strip() or len(message_id) > 128
        or not isinstance(business_scope, str) or not business_scope.strip() or len(business_scope) > 64
    ):
        return {"status": "REVIEW", "created": 0, "resumed": 0, "errors": ["parent_task_link_required"]}
    ids = [str(value).strip() for value in executor_ids if str(value).strip()]
    names = [str(value).strip() for value in executor_names if str(value).strip()]
    if not ids or len(ids) != len(set(ids)) or not names or len(names) != len(set(names)) or len(ids) != len(names):
        return {"status": "REVIEW", "created": 0, "resumed": 0, "errors": ["verified_executor_ids_required"]}

    rows = list(components)
    try:
        proposals = propose_child_actions(
            parent_task_id=parent_task_id,
            conversation_id=conversation_id,
            message_id=message_id,
            business_scope=business_scope,
            components=rows,
        )
    except ValueError as exc:
        return {"status": "REVIEW", "created": 0, "resumed": 0, "errors": [str(exc)]}

    # Validate the entire split before the first child claim or DWS write.
    prepared: list[tuple[dict[str, Any], dict[str, Any], list[str], list[str]]] = []
    prepared_dates: set[str] = set()
    for proposal, component in zip(proposals, rows):
        title = str(component.get("title") or component.get("source_span") or "").strip()
        due_iso = component.get("due_iso")
        source_span = str(component.get("source_span") or "").strip()
        deadline_text = str(component.get("deadline_text") or "").strip()
        run_date_text = str(component.get("run_date") or "").strip()
        if (
            not title or title != source_span or len(title) > 80
            or not isinstance(due_iso, str) or not due_iso.strip()
            or not deadline_text or not run_date_text
        ):
            return {"status": "REVIEW", "created": 0, "resumed": 0, "errors": ["child_title_or_explicit_due_missing"]}
        try:
            parsed_run_date = date.fromisoformat(run_date_text)
            resolved_date, resolved_due_iso = parse_due_datetime(deadline_text, parsed_run_date)
            if (
                not resolved_date or resolved_date != component.get("due_date")
                or resolved_due_iso != due_iso or resolved_date in prepared_dates
            ):
                raise ValueError("date_mismatch")
            prepared_dates.add(resolved_date)
        except (TypeError, ValueError):
            return {"status": "REVIEW", "created": 0, "resumed": 0, "errors": ["child_deadline_not_source_proven"]}
        child_ids = [str(value).strip() for value in component.get("executor_ids", ids) if str(value).strip()]
        child_names = [str(value).strip() for value in component.get("executor_names", names) if str(value).strip()]
        if (
            not child_ids or len(child_ids) != len(child_names)
            or len(child_ids) != len(set(child_ids)) or len(child_names) != len(set(child_names))
        ):
            return {"status": "REVIEW", "created": 0, "resumed": 0, "errors": ["child_executor_identity_incomplete"]}
        try:
            build_create_sub_args(
                parent_task_id=parent_task_id, title=title, executor_ids=child_ids,
                due_iso=due_iso, priority=int(component.get("priority", 20)),
            )
        except (TypeError, ValueError):
            return {"status": "REVIEW", "created": 0, "resumed": 0, "errors": ["child_plan_invalid"]}
        prepared.append((proposal, component, child_ids, child_names))

    created_count = 0
    resumed_count = 0
    for proposal, component, ids, names in prepared:
        title = str(component.get("title") or component.get("source_span") or "").strip()
        due_iso = str(component["due_iso"])
        claim_request = {
            "operation": "claim_child_action",
            "contract_version": CONTRACT_VERSION,
            "parent_action_id": parent_action_id,
            "parent_task_id": parent_task_id,
            "conversation_id": conversation_id,
            "message_id": message_id,
            "business_scope": business_scope,
            "scope_key": proposal["scope_key"],
            "action_key": proposal["action_key"],
            "status": "CLAIMED",
        }
        try:
            claim = writer.execute(claim_request)
        except Exception:
            return {"status": "REVIEW", "created": created_count, "resumed": resumed_count,
                    "errors": ["child_action_claim_failed"]}
        action_id = claim.get("action_id") if isinstance(claim, dict) else None
        if isinstance(action_id, bool) or not isinstance(action_id, int) or action_id <= 0:
            return {"status": "REVIEW", "created": created_count, "resumed": resumed_count,
                    "errors": ["child_action_id_missing"]}
        task_id = str(claim.get("external_task_id") or "").strip()
        is_new_claim = claim.get("created") is True
        if not task_id and not is_new_claim:
            return {"status": "REVIEW", "created": created_count, "resumed": resumed_count,
                    "errors": ["child_create_state_unknown"]}
        if task_id and is_new_claim:
            return {"status": "REVIEW", "created": created_count, "resumed": resumed_count,
                    "errors": ["child_claim_response_inconsistent"]}

        comment = (
            "父任务：" + parent_task_id + "\n"
            + "来源：" + conversation_id + "/" + message_id + "\n"
            + "业务范围：" + business_scope + "\n"
            + "子任务稳定键：" + proposal["action_key"] + "\n"
            + "原文：" + str(component["source_span"])
        )
        if not task_id:
            try:
                task_id = create_claimed_task(
                    action_id=action_id,
                    claim_acquired=is_new_claim,
                    create=lambda: dws.create_sub_todo(
                        parent_task_id, title, ids, due_iso,
                        priority=int(component.get("priority", 20)),
                    ),
                    link=lambda claimed_action_id, created_task_id: writer.execute({
                        "operation": "link_external_task", "action_id": int(claimed_action_id),
                        "status": "EXTERNAL_CREATED", "external_task_id": created_task_id,
                    }),
                )
            except Exception:
                try:
                    writer.execute({"operation": "save_action_result", "action_id": action_id,
                                    "status": "REVIEW", "last_error": "child_create_outcome_unknown"})
                except Exception:
                    pass
                return {"status": "REVIEW", "created": created_count, "resumed": resumed_count,
                        "errors": ["child_create_outcome_unknown"]}
            created_count += 1
        else:
            resumed_count += 1

        comment_write_started = False
        try:
            detail = dws.get_todo(task_id)
            if not isinstance(detail, dict) or str(detail.get("taskId") or detail.get("todoTaskId") or "").strip() != task_id:
                raise DwsError("child_readback_task_id_mismatch")
            if str(detail.get("parentId") or detail.get("parentTaskId") or "").strip() != parent_task_id:
                raise DwsError("child_readback_parent_mismatch")
            child_plan = {
                "title": title,
                "due_iso": due_iso,
                "priority": int(component.get("priority", 20)),
                "executor_ids": ids,
                "executor_names": names,
            }
            if verify_todo(detail, child_plan):
                raise DwsError("child_readback_mismatch")
            if claim.get("last_error") == "child_comment_write_started":
                comment_attempted = True
            else:
                comment_attempted = False

            def mark_child_comment_started():
                nonlocal comment_write_started
                writer.execute({
                    "operation": "save_action_result", "action_id": action_id,
                    "status": "EXTERNAL_CREATED", "last_error": "child_comment_write_started",
                })
                comment_write_started = True

            def mark_child_comment_verified():
                writer.execute({
                    "operation": "save_action_result", "action_id": action_id,
                    "status": "EXTERNAL_CREATED", "last_error": None,
                })

            add_verified_source_comment(
                dws,
                task_id,
                comment,
                already_attempted=comment_attempted,
                check_existing=True,
                on_attempt=mark_child_comment_started,
                on_verified=mark_child_comment_verified,
            )
        except Exception:
            try:
                writer.execute({
                    "operation": "save_action_result", "action_id": action_id,
                    "status": "REVIEW",
                    "last_error": "child_comment_write_started" if comment_write_started else "child_readback_or_comment_incomplete",
                })
            except Exception:
                pass
            return {"status": "REVIEW", "created": created_count, "resumed": resumed_count,
                    "errors": ["child_readback_or_comment_incomplete"]}

    return {"status": "COMPLETED", "created": created_count, "resumed": resumed_count, "errors": []}
