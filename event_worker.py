#!/usr/bin/env python3
"""Single-shot historical poller for the local DingTalk auto-todo workflow.

The default mode is shadow and reads configured group history once per run.
Ledger writes and external todo writes require separate explicit flags.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, time as clock_time, timedelta, timezone
import hashlib
import json
import math
import os
import re
import stat
import unicodedata
from pathlib import Path
import sys
from typing import Any, Callable, Iterable, Sequence

SCRIPT_DIRECTORY = Path(__file__).resolve().parent
if str(SCRIPT_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIRECTORY))

POLARDB_ROOT = Path(os.environ.get("HERMES_POLARDB_RUNTIME", "/root/.hermes/polardb_runtime"))
if str(POLARDB_ROOT) not in sys.path:
    sys.path.insert(0, str(POLARDB_ROOT))

from workflows.polardb_work_adapter.query import WorkQueryAdapter  # noqa: E402
from workflows.polardb_work_writer.writer import WorkWriter  # noqa: E402

from dingtalk_reader import DingTalkReader, parse_not_before  # noqa: E402
from assignment_policy import persistable_assignment_audit  # noqa: E402
from business_dedupe import BusinessCandidateError, candidate_matches_business  # noqa: E402
from composite_tasks import (  # noqa: E402
    CONTRACT_VERSION as CHILD_ACTION_CONTRACT_VERSION,
    detect_composite_candidate,
    prepare_child_components,
    run_durable_child_actions,
)
from event_lifecycle import sync_todo_lifecycle_events, validate_queued_todo_event  # noqa: E402
from deadline_clarifications import DeadlineClarification, load_deadline_clarification  # noqa: E402
from dws_adapter import DwsClient, DwsError, verify_todo  # noqa: E402
from dws_parity import DwsParityError, DwsParityLedger  # noqa: E402
from todo_execution import (  # noqa: E402
    add_verified_source_comment,
    create_claimed_task,
)
from jev_v2 import JevV2Resolver
from llm_adapter import (  # noqa: E402
    CommandLlmResolver,
    DateRoleFallbackResolver,
    DeepSeekLlmResolver,
    HermesCodexLlmResolver,
    JevResolver,
    LlmError,
    SemanticFallbackResolver,
    TitleFieldResolver,
    _message_mention_names,
    _redact_jev_text,
    build_semantic_fallback_context,
    build_title_field_context,
    validate_semantic_fallback_result,
    validate_title_field_result,
    validate_date_role_fallback_result,
)
from live_runner import (  # noqa: E402
    calendar_gate,
    build_planner_config,
    group_specs,
    read_all,
)
from runner import plan_batch  # noqa: E402
from routing import (  # noqa: E402
    ACTION_TERMS,
    CONTEXTUAL_FOLLOWUP_VERIFIED_TOKEN,
    LOCAL_WORKGROUP_SOURCE_GROUPS,
    WORKGROUP_COVERAGE_TERMS,
    _at_names,
    _has_all_scope_mention,
    _self_assignment,
    build_source_comment,
    extract_due_candidates,
    has_competing_assessment_cutoff,
    infer_semantic_context,
    parse_due_date,
)


CHINA_TZ = timezone(timedelta(hours=8))
EXECUTION_START = clock_time(9, 0)
EXECUTION_END = clock_time(17, 30)


def within_execution_window(moment: datetime) -> bool:
    local = moment.astimezone(CHINA_TZ)
    return EXECUTION_START <= local.time() < EXECUTION_END


def configure_writer_environment() -> None:
    os.environ.setdefault("HERMES_WORK_WRITER_ENV", "/etc/cipher-work.env")


class EventFormatError(ValueError):
    pass


def _strip_untrusted_private_flags(value: Any) -> Any:
    """Remove private planner evidence markers from untrusted raw event payloads."""
    if isinstance(value, dict):
        private_fields = {
            "_jev_v2_title_verified",
            "_jev_v2_authorized_shadow_create",
            "_contextual_followup_verified",
            "_due_date_selection_resolved",
            "_llm_semantic_intent",
            "_jev_v2_applicable",
            "_jev_v2_policy_context",
            "human_confirmation_evidence",
            "_human_confirmation_evidence",
            "_deadline_clarification",
            "_deadline_clarification_verified",
            "_deadline_clarification_evidence",
        }
        return {
            key: _strip_untrusted_private_flags(item)
            for key, item in value.items()
            if key not in private_fields
        }
    if isinstance(value, list):
        return [_strip_untrusted_private_flags(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_strip_untrusted_private_flags(item) for item in value)
    return value


def _resolve_new_policy_due_key(
    date_candidates: Sequence[dict[str, Any]],
    source_text: str = "",
) -> tuple[str | None, str | None]:
    """Accept only one locally classified deadline; model choice cannot set dates."""
    if any(candidate.get("role") == "deadline" for candidate in date_candidates) and has_competing_assessment_cutoff(source_text):
        return None, "jev_v2_competing_deadline_context"

    unknown_roles = [
        candidate for candidate in date_candidates
        if candidate.get("role") not in {"deadline", "effective", "event", "historical"}
    ]
    if unknown_roles:
        return None, "jev_v2_date_role_unverified"

    deadlines = [candidate for candidate in date_candidates if candidate.get("role") == "deadline"]
    if len(deadlines) == 1:
        return str(deadlines[0].get("key")), None
    if len(deadlines) > 1:
        return None, "jev_v2_multiple_explicit_deadlines"

    # Known effective, scheduled-event, and historical dates are source facts,
    # not task deadlines, regardless of the model's choice.
    return None, None


def _apply_configured_hazard_owner(plan: dict[str, Any], config: dict[str, Any]) -> None:
    """Apply only the configured hazardous-waste owner to a review proposal."""
    if plan.get("route_module") != "hazardous_waste":
        return
    if config.get("assign_to_self_only") is True:
        plan["executor_ids"] = []
        plan["executor_names"] = []
        plan["routed_executor_names"] = []
        return
    configured_owner = _as_text((config.get("responsibility_rules") or {}).get("hazardous_waste"))
    # Review rows never carry executable identities, even if an earlier
    # deterministic plan was temporarily promoted before a later review gate.
    plan["executor_ids"] = []
    plan["executor_names"] = []
    plan["routed_executor_names"] = []
    for field in ("route_owner_id", "route_owner_ids", "executor_id"):
        if field in plan:
            plan[field] = [] if field.endswith("ids") else None
    assignment_audit = plan.get("assignment_audit") or {}
    if assignment_audit.get("rule_id") == "F":
        # Confirmed current-month evidence is authoritative. Never replace it
        # with a legacy database owner merely because a later gate reviews.
        return
    if configured_owner:
        plan["route_owner"] = configured_owner
        plan["matrix_route_owner"] = configured_owner
        plan["missing_fields"] = [
            field for field in (plan.get("missing_fields") or []) if field != "route_owner"
        ]
        return
    plan["route_owner"] = None
    plan["matrix_route_owner"] = None
    plan["missing_fields"] = list(dict.fromkeys([*(plan.get("missing_fields") or []), "route_owner"]))
    plan["warnings"] = list(dict.fromkeys([*(plan.get("warnings") or []), "authoritative_hazardous_waste_owner_unavailable"]))


_SAFE_RESOLVER_CODES = frozenset({
    "codex_runtime_initialization_failed", "codex_runtime_failed", "codex_runtime_incomplete",
    "codex_returned_non_json", "codex_missing_response_text", "codex_invalid_response",
    "date_role_source_incomplete", "date_role_candidates_invalid", "date_role_candidate_set_too_large",
    "date_role_candidate_invalid", "date_role_source_evidence_incomplete", "date_role_redaction_incomplete",
    "date_role_fallback_schema_invalid", "date_role_fallback_keys_invalid", "date_role_fallback_evidence_invalid",
    "date_role_fallback_identifiers_invalid", "date_role_resolver_contract_only", "date_role_runtime_failed",
    "date_role_runtime_incomplete", "date_role_returned_non_json", "date_role_invalid_response",
    "deepseek_transport_failed", "deepseek_invalid_response", "jev_redaction_incomplete",
    "jev_api_key_unavailable", "jev_transport_failed", "jev_invalid_response", "jev_v2_ineligible_plan",
    "jev_v2_candidate_invalid", "jev_date_candidate_invalid", "jev_v2_policy_context_invalid",
    "jev_v2_source_invalid", "jev_v2_source_incomplete",
    "jev_v2_answer_schema_invalid", "jev_v2_probability_invalid", "llm_command_failed",
    "llm_returned_non_json", "llm_result_must_be_object", "llm_cannot_override",
    "llm_result_has_no_completion", "semantic_fallback_schema_invalid", "semantic_fallback_enum_invalid",
    "semantic_fallback_due_key_invalid", "semantic_fallback_match_invalid", "semantic_fallback_title_invalid",
    "semantic_fallback_evidence_invalid", "semantic_fallback_due_candidate_missing",
    "semantic_fallback_source_invalid", "semantic_fallback_source_incomplete", "semantic_fallback_title_incomplete",
    "semantic_fallback_date_candidate_invalid", "semantic_fallback_business_candidate_invalid",
    "semantic_fallback_business_candidate_incomplete", "semantic_fallback_policy_context_invalid",
    "semantic_fallback_redaction_incomplete",
    "title_source_incomplete", "title_redaction_incomplete", "title_field_schema_invalid",
    "title_field_invalid", "title_field_evidence_invalid", "title_field_unredacted_or_placeholder",
    "title_runtime_failed", "title_runtime_incomplete", "title_returned_non_json",
    "title_compiler_invalid_title", "title_compiler_replan_incomplete",
})


def _safe_resolver_failure(exc: BaseException) -> dict[str, str]:
    """Reduce resolver failures to a stable code and coarse failure stage."""
    try:
        raw = str(exc)
    except Exception:
        raw = ""
    code_prefix = raw.split(":", 1)[0]
    if code_prefix.lower() not in _SAFE_RESOLVER_CODES:
        code = "resolver_error"
    else:
        code = code_prefix.lower()

    if any(marker in code for marker in (
        "transport", "runtime_failed", "runtime_initialization_failed", "command_failed",
        "api_key_unavailable", "runtime_unavailable",
    )) or isinstance(exc, (TimeoutError, OSError)):
        stage = "transport"
    elif any(marker in code for marker in (
        "schema", "non_json", "invalid_response", "answer_schema", "missing_response",
        "runtime_incomplete",
    )) or isinstance(exc, (json.JSONDecodeError,)):
        stage = "schema"
    elif code == "resolver_error" and isinstance(exc, RuntimeError):
        stage = "transport"
    else:
        stage = "validation"
    return {"code": code, "stage": stage}


def _record_resolver_failure(plan: dict[str, Any], field: str, exc: BaseException) -> None:
    plan[field] = _safe_resolver_failure(exc)


def _validate_semantic_fallback_result(
    value: Any,
    event: dict[str, Any],
    plan: dict[str, Any],
    date_candidates: Sequence[dict[str, Any]],
    business_candidates: Sequence[dict[str, Any]],
    semantic_fallback: Any,
) -> tuple[dict[str, Any], Any]:
    """Recheck a fallback result at the processor boundary before counting success."""
    try:
        max_source_chars = int(getattr(semantic_fallback, "max_source_chars", 4000))
    except (TypeError, ValueError, OverflowError):
        raise ValueError("semantic_fallback_source_incomplete") from None
    safe_context = build_semantic_fallback_context(
        event,
        plan,
        date_candidates=date_candidates,
        business_candidates=business_candidates,
        max_source_chars=max_source_chars,
    )
    validated = validate_semantic_fallback_result(
        value,
        event,
        plan,
        date_candidates,
        business_candidates,
        max_source_chars=max_source_chars,
        context=safe_context,
    )
    return validated, safe_context


def _apply_review_field_integrity(
    plan: dict[str, Any],
    event: dict[str, Any],
    date_candidates: Sequence[dict[str, Any]],
    deadline_clarification: DeadlineClarification | None,
    human_confirmation_evidence: dict[str, str] | None,
) -> dict[str, str] | None:
    """Keep REVIEW due fields tied to validated source or human evidence."""
    if plan.get("decision") != "REVIEW":
        return None

    clarification_targets_source = bool(
        deadline_clarification is not None
        and str(event.get("message_id") or "") == deadline_clarification.source_message_id
    )
    if (
        plan.get("route_module") == "hazardous_waste"
        and deadline_clarification is not None
        and deadline_clarification.matches(event)
        and human_confirmation_evidence is not None
    ):
        plan["due_date"] = deadline_clarification.confirmed_due_date
        plan["due_iso"] = deadline_clarification.confirmed_due_iso
        plan["due_role_unresolved"] = False
        plan.pop("due_selection_error", None)
        plan["missing_fields"] = [
            field for field in (plan.get("missing_fields") or []) if field != "due_date"
        ]
        return human_confirmation_evidence

    if clarification_targets_source:
        plan["due_date"] = None
        plan["due_iso"] = None
        plan["due_role_unresolved"] = True
        plan["missing_fields"] = list(dict.fromkeys([*(plan.get("missing_fields") or []), "due_date"]))
        return None

    due_date = _as_text(plan.get("due_date"))
    due_iso = _as_text(plan.get("due_iso"))
    deadlines = [candidate for candidate in date_candidates if candidate.get("role") == "deadline"]
    source_text = str(event.get("text") or event.get("content") or "")
    source_deadline_is_unambiguous = (
        len(deadlines) == 1
        and not has_competing_assessment_cutoff(source_text)
        and bool(deadlines[0].get("due_date"))
        and bool(deadlines[0].get("due_iso"))
    )
    if source_deadline_is_unambiguous:
        plan["due_date"] = str(deadlines[0]["due_date"])
        plan["due_iso"] = str(deadlines[0]["due_iso"])
        plan["due_role_unresolved"] = False
        plan.pop("due_selection_error", None)
        plan["missing_fields"] = [
            field for field in (plan.get("missing_fields") or []) if field != "due_date"
        ]
        return None

    unresolved_roles = (
        any(candidate.get("role") == "unknown" for candidate in date_candidates)
        or len(deadlines) > 1
        or bool(deadlines and has_competing_assessment_cutoff(source_text))
    )
    had_unverified_due = bool(due_date or due_iso)
    if had_unverified_due:
        plan["due_date"] = None
        plan["due_iso"] = None
    due_role_unresolved = unresolved_roles or (had_unverified_due and not date_candidates)
    if due_role_unresolved:
        plan["due_role_unresolved"] = True
        plan["missing_fields"] = list(dict.fromkeys([*(plan.get("missing_fields") or []), "due_date"]))
    else:
        plan["due_role_unresolved"] = False
        plan["missing_fields"] = [
            field for field in (plan.get("missing_fields") or []) if field != "due_date"
        ]
    return None


def _as_text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _has_substantive_jev_text(value: Any) -> bool:
    text = _as_text(value)
    text = re.sub(r"\[文件\][^\n]*|\[图片消息\][^\n]*|\[附件\][^\n]*", "", text, flags=re.I)
    text = re.sub(r"\b(?:fileId|mediaId)\s*[:=]\s*\S+", "", text, flags=re.I)
    text = re.sub(r"https?://\S+|<[^>]*>|\[[^\]]*\]\([^)]*\)", "", text)
    return bool(re.sub(r"[\s\W_]+", "", text, flags=re.UNICODE))


ARRIVED_CONTEXT_LIMIT = 128
ARRIVED_NOTICE_MAX_CONVERSATION_GAP = 12
ARRIVED_NOTICE_MAX_AGE_SECONDS = 30 * 60
CAUSAL_NOTICE_MAX_AGE_SECONDS = 120
COMPLETE_DWS_PAGE_ORDER = "complete_dws_chronological_page"
CONTEXT_FOLLOWUP_MAX_AGE_SECONDS = 7 * 24 * 60 * 60
_LOCAL_WORKGROUP_PREFIX_RE = re.compile(r"^\s*铁炉西(?:工班)?(?:的|[:：,，\s]*)")
_UNFINISHED_INSPECTION_RE = re.compile(
    r"(?:尚未|暂未|还未|还没|未)(?:取回|拿回|归还|领回)"
)
_TERMINAL_INSPECTION_RE = re.compile(
    r"(?:(?:已经|已|刚刚|刚)\s*(?:取回|拿回|归还|领回))"
    r"|(?:取消|撤销|作废|无需|不用|不必).{0,8}(?:取回|拿回|归还|领回)"
)
_INSPECTION_CONTEXT_RE = re.compile(r"送检|取回|拿回|归还|领回|取消|撤销|作废")
_RETRIEVAL_INSTRUCTION_RE = re.compile(
    r"(?P<unfinished>尚未取回|还未取回|还没取回|未取回|尚未归还|未归还)的?"
    r"(?P<meeting>(?:下次|下一次)[^，,；;。！？!?\n]{0,8}?(?:开会|会议|碰头)(?:的时候|时|上)?)"
    r"\s*(?:(?:就|再|记得|要|去|再去)\s*)*"
    r"(?P<action>取回来|拿回来|取回|拿回|取|拿)"
)
_PERSON_MENTION_PATTERN = re.compile(r"@\s*[^\s,，;；。！？!?、]+")
_FILE_TITLE_PATTERN = re.compile(
    r"(?<![\w])([^\s\[\]<>\"']{1,180}\.(?:docx?|xlsx?|xls|pdf|pptx?))(?=$|[?#]|[\s\]，,。；;））》])",
    re.IGNORECASE,
)
_FILE_EXTENSION_PATTERN = re.compile(r"\.(?:docx?|xlsx?|xls|pdf|pptx?)(?:$|[?#])", re.IGNORECASE)


def _verified_message_sender(event: dict[str, Any], self_user: dict[str, Any]) -> bool:
    """Trust only the independently supplied message-sender namespace identity."""
    expected = _as_text(self_user.get("message_sender_id"))
    actual = _as_text(event.get("sender_id"))
    return bool(expected and actual and expected == actual)


def _inspection_group_label(group: str) -> str:
    return "铁炉西" if group in LOCAL_WORKGROUP_SOURCE_GROUPS else ""


def _clean_inspection_item(value: str, group: str) -> str:
    candidate = str(value or "").strip()
    group_label = _inspection_group_label(group)
    aliases = (group, group.removesuffix("群"), group_label)
    for alias in sorted((item for item in aliases if item), key=len, reverse=True):
        candidate = candidate.replace(alias, "")
    candidate = re.sub(r"(?:已经|已于|已|曾经|曾|将要|准备|计划|目前|昨日|今天|今日|刚刚|刚)", "", candidate)
    candidate = re.sub(r"^.*(?:把|将|对|要求|安排|负责)", "", candidate)
    candidate = candidate.replace("工班", "").replace("的", "")
    candidate = candidate.strip(" \t\r\n：:，,；;、。()（）[]【】")
    candidate = re.sub(r"^(?:有|在|由|把|将|对|该|此|这把|这个|那把|那个)+", "", candidate)
    candidate = re.sub(r"(?:仍|尚|暂|未|已经|已|取回|拿回|归还|领回|后)+$", "", candidate)
    candidate = candidate.strip(" \t\r\n：:，,；;、。()（）[]【】")
    if (
        not candidate
        or len(candidate) > 24
        or re.search(r"[和与及、]", candidate)
        or candidate in {"物品", "东西", "物资", "用品", "设备", "器具", "工具", "仪器"}
        or not re.search(r"[\u3400-\u9fffA-Za-z0-9]", candidate)
    ):
        return ""
    return candidate


def _unfinished_inspection_status(
    text: str,
    group: str,
    *,
    allow_unscoped: bool = False,
) -> dict[str, str] | None:
    """Extract one unfinished object explicitly scoped to the managed workgroup."""
    source = _as_text(text)
    local_prefix = _LOCAL_WORKGROUP_PREFIX_RE.match(source)
    if (
        (local_prefix is None and not allow_unscoped)
        or len(re.findall("送检", source)) != 1
        or _TERMINAL_INSPECTION_RE.search(source)
        or re.search(r"(?:准备|计划|将|待|即将|尚未|未)\s*送检", source)
    ):
        return None
    pending = list(_UNFINISHED_INSPECTION_RE.finditer(source))
    if len(pending) != 1:
        return None
    send = source.find("送检")
    if send < 0:
        return None
    state = pending[0]
    left_start = local_prefix.end() if local_prefix else 0
    left = _clean_inspection_item(source[left_start:send], group)
    right = _clean_inspection_item(source[send + len("送检"):state.start()], group)
    if left and right and left != right:
        return None
    item = left or right
    if not item:
        return None
    return {
        "item": item,
        "inspection_action": "送检",
        "workgroup_prefix": "铁炉西" if local_prefix else _inspection_group_label(group),
        "scope_basis": (
            "explicit_managed_workgroup_prefix"
            if local_prefix
            else "configured_local_group_verified_sender"
        ),
        "unfinished_evidence": source[:state.end()].strip(),
        "item_evidence": source[max(0, send - 32):min(len(source), state.start() + 32)].strip(),
    }


def _retrieval_instruction(text: str, item: str) -> dict[str, str] | None:
    """Return one exact future-meeting retrieval instruction, if present."""
    source = _as_text(text)
    matches = list(_RETRIEVAL_INSTRUCTION_RE.finditer(source))
    if len(matches) != 1:
        return None
    match = matches[0]
    # A separately named object in this same retrieval clause must agree with
    # the prior status. Commas and sentence boundaries isolate nearby duties.
    left = max((source.rfind(mark, 0, match.start()) for mark in "，,；;。！？!?\n"), default=-1) + 1
    ends = [source.find(mark, match.end()) for mark in "，,；;。！？!?\n"]
    ends = [end for end in ends if end >= 0]
    right = min(ends) if ends else len(source)
    clause = source[left:right].strip()
    # This helper is only for an unqualified anaphoric instruction. Keep these
    # qualified source literals review-only: "@其他负责人 未取回的下次开会时去取",
    # "秤未取回的下次开会时去取", "未取回的下次开会时去取卡尺 @其他负责人".
    if _PERSON_MENTION_PATTERN.search(clause) or source[left:match.start()].strip():
        return None
    tail = source[match.end():right].strip(" \t\r\n ：:()（）")
    if tail and not (item in tail and not re.search(r"[和与及、]", tail)):
        return None
    if _TERMINAL_INSPECTION_RE.search(clause):
        return None
    return {
        "meeting_condition": match.group("meeting"),
        "action": "取回",
        "instruction_evidence": match.group(0),
    }


def _terminal_update_for_item(text: str, item: str) -> bool:
    source = _as_text(text)
    if not item or item not in source:
        return False
    for match in re.finditer(re.escape(item), source):
        window = source[max(0, match.start() - 24):min(len(source), match.end() + 24)]
        if _TERMINAL_INSPECTION_RE.search(window):
            return True
    return False


def _terminal_update_applies_to_managed_item(text: str, item: str, group: str) -> bool:
    """Require local scope before a mixed-workgroup reply can close a local item."""
    local_group = group in LOCAL_WORKGROUP_SOURCE_GROUPS
    if not local_group and _LOCAL_WORKGROUP_PREFIX_RE.match(_as_text(text)) is None:
        return False
    return _terminal_update_for_item(text, item)


def _is_pure_person_mention_only(event: dict[str, Any]) -> bool:
    """Recognize sender-verified person mentions with no new source content."""
    text = _as_text(event.get("text") or event.get("content"))
    if (
        event.get("resource_refs")
        or event.get("quoted_message")
        or any(event.get(key) for key in ("reply_to_message_id", "quoted_message_id", "related_message_id"))
        or _has_all_scope_mention(event, text)
    ):
        return False

    mention_tokens = _PERSON_MENTION_PATTERN.findall(text)
    structured_names = _at_names(event)
    group_mentions = {"all", "everyone", "所有人", "全体", "全体人员", "全员"}
    is_person_mention = any(
        re.sub(r"^@\s*", "", name, flags=re.I).casefold() not in group_mentions
        for name in (*mention_tokens, *structured_names)
    )
    if not is_person_mention:
        return False

    residual = _PERSON_MENTION_PATTERN.sub(" ", text)
    residual = re.sub(r"<\s*at\b[^>]*>|\[\s*at\b[^\]]*\]", " ", residual, flags=re.I)
    return not re.sub(r"[\s\W_]+", "", residual, flags=re.UNICODE)


def _source_file_titles(event: dict[str, Any]) -> list[tuple[str, str]]:
    """Return local-only (display title, normalized stem) attachment evidence."""
    titles: list[str] = []
    refs = event.get("resource_refs") or []

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                if str(key).casefold() in {
                    "name", "filename", "file_name", "filetitle", "file_title",
                    "originalname", "original_name", "title",
                } and isinstance(item, str) and _FILE_EXTENSION_PATTERN.search(item.strip()):
                    titles.append(item.strip().split("?")[0].split("#")[0])
                visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    visit(refs)
    text = unicodedata.normalize("NFKC", _as_text(event.get("text") or event.get("content")))
    titles.extend(match.group(1).rstrip(".,，。；;））》") for match in _FILE_TITLE_PATTERN.finditer(text))

    result: list[tuple[str, str]] = []
    seen: set[str] = set()
    for title in titles:
        cleaned = title.strip().strip("'\"“”‘’<>[]()（）")
        stem = re.sub(r"\.(?:docx?|xlsx?|xls|pdf|pptx?)$", "", cleaned, flags=re.I)
        normalized = re.sub(r"[^\w\u4e00-\u9fff]+", "", unicodedata.normalize("NFKC", stem).casefold())
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append((cleaned, normalized))
    return result


def _is_attachment_only_source(event: dict[str, Any], titles: Sequence[tuple[str, str]]) -> bool:
    refs = event.get("resource_refs") or []
    has_file_ref = _resource_refs_contain_file(refs)
    text = unicodedata.normalize("NFKC", _as_text(event.get("text") or event.get("content")))
    bare_attachment_marker = bool(
        re.fullmatch(r"\s*(?:\[文件\]|\[附件\]|文件|附件)\s*", text, flags=re.I)
    )
    if not titles and not has_file_ref and not bare_attachment_marker:
        return False
    residual = _FILE_TITLE_PATTERN.sub(" ", text)
    residual = re.sub(r"\[(?:文件|附件)\]|(?:文件|附件)\s*[:：]?", " ", residual, flags=re.I)
    residual = re.sub(
        r"(?i)\b(?:fileId|mediaId|resourceId|resource_id)\s*[:=]\s*[^\s,，;；]+",
        " ",
        residual,
    )
    return not re.sub(r"[\s\W_]+", "", residual, flags=re.UNICODE)


def _resource_refs_contain_file(value: Any) -> bool:
    if isinstance(value, dict):
        kind = _as_text(value.get("resourceIdType") or value.get("resource_id_type") or value.get("type")).casefold()
        content_type = _as_text(value.get("contentType") or value.get("content_type")).casefold()
        if kind in {"file", "attachment", "fileid", "file_id"} or content_type.startswith("application/"):
            return True
        return any(_resource_refs_contain_file(item) for item in value.values())
    if isinstance(value, list):
        return any(_resource_refs_contain_file(item) for item in value)
    return False


def _source_reference_ids(event: dict[str, Any]) -> set[str]:
    values: set[str] = set()
    for key in ("reply_to_message_id", "quoted_message_id", "related_message_id"):
        value = event.get(key)
        if value not in (None, "") and not isinstance(value, (dict, list)):
            values.add(str(value))
    quoted = event.get("quoted_message")
    if isinstance(quoted, dict):
        for key in ("message_id", "messageId", "open_message_id", "openMessageId"):
            value = quoted.get(key)
            if value not in (None, ""):
                values.add(str(value))
    return values


def _file_title_matches_notice(title_stem: str, notice_text: str) -> bool:
    # Short generic names such as "意见.docx" do not establish an object link.
    generic = {"附件", "文件", "资料", "方案", "意见", "通知", "汇报", "document", "file"}
    return len(title_stem) >= 6 and title_stem not in generic and title_stem in re.sub(
        r"[^\w\u4e00-\u9fff]+", "", unicodedata.normalize("NFKC", notice_text).casefold()
    )


def _explicit_managed_workgroup_scope(text: str) -> bool:
    """Use only current-workgroup terms already recognized by the local router."""
    return any(term in text for term in WORKGROUP_COVERAGE_TERMS)


def _strip_opaque_resource_metadata(value: Any) -> str:
    text = _as_text(value)
    text = re.sub(
        r"(?i)\b(?:fileId|mediaId|resourceId|resource_id)\s*[:=]\s*[^\s,，;；]+",
        "",
        text,
    )
    return re.sub(r"\s+", " ", text).strip(" ：:，,；;")


def _has_explicit_other_assignment(event: dict[str, Any], self_user: dict[str, Any]) -> bool:
    """Detect a directly named other-person assignment that fallback cannot reauthorize."""
    names = _at_names(event)
    self_name = _as_text(self_user.get("name") or self_user.get("display_name"))
    text = str(event.get("text") or event.get("content") or "")
    cues = ("负责", "完成", "落实", "开展", "核查", "检查", "整改", "反馈", "报送", "确认", "处理")
    for name in names:
        if not name or name == self_name:
            continue
        target = re.escape(name)
        direct = re.search(
            rf"@\s*{target}(?:\s*\([^)]*\))?[^。！？；;\n]{{0,32}}", text
        )
        reverse = re.search(
            rf"(?:请|由|交由|安排给)[^。！？；;\n]{{0,16}}@\s*{target}", text
        )
        if (direct and any(cue in direct.group(0) for cue in cues)) or reverse:
            return True
    # Source text can retain a literal @name even when structured participant
    # identity is missing. Treat an action next to that unresolved mention as
    # unavailable assignment authority, without trying to resolve the name.
    for mention in re.finditer(r"@(?!(?:所有人|全体成员|全体|all\b|everyone\b))", text, re.I):
        following = text[mention.end():]
        if self_name and following.startswith(self_name):
            continue
        clause = re.split(r"[。！？；;\n]", text[mention.start():], maxsplit=1)[0][:80]
        if any(cue in clause for cue in cues):
            return True
    return False


_SCOPE_ACTION_TERMS = (
    *(term for term in ACTION_TERMS if term not in {"请", "需要"}),
    "填报",
    "填写",
)


def _instruction_clause_scope(
    event: dict[str, Any], self_user: dict[str, Any]
) -> str:
    """Classify recipient scope only inside bounded, prospective action clauses."""
    text = _as_text(event.get("text") or event.get("content"))
    evidence: set[str] = set()
    for clause in re.split(r"[，,。！？!?；;\n]", text):
        clause = clause.strip()
        if not clause:
            continue
        action_matches = [
            match
            for term in _SCOPE_ACTION_TERMS
            for match in re.finditer(re.escape(term), clause)
        ]
        first_action = min((match.start() for match in action_matches), default=None)
        bounded_event = dict(event, text=clause)
        direct_self, self_basis = _self_assignment(bounded_event, self_user)
        direct_other = _has_explicit_other_assignment(bounded_event, self_user)
        if first_action is None:
            continue

        group_target = bool(
            first_action is not None
            and any(
                (position := clause.find(term)) >= 0 and position < first_action
                for term in WORKGROUP_COVERAGE_TERMS
            )
        )

        if direct_self:
            # Role aliases and plain-name references only count as recipients
            # when they occur before the action, rather than in its object.
            self_position_candidates = [
                position
                for value in (self_user.get("name"),)
                if value
                for position in [clause.find(_as_text(value))]
                if position >= 0
            ]
            if self_basis == "role_alias":
                self_position_candidates.extend(
                    position
                    for term in ("各总库工班长", "总库工班长")
                    if (position := clause.find(term)) >= 0
                )
            if self_position_candidates and first_action is not None and min(self_position_candidates) > first_action:
                direct_self = False
            elif self_basis == "self_and_workgroup_reference" and not group_target:
                direct_self = False

        if direct_other and group_target and first_action is not None:
            # A named person after a workgroup action is a handoff recipient
            # unless the clause gives that person a separate action as well.
            mentions = list(re.finditer(r"@\s*[^\s,，;；。！？!?、]+", clause))
            if mentions and all(
                mention.start() > first_action
                and not any(action.start() > mention.end() for action in action_matches)
                for mention in mentions
            ):
                direct_other = False

        if direct_self:
            evidence.add("self")
        if group_target:
            evidence.add("workgroup")
        if direct_other:
            evidence.add("other")

    if "other" in evidence and ("self" in evidence or "workgroup" in evidence):
        return "ambiguous"
    if "other" in evidence:
        return "outside_scope"
    if "self" in evidence or "workgroup" in evidence:
        return "eligible"
    return "unresolved"


def _scope_fallback_context_is_complete(
    event: dict[str, Any],
    plan: dict[str, Any],
    candidate_plan: dict[str, Any],
    semantic_fallback: Any,
    self_user: dict[str, Any],
) -> bool:
    """Allow semantic escalation only when source facts and program routing are present."""
    source = str(event.get("text") or event.get("content") or "")
    try:
        fallback_limit = int(getattr(semantic_fallback, "max_source_chars", JevResolver.MAX_SOURCE_CHARS))
    except (TypeError, ValueError, OverflowError):
        return False
    if (
        not _has_substantive_jev_text(source)
        or len(source) > min(JevResolver.MAX_SOURCE_CHARS, fallback_limit)
    ):
        return False

    missing = set(plan.get("missing_fields") or [])
    if missing & {"referenced_content", "source_identity", "source_group", "source_text"}:
        return False
    warnings = set(plan.get("warnings") or []) | set(candidate_plan.get("warnings") or [])
    if warnings & {
        "self_authorship_unverified",
        "self_authored_requires_review",
        "identity_unverified",
        "source_identity_unverified",
        "source_truncated",
        "jev_source_truncated",
        "attachment_content_unavailable",
    }:
        return False

    if not all(
        _as_text(plan.get(key))
        for key in ("conversation_id", "message_id", "source_group")
    ):
        return False
    # Route ownership is program-selected. If the applicable routing probe has
    # no owner, semantic reasoning cannot supply one.
    if not _as_text(candidate_plan.get("route_owner")):
        return False
    if _has_explicit_other_assignment(event, self_user):
        return False
    return True


def _date_role_fallback_candidates_if_eligible(
    event: dict[str, Any],
    plan: dict[str, Any],
    candidate_plan: dict[str, Any],
    date_candidates: Sequence[dict[str, Any]],
    resolver: Any,
    self_user: dict[str, Any],
    *,
    business_candidate_query_complete: bool | None,
    business_candidate_pool_issue: str | None,
    business_candidate_overflow: bool,
    date_candidate_error: str | None,
) -> list[dict[str, Any]]:
    """Return parser-unknown dates only after Jev leaves role confidence unresolved."""
    if (
        plan.get("decision") != "REVIEW"
        or plan.get("reason") != "jev_v2_date_role_unverified"
        or business_candidate_query_complete is not True
        or business_candidate_pool_issue
        or business_candidate_overflow
        or date_candidate_error
    ):
        return []
    source_text = str(event.get("text") or event.get("content") or "")
    unknown = [candidate for candidate in date_candidates if candidate.get("role") == "unknown"]
    if (
        not unknown
        or has_competing_assessment_cutoff(source_text)
        or bool(event.get("resource_refs"))
        or re.search(r"(?i)(?:\[文件\]|\[附件\]|\[图片消息\]|\bfileId\b|\bmediaId\b)", source_text)
        or re.search(r"(?:见|查看|详见|参见|按照).{0,24}(?:附件|文件|群内上下文|R列)", source_text)
    ):
        return []
    if not _scope_fallback_context_is_complete(event, plan, candidate_plan, resolver, self_user):
        return []
    if not _as_text(plan.get("route_module")) or not _as_text(plan.get("route_owner")):
        return []
    if set(plan.get("missing_fields") or []) - {"due_date", "title"}:
        return []
    for candidate in unknown:
        start, end = candidate.get("source_span_start"), candidate.get("source_span_end")
        span = candidate.get("source_span")
        if (
            not isinstance(candidate.get("key"), str)
            or not re.fullmatch(r"date_[0-9]{1,3}", str(candidate.get("key")))
            or not isinstance(span, str)
            or not span.strip()
            or isinstance(start, bool)
            or not isinstance(start, int)
            or isinstance(end, bool)
            or not isinstance(end, int)
            or start < 0
            or end <= start
            or end > len(source_text)
            or source_text[start:end] != span
        ):
            return []

    semantic = plan.get("semantic_evidence")
    if not isinstance(semantic, dict):
        return []
    action = semantic.get("prospective_action")
    followup = semantic.get("needs_follow_up")
    scope = semantic.get("applies_to_workgroup")
    def probability(value: Any) -> float | None:
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            return None
        return float(value)
    action_value, followup_value, scope_value = map(probability, (action, followup, scope))
    if action_value is None or action_value < 0.75 or followup_value is None or followup_value < 0.75:
        return []
    if scope_value is None or (
        scope_value < 0.75 and semantic.get("scope_basis") != "explicit_workgroup_or_self_target"
    ):
        return []

    judgments = semantic.get("date_role_judgments")
    if not isinstance(judgments, list):
        return []
    by_key: dict[str, dict[str, Any]] = {}
    for item in judgments:
        if isinstance(item, dict) and isinstance(item.get("candidate_key"), str):
            if item["candidate_key"] in by_key:
                return []
            by_key[item["candidate_key"]] = item
    for candidate in unknown:
        item = by_key.get(str(candidate.get("key")))
        confidence = item.get("confidence") if isinstance(item, dict) else None
        margin = item.get("margin") if isinstance(item, dict) else None
        unresolved_confidence = (
            not isinstance(confidence, bool)
            and isinstance(confidence, (int, float))
            and math.isfinite(float(confidence))
            and (
                float(confidence) < JevResolver.CHOICE_CONFIDENCE_MIN
                or (
                    not isinstance(margin, bool)
                    and isinstance(margin, (int, float))
                    and math.isfinite(float(margin))
                    and float(margin) < JevResolver.CHOICE_MARGIN_MIN
                )
            )
        )
        if (
            not isinstance(item, dict)
            or item.get("parser_role") != "unknown"
            or item.get("resolved_role") != "unknown"
            or item.get("role_origin") != "parser_unresolved"
            or item.get("model_choice") != "uncertain"
            or not unresolved_confidence
        ):
            return []
    return unknown


def load_existing_actions(
    adapter: WorkQueryAdapter,
    runtime_config: dict[str, Any],
) -> list[dict[str, Any]]:
    """Load only action identity metadata for currently enabled scan groups."""
    actions: list[dict[str, Any]] = []
    seen_conversations: set[str] = set()
    for row in runtime_config.get("config_group", []):
        if _as_text(row.get("purpose")) != "todo_scan":
            continue
        conversation_id = _as_text(row.get("conversation_id"))
        if not conversation_id or conversation_id in seen_conversations:
            continue
        seen_conversations.add(conversation_id)
        actions.extend(read_all(adapter, "actions", conversation_id=conversation_id))
    return actions


def _contextual_retrieval_source_title(
    event: dict[str, Any],
    subject: Any,
) -> tuple[bool, str | None]:
    """Match a linked subject to the local unfinished-inspection retrieval instruction."""
    source = _as_text(event.get("text"))
    retrieval_matches = list(_RETRIEVAL_INSTRUCTION_RE.finditer(source))
    if not retrieval_matches:
        return False, None
    if len(retrieval_matches) != 1 or not isinstance(subject, str):
        return True, None

    match = retrieval_matches[0]
    prefix = match.group("meeting") + "取回铁炉西送检"
    if not subject.startswith(prefix):
        return True, None
    item = subject[len(prefix):].strip()
    group = _as_text(event.get("group"))
    if not item or _clean_inspection_item(item, group) != item:
        return True, None
    instruction = _retrieval_instruction(source, item)
    if instruction is None:
        return True, None
    title = prefix + item
    if not 0 < len(title) <= 80:
        return True, None
    return True, title


def load_business_candidate_context(
    adapter: WorkQueryAdapter,
    runtime_config: dict[str, Any],
    self_user: dict[str, Any],
    actions: Sequence[dict[str, Any]],
    *,
    dws: DwsClient | None = None,
    max_candidates: int = JevResolver.MAX_BUSINESS_CANDIDATES,
) -> tuple[list[dict[str, Any]], str | None]:
    """Hydrate a small set of linked tasks through exact source-message keys."""
    group_by_conversation = {
        _as_text(row.get("conversation_id")): _as_text(row.get("group_name"))
        for row in runtime_config.get("config_group", [])
        if _as_text(row.get("purpose")) == "todo_scan"
        and _as_text(row.get("conversation_id"))
        and _as_text(row.get("group_name"))
    }
    eligible = [
        row for row in actions
        if isinstance(row, dict)
        and _as_text(row.get("conversation_id")) in group_by_conversation
        and _as_text(row.get("message_id"))
        and _as_text(row.get("external_task_id"))
        and _as_text(row.get("status")).upper() in {"EXTERNAL_CREATED", "SUCCESS"}
    ]
    eligible.sort(key=lambda row: _as_text(row.get("created_at")), reverse=True)
    max_candidates = max(1, min(int(max_candidates), JevResolver.MAX_BUSINESS_CANDIDATES))
    eligible = eligible[:max_candidates]

    hydrated: list[dict[str, Any]] = []
    for action in eligible:
        conversation_id = _as_text(action.get("conversation_id"))
        message_id = _as_text(action.get("message_id"))
        try:
            source_rows = read_all(
                adapter,
                "messages",
                conversation_id=conversation_id,
                message_id=message_id,
            )
        except Exception:
            return [], "business_candidate_source_unavailable"
        exact_rows = [
            row for row in source_rows
            if _as_text(row.get("conversation_id")) == conversation_id
            and _as_text(row.get("message_id")) == message_id
        ]
        if len(exact_rows) != 1:
            return [], "business_candidate_source_unavailable"
        source = exact_rows[0]
        text = _as_text(source.get("text"))
        message_time = _as_text(source.get("message_time"))
        if not text or not message_time:
            return [], "business_candidate_source_unavailable"
        try:
            run_date = _local_datetime(message_time).date()
        except (TypeError, ValueError):
            return [], "business_candidate_source_unavailable"
        event = {
            "conversation_id": conversation_id,
            "message_id": message_id,
            "group": group_by_conversation[conversation_id],
            "sender_id": source.get("sender_id"),
            "sender": source.get("sender_name"),
            "create_time": message_time,
            "text": text,
        }
        probe = plan_batch(
            {
                "config": runtime_config,
                "self_user": self_user,
                "existing_actions": [],
                "messages": [event],
            },
            mode="test",
            run_date=run_date,
        )["plans"][0]
        if "semantic_intent_needs_llm" in (probe.get("warnings") or []):
            route_event = dict(event)
            route_event["_llm_semantic_intent"] = "follow_up"
            probe = plan_batch(
                {
                    "config": runtime_config,
                    "self_user": self_user,
                    "existing_actions": [],
                    "messages": [route_event],
                },
                mode="test",
                run_date=run_date,
            )["plans"][0]
        if not probe.get("route_module") or not probe.get("route_owner"):
            retrieval_matches = list(_RETRIEVAL_INSTRUCTION_RE.finditer(text))
            if not retrieval_matches:
                continue
            if len(retrieval_matches) != 1 or dws is None:
                return [], "business_candidate_context_unavailable"
            external_task_id = action.get("external_task_id")
            if not isinstance(external_task_id, str) or not external_task_id.strip():
                return [], "business_candidate_context_unavailable"
            try:
                detail = dws.get_todo(external_task_id)
            except Exception:
                return [], "business_candidate_context_unavailable"
            if not isinstance(detail, dict):
                return [], "business_candidate_context_unavailable"
            task_id = detail.get("taskId")
            subject = detail.get("subject")
            is_done = detail.get("isDone")
            executor_infos = detail.get("executorInfos")
            contextual_retrieval, expected_title = _contextual_retrieval_source_title(
                event, subject
            )
            executor_name = _as_text(self_user.get("name"))
            remote_executor_name = (
                executor_infos[0].get("name")
                if isinstance(executor_infos, list)
                and len(executor_infos) == 1
                and isinstance(executor_infos[0], dict)
                else None
            )
            if (
                not contextual_retrieval
                or not expected_title
                or not isinstance(task_id, str)
                or task_id != external_task_id
                or not isinstance(subject, str)
                or subject != expected_title
                or not isinstance(is_done, bool)
                or not isinstance(executor_infos, list)
                or len(executor_infos) != 1
                or not isinstance(executor_infos[0], dict)
                or not isinstance(remote_executor_name, str)
                or not remote_executor_name.strip()
                or remote_executor_name != executor_name
                or not executor_name
            ):
                return [], "business_candidate_context_unavailable"
            if is_done:
                continue
            hydrated.append({
                **action,
                "route_module": "workgroup_follow_up",
                "route_owner": executor_name,
                "due_date": None,
                "title": subject,
                "source_text": f"{text}\n关联任务标题：{subject}",
                "message_time": message_time,
            })
            continue
        hydrated.append({
            **action,
            "route_module": probe.get("route_module"),
            "route_owner": probe.get("route_owner"),
            "due_date": probe.get("due_date"),
            "source_text": text,
            "message_time": message_time,
        })
    return hydrated, None


class OneShotHistoryPoller:
    """Read configured group history once; process only after complete reads."""

    def __init__(
        self,
        *,
        adapter: WorkQueryAdapter,
        reader: DingTalkReader,
        dws: DwsClient,
        writer: WorkWriter | None = None,
        resolver: Any = None,
        mode: str = "shadow",
        write_ledger: bool = False,
        allow_external_writes: bool = False,
        processor_factory: Callable[..., Any] | None = None,
        now_factory: Callable[[], datetime] | None = None,
        semantic_fallback: Any = None,
        date_role_fallback_resolver: Any = None,
        title_resolver: Any = None,
        supervised_rollout: bool = False,
        not_before: datetime | None = None,
        daily_recovery: bool = False,
        todo_lifecycle_events: Iterable[dict[str, Any]] = (),
        history_replay_state: Any = None,
    ):
        self.adapter = adapter
        self.reader = reader
        self.dws = dws
        self.writer = writer
        self.resolver = resolver
        self.mode = mode
        self.write_ledger = bool(write_ledger)
        self.allow_external_writes = bool(allow_external_writes)
        self.semantic_fallback = semantic_fallback
        self.date_role_fallback_resolver = date_role_fallback_resolver
        self.title_resolver = title_resolver
        self.supervised_rollout = bool(supervised_rollout)
        self.not_before = parse_not_before(not_before) if not_before is not None else None
        self.daily_recovery = bool(daily_recovery)
        self.todo_lifecycle_events = [validate_queued_todo_event(row) for row in todo_lifecycle_events]
        self.history_replay_state = history_replay_state
        self.parity_error = False
        try:
            self.parity_ledger = DwsParityLedger.from_environment()
        except DwsParityError:
            self.parity_ledger = None
            self.parity_error = True
        self.processor_factory = processor_factory or EventProcessor
        self.now_factory = now_factory or (lambda: datetime.now(CHINA_TZ))

    def run_once(self) -> dict[str, Any]:
        now = self.now_factory()
        if now.tzinfo is None:
            now = now.replace(tzinfo=CHINA_TZ)
        now = now.astimezone(CHINA_TZ)
        initial_cutoff = self.not_before.isoformat() if self.not_before is not None else None
        recovery_window = now.time().hour == 17 and now.time().minute >= 30
        if not within_execution_window(now) and not (self.daily_recovery and recovery_window):
            return {
                "status": "SKIPPED", "reason": "outside_execution_window", "messages": 0,
                "initial_cutoff": initial_cutoff,
            }
        if self.not_before is not None and self.not_before > now and not self.daily_recovery:
            return {
                "status": "SKIPPED", "reason": "not_before_after_now", "messages": 0,
                "initial_cutoff": initial_cutoff,
            }

        pending_history_events: dict[str, dict[str, dict[str, str]]] = {}
        pending_history_errors: dict[str, list[str]] = {}
        if self.history_replay_state is not None:
            pending_history_events = self.history_replay_state.history_event_snapshot()
            pending_history_errors = self.history_replay_state.history_event_errors_snapshot()
        if any(pending_history_errors.values()):
            return {
                "status": "REVIEW",
                "reason": "history_event_identity_unavailable",
                "messages": 0,
                "writes": 0,
                "initial_cutoff": initial_cutoff,
                "history_replay_complete": False,
            }

        runtime = self.adapter.query({"view": "runtime_config"})
        config = runtime.get("config") or {}
        gate = calendar_gate(config, now.date())
        if not gate.get("allowed"):
            return {
                "status": "SKIPPED",
                "reason": "calendar_gate_closed" if self.daily_recovery else gate.get("reason", "calendar_gate_closed"),
                "calendar": gate, "messages": 0,
                "initial_cutoff": initial_cutoff,
                "daily_recovery": self.daily_recovery,
            }
        if self.not_before is not None and self.not_before > now:
            return {
                "status": "SKIPPED", "reason": "not_before_after_now", "messages": 0,
                "initial_cutoff": initial_cutoff,
                "daily_recovery": self.daily_recovery,
            }

        scan_rows = read_all(self.adapter, "scan_state")
        specs = group_specs(config, scan_rows)
        if not specs or any(not _as_text(spec.get("conversation_id")) for spec in specs):
            return {
                "status": "REVIEW",
                "reason": "configured_conversation_ids_missing",
                "groups_attempted": len(specs),
                "groups_complete": 0,
                "messages": 0,
                "writes": 0,
                "initial_cutoff": initial_cutoff,
            }

        configured_ids = {_as_text(spec.get("conversation_id")) for spec in specs}
        replay_group_ids = {group_id for group_id, events in pending_history_events.items() if events}
        if replay_group_ids - configured_ids:
            return {
                "status": "REVIEW",
                "reason": "history_event_scope_missing",
                "messages": 0,
                "writes": 0,
                "initial_cutoff": initial_cutoff,
                "history_replay_complete": False,
            }
        replay_times_by_group: dict[str, list[str]] = {}
        replay_hashes: set[str] = set()
        for group_id, events in pending_history_events.items():
            for event_hash, event in events.items():
                event_time = event.get("event_time")
                try:
                    parsed_event_time = _local_datetime(event_time)
                except (TypeError, ValueError):
                    return {
                        "status": "REVIEW",
                        "reason": "history_event_time_invalid",
                        "messages": 0,
                        "writes": 0,
                        "initial_cutoff": initial_cutoff,
                        "history_replay_complete": False,
                    }
                if (
                    self.not_before is not None
                    and parsed_event_time < self.not_before.astimezone(CHINA_TZ).replace(tzinfo=None)
                ):
                    return {
                        "status": "REVIEW",
                        "reason": "history_event_before_collection_floor",
                        "messages": 0,
                        "writes": 0,
                        "initial_cutoff": initial_cutoff,
                        "history_replay_complete": False,
                    }
                replay_times_by_group.setdefault(group_id, []).append(event_time)
                replay_hashes.add(event_hash)

        # Only the current authoritative config supplies conversation scope and
        # display names; scan_state contributes watermarks, never scope.
        for spec in specs:
            spec["group_name"] = spec["configured_group_name"]
        messages_by_source: dict[tuple[str, str], dict[str, Any]] = {}
        reports: list[dict[str, Any]] = []
        read_errors: list[dict[str, str]] = []
        page_order_provenance_complete = True
        parity_history_messages = 0
        parity_history_scans = 0
        for spec in specs:
            conversation_id = _as_text(spec["conversation_id"])
            last_msg_time = spec.get("last_msg_time")
            replay_times = replay_times_by_group.get(conversation_id, [])
            if replay_times:
                earliest_replay_time = min(replay_times, key=_local_datetime)
                if not last_msg_time or _local_datetime(earliest_replay_time) < _local_datetime(last_msg_time):
                    last_msg_time = earliest_replay_time
            try:
                result = self.reader.read_group(
                    conversation_id=conversation_id,
                    group_name=spec["group_name"],
                    last_msg_time=last_msg_time,
                    end=now,
                    not_before=self.not_before,
                )
            except Exception as exc:
                read_errors.append({
                    "conversation_id": conversation_id,
                    "error": "history_read_failed:" + type(exc).__name__,
                })
                continue
            if result.get("complete") is not True:
                read_errors.append({
                    "conversation_id": conversation_id,
                    "error": "history_read_incomplete",
                })
                continue
            if self.parity_ledger is not None:
                try:
                    parity_history_messages += self.parity_ledger.record_history(
                        conversation_id,
                        result.get("messages") or [],
                        scan_start=result.get("start"),
                        scan_end=result.get("end"),
                    )
                    parity_history_scans += 1
                except DwsParityError:
                    self.parity_ledger = None
                    self.parity_error = True
            if result.get("source_order_provenance") != COMPLETE_DWS_PAGE_ORDER:
                page_order_provenance_complete = False
            group_messages = result.get("messages") or []
            reports.append({"conversation_id": conversation_id, "messages": len(group_messages)})
            for message in group_messages:
                if not isinstance(message, dict):
                    read_errors.append({
                        "conversation_id": conversation_id,
                        "error": "history_message_invalid",
                    })
                    continue
                message_conversation = _as_text(message.get("conversation_id"))
                message_id = _as_text(message.get("message_id"))
                create_time = _as_text(message.get("create_time"))
                if message_conversation != conversation_id or not message_id or not create_time:
                    read_errors.append({
                        "conversation_id": conversation_id,
                        "error": "history_message_identity_incomplete",
                    })
                    continue
                try:
                    message_time = _local_datetime(create_time)
                except (TypeError, ValueError):
                    read_errors.append({
                        "conversation_id": conversation_id,
                        "error": "history_message_time_invalid",
                    })
                    continue
                if self.not_before is not None and message_time.replace(tzinfo=CHINA_TZ) < self.not_before:
                    continue
                message = dict(message)
                message["group"] = spec["group_name"]
                source_key = (conversation_id, message_id)
                previous = messages_by_source.get(source_key)
                if previous is not None and (
                    previous.get("text") != message.get("text")
                    or previous.get("create_time") != message.get("create_time")
                ):
                    read_errors.append({
                        "conversation_id": conversation_id,
                        "error": "duplicate_source_conflict",
                    })
                    continue
                messages_by_source.setdefault(source_key, message)

        if read_errors:
            return {
                "status": "REVIEW",
                "reason": "history_not_complete_or_valid",
                "groups_attempted": len(specs),
                "groups_complete": len(reports),
                "messages_read": sum(report["messages"] for report in reports),
                "messages": 0,
                "writes": 0,
                "initial_cutoff": initial_cutoff,
                "errors": read_errors,
                "history_replay_complete": False,
            }

        if replay_hashes:
            covered_event_hashes = {
                hashlib.sha256(f"{conversation_id}\0{message_id}".encode("utf-8")).hexdigest()
                for conversation_id, message_id in messages_by_source
            }
            if not replay_hashes.issubset(covered_event_hashes):
                return {
                    "status": "REVIEW",
                    "reason": "history_event_not_in_complete_scan",
                    "groups_attempted": len(specs),
                    "groups_complete": len(reports),
                    "messages_read": sum(report["messages"] for report in reports),
                    "messages": 0,
                    "writes": 0,
                    "initial_cutoff": initial_cutoff,
                    "history_replay_complete": False,
                }

        existing_actions = load_existing_actions(self.adapter, config)
        self_user = self.dws.get_self()
        business_candidate_pool_issue: str | None = None
        is_new_policy_v2 = (
            isinstance(self.resolver, JevV2Resolver)
            and getattr(self.resolver, "new_policy", False)
        )
        if isinstance(self.resolver, JevResolver) or is_new_policy_v2:
            business_context, business_candidate_pool_issue = load_business_candidate_context(
                self.adapter, config, self_user, existing_actions, dws=self.dws
            )
            context_by_source = {
                (_as_text(row.get("conversation_id")), _as_text(row.get("message_id"))): row
                for row in business_context
            }
            for action in existing_actions:
                context = context_by_source.get(
                    (_as_text(action.get("conversation_id")), _as_text(action.get("message_id")))
                )
                if context:
                    for key in ("route_module", "route_owner", "due_date", "source_text", "message_time"):
                        action[key] = context.get(key)
        processor = self.processor_factory(
            config,
            self_user,
            mode=self.mode,
            write_ledger=self.write_ledger,
            allow_external_writes=self.allow_external_writes,
            dws=self.dws,
            writer=self.writer,
            llm_resolver=self.resolver,
            existing_actions=existing_actions,
            business_candidate_pool_issue=business_candidate_pool_issue,
            business_candidate_query_complete=(
                business_candidate_pool_issue is None if is_new_policy_v2 else None
            ),
            semantic_fallback=self.semantic_fallback,
            date_role_fallback_resolver=self.date_role_fallback_resolver,
            title_resolver=self.title_resolver,
            supervised_rollout=self.supervised_rollout,
            source_order_provenance=(
                COMPLETE_DWS_PAGE_ORDER if page_order_provenance_complete else "unverified_input_order"
            ),
        )
        messages = sorted(
            messages_by_source.values(),
            key=lambda item: _local_datetime(item["create_time"]),
        )
        plans = processor.process_batch(messages)
        stats = dict(processor.stats)
        todo_sync: dict[str, Any] | None = None
        if self.todo_lifecycle_events:
            if not (self.mode == "production" and self.write_ledger and self.writer is not None):
                todo_sync = {
                    "processed": 0, "ignored": 0,
                    "errors": ["todo_sync_requires_production_ledger"],
                }
            else:
                todo_sync = sync_todo_lifecycle_events(
                    self.todo_lifecycle_events, existing_actions, dws=self.dws, writer=self.writer,
                )
            stats["errors"] = int(stats.get("errors") or 0) + len(todo_sync["errors"])
        return {
            "status": "REVIEW" if stats.get("errors") else "COMPLETED",
            "groups_attempted": len(specs),
            "groups_complete": len(reports),
            "messages_read": sum(report["messages"] for report in reports),
            "messages": len(messages),
            "plans": plans,
            "stats": stats,
            "writes": stats.get("ledger_writes", 0),
            "external_todos_created": stats.get("external_todos_created", 0),
            "group_messages_sent": 0,
            "initial_cutoff": initial_cutoff,
            "history_replay_complete": True,
            "todo_lifecycle_sync": (
                {key: todo_sync[key] for key in ("processed", "ignored", "errors")}
                if todo_sync is not None else None
            ),
            "parity_observer": (
                {
                    "enabled": self.parity_ledger is not None,
                    "history_messages_added": parity_history_messages,
                    "complete_history_scans": parity_history_scans,
                    "error": self.parity_error,
                }
                if self.parity_ledger is not None or self.parity_error else None
            ),
        }


def _sender_name(value: Any) -> str | None:
    if isinstance(value, dict):
        return _as_text(value.get("name") or value.get("nick") or value.get("senderName")) or None
    return _as_text(value) or None


def _find_message_object(payload: Any) -> dict[str, Any] | None:
    if isinstance(payload, dict):
        if any(key in payload for key in ("messageId", "message_id", "openMessageId", "open_message_id")) and any(
            key in payload
            for key in ("conversationId", "conversation_id", "openConversationId", "open_conversation_id")
        ):
            return payload
        for key in ("data", "payload", "body", "event", "content"):
            if key in payload:
                found = _find_message_object(payload[key])
                if found is not None:
                    return found
        for value in payload.values():
            found = _find_message_object(value)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for value in payload:
            found = _find_message_object(value)
            if found is not None:
                return found
    return None


def normalize_event(payload: dict[str, Any]) -> dict[str, Any]:
    message = _find_message_object(payload)
    if message is None:
        raise EventFormatError("event_has_no_message_identity")
    text = message.get("text")
    if text is None:
        text = message.get("content")
    if isinstance(text, dict):
        text = text.get("content") or text.get("text") or ""
    conversation_id = _as_text(
        message.get("conversationId")
        or message.get("conversation_id")
        or message.get("openConversationId")
        or message.get("open_conversation_id")
    )
    message_id = _as_text(
        message.get("messageId")
        or message.get("message_id")
        or message.get("openMessageId")
        or message.get("open_message_id")
    )
    if not conversation_id or not message_id:
        raise EventFormatError("event_has_incomplete_message_identity")
    return {
        "conversation_id": conversation_id,
        "message_id": message_id,
        "group": _as_text(message.get("group") or message.get("groupName")),
        "sender_id": _as_text(
            message.get("senderId")
            or message.get("sender_id")
            or message.get("senderOpenDingTalkId")
            or message.get("sender_open_dingtalk_id")
        ) or None,
        "sender": _sender_name(
            message.get("sender") or message.get("senderName") or message.get("sender_name")
        ),
        "create_time": _as_text(
            message.get("createTime")
            or message.get("create_time")
            or message.get("time")
        ),
        "text": _as_text(text),
        "at_users": message.get("atUsers") or message.get("at_users") or message.get("mentions") or [],
        "msg_type": message.get("msgType") or message.get("msg_type"),
        "resource_refs": message.get("resourceRefs") or message.get("resource_refs") or [],
        "quoted_message": message.get("quotedMessage") or message.get("quoted_message"),
        "reply_to_message_id": _as_text(
            message.get("replyToMessageId") or message.get("reply_to_message_id")
        ) or None,
        "quoted_message_id": _as_text(
            message.get("quotedMessageId") or message.get("quoted_message_id")
        ) or None,
        "related_message_id": _as_text(
            message.get("relatedMessageId") or message.get("related_message_id")
        ) or None,
        "thread_id": _as_text(
            message.get("threadId") or message.get("thread_id")
            or message.get("openConvThreadId") or message.get("open_conv_thread_id")
        ) or None,
        "related_contexts": list(message.get("related_contexts") or [])
        if isinstance(message.get("related_contexts") or [], list) else [],
        "context_missing_message_ids": list(message.get("context_missing_message_ids") or [])
        if isinstance(message.get("context_missing_message_ids") or [], list) else [],
        "context_status": _as_text(message.get("context_status") or "complete"),
        "thread_context_status": _as_text(message.get("thread_context_status") or "complete"),
        "thread_context_thread_id": _as_text(message.get("thread_context_thread_id")),
        "raw_payload": _strip_untrusted_private_flags(payload),
        "event_type": _as_text(payload.get("event_type") or payload.get("eventType")),
    }


def dedupe_events(events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for event in events:
        key = (_as_text(event.get("conversation_id")), _as_text(event.get("message_id")))
        if not all(key) or key in seen:
            continue
        seen.add(key)
        result.append(event)
    return result


def should_process_group(conversation_id: str, group_map: dict[str, str]) -> bool:
    return _as_text(conversation_id) in group_map


def _local_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=CHINA_TZ)
    return parsed.astimezone(CHINA_TZ).replace(tzinfo=None)


def _possible_cross_group_repeat(event: dict[str, Any], prior_messages: list[dict[str, Any]]) -> bool:
    """Hold an identical, recently repeated notice for review before another write."""
    sender_id = _as_text(event.get("sender_id"))
    conversation_id = _as_text(event.get("conversation_id"))
    text = _as_text(event.get("text"))
    if not sender_id or not conversation_id or not text or len(text) > 4000:
        return False

    def fingerprint(value: str) -> str:
        value = unicodedata.normalize("NFKC", value)
        return re.sub(r"\s+", "", re.sub(r"@[^\s@]+", "", value))

    normalized = fingerprint(text)
    if len(normalized) < 20:
        return False
    try:
        current_time = _local_datetime(event.get("create_time"))
    except (TypeError, ValueError):
        return False
    for prior in prior_messages:
        if (prior.get("planned_decision") != "CREATE"
                or _as_text(prior.get("sender_id")) != sender_id
                or not _as_text(prior.get("conversation_id"))
                or _as_text(prior.get("conversation_id")) == conversation_id):
            continue
        prior_text = _as_text(prior.get("text"))
        if not prior_text or len(prior_text) > 4000 or fingerprint(prior_text) != normalized:
            continue
        try:
            elapsed = (current_time - _local_datetime(prior.get("create_time"))).total_seconds()
        except (TypeError, ValueError):
            continue
        if 0 <= elapsed <= 600:
            return True
    return False


def _max_cursor(events: Iterable[dict[str, Any]]) -> tuple[str, str] | None:
    usable = [event for event in events if event.get("create_time") and event.get("message_id")]
    if not usable:
        return None
    chosen = max(usable, key=lambda item: (_local_datetime(item["create_time"]), item["message_id"]))
    return _local_datetime(chosen["create_time"]).isoformat(sep=" "), str(chosen["message_id"])


def _resource_ids(message: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for ref in message.get("resource_refs") or []:
        if not isinstance(ref, dict):
            continue
        resource_id = ref.get("resourceId") or ref.get("resource_id")
        resource_type = ref.get("resourceIdType") or ref.get("resource_id_type") or ref.get("type")
        download = ref.get("download") or {}
        arguments = download.get("arguments") if isinstance(download, dict) else {}
        resource_id = resource_id or (arguments.get("resource-id") if isinstance(arguments, dict) else None)
        resource_type = resource_type or (arguments.get("type") if isinstance(arguments, dict) else None)
        if resource_id and (not resource_type or str(resource_type) in {"mediaId", "image", "file"}):
            values.append(str(resource_id))
    return list(dict.fromkeys(values))


def _image_only_unreadable(event: dict[str, Any]) -> bool:
    text = _as_text(event.get("text") or event.get("content"))
    text = re.sub(r"\[图片消息\](?:\([^)]*\))?", " ", text)
    text = re.sub(r"(?:图片|照片|image)\s*[:：]?", " ", text, flags=re.I)
    text = re.sub(r"@[^\s]+(?:\([^)]*\))?", " ", text)
    if re.sub(r"[\s\W_]+", "", text, flags=re.UNICODE):
        return False

    def contains_image(value: Any) -> bool:
        if isinstance(value, dict):
            kind = _as_text(value.get("resourceIdType") or value.get("resource_id_type") or value.get("type")).casefold()
            content_type = _as_text(value.get("contentType") or value.get("content_type") or value.get("mimeType")).casefold()
            filename = _as_text(value.get("fileName") or value.get("file_name") or value.get("name")).casefold()
            if kind in {"image", "picture", "photo"} or content_type.startswith("image/") or filename.endswith((".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp")):
                return True
            return any(contains_image(item) for item in value.values())
        if isinstance(value, list):
            return any(contains_image(item) for item in value)
        return False

    message_type = _as_text(event.get("msg_type")).casefold()
    return "image" in message_type or "picture" in message_type or contains_image(event.get("resource_refs"))


def _private_resource_directory(path: Path) -> Path:
    if path.is_symlink():
        raise DwsError("resource_workspace_symlink_rejected")
    try:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        path.chmod(0o700)
    except OSError as exc:
        raise DwsError("resource_workspace_unavailable") from exc
    if path.is_symlink() or not path.is_dir():
        raise DwsError("resource_workspace_invalid")
    return path.resolve(strict=True)


def _download_integrity(path_value: Any, resource_root: Path) -> dict[str, Any]:
    path = Path(str(path_value or ""))
    if not path_value or path.is_symlink():
        raise DwsError("downloaded_resource_path_invalid")
    root = _private_resource_directory(resource_root)
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise DwsError("downloaded_resource_missing") from exc
    if root not in resolved.parents or resolved.is_symlink() or not resolved.is_file():
        raise DwsError("downloaded_resource_path_outside_workspace")
    try:
        resolved.chmod(0o600)
    except OSError as exc:
        raise DwsError("downloaded_resource_permissions_unavailable") from exc
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(resolved, flags)
    except OSError as exc:
        raise DwsError("downloaded_resource_open_failed") from exc
    digest = hashlib.sha256()
    size = 0
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            os.close(fd)
            raise DwsError("downloaded_resource_not_regular_file")
        with os.fdopen(fd, "rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                size += len(chunk)
                digest.update(chunk)
    except OSError as exc:
        raise DwsError("downloaded_resource_read_failed") from exc
    return {
        "sha256": digest.hexdigest(),
        "size_bytes": size,
        "relative_path": resolved.relative_to(root).as_posix(),
        "file_name": resolved.name,
    }


def _append_download_ledger(resource_root: Path, record: dict[str, Any]) -> None:
    root = _private_resource_directory(resource_root)
    ledger = root / "resource-download-ledger.jsonl"
    if ledger.is_symlink():
        raise DwsError("download_ledger_symlink_rejected")
    payload = json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(ledger, flags, 0o600)
        os.fchmod(fd, 0o600)
        try:
            offset = 0
            while offset < len(payload):
                written = os.write(fd, payload[offset:])
                if written <= 0:
                    raise OSError("short ledger write")
                offset += written
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError as exc:
        raise DwsError("download_ledger_write_failed") from exc


def recall_business_candidates(
    plan: dict[str, Any],
    existing_actions: Sequence[dict[str, Any]],
    *,
    limit: int = JevResolver.MAX_BUSINESS_CANDIDATES,
    allow_simulated: bool = False,
) -> tuple[list[dict[str, str]], bool]:
    """Recall only externally linked tasks in the exact deterministic route bucket."""
    module = str(plan.get("route_module") or "").strip()
    owner = str(plan.get("route_owner") or "").strip()
    if not module or not owner:
        return [], False
    candidates: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    overflow = False
    for row in existing_actions:
        if not isinstance(row, dict):
            continue
        business_key = str(row.get("business_key") or "").strip()
        linked_id = str(row.get("external_task_id") or "").strip()
        status = str(row.get("status") or "").upper()
        is_simulated_eligible = (
            allow_simulated is True
            and row.get("simulated") is True
            and bool(str(row.get("simulation_id") or "").strip())
            and status == "SUCCESS"
        )
        if not business_key and not linked_id:
            continue
        seen_identity = (
            ("external_task", linked_id)
            if linked_id
            else ("business_key", business_key)
        )
        if (
            str(row.get("route_module") or "").strip() != module
            or str(row.get("route_owner") or "").strip() != owner
            or not (linked_id or is_simulated_eligible)
            or status not in {"EXTERNAL_CREATED", "SUCCESS"}
            or seen_identity in seen
        ):
            continue
        text = str(row.get("source_text") or row.get("message_text") or row.get("text") or "").strip()
        create_time = str(row.get("message_time") or row.get("create_time") or "").strip()
        if not text or not create_time:
            continue
        if len(candidates) >= max(1, min(int(limit), JevResolver.MAX_BUSINESS_CANDIDATES)):
            overflow = True
            continue
        seen.add(seen_identity)
        candidate = {
            "key": f"business_{len(candidates)}",
            "text": text[:JevResolver.MAX_BUSINESS_TEXT_CHARS],
            "create_time": create_time[:64],
        }
        if len(text) > JevResolver.MAX_BUSINESS_TEXT_CHARS:
            # Jev keeps its established bounded candidate view, while semantic
            # fallback refuses to claim a faithful same/different judgment from
            # this incomplete local candidate.
            candidate["_semantic_source_truncated"] = True
        candidates.append(candidate)
    return candidates, overflow


def validate_jev_evaluation(
    completion: Any,
    date_candidates: Sequence[dict[str, Any]],
    business_candidates: Sequence[dict[str, Any]] = (),
) -> dict[str, Any]:
    """Enforce the Jev allowlist and map candidate keys back to local dates."""
    allowed_fields = {
        "action_probability", "action_signal", "semantic_intent", "item_status",
        "item_status_confidence", "item_status_margin", "due_candidate_key",
        "due_date_confidence", "due_date_margin", "due_date", "due_iso",
        "business_matches", "decision_boundary", "usage",
    }
    if not isinstance(completion, dict) or set(completion) - allowed_fields:
        raise ValueError("jev_result_contains_nonsemantic_fields")
    probability = completion.get("action_probability")
    if (
        isinstance(probability, bool)
        or not isinstance(probability, (int, float))
        or not math.isfinite(float(probability))
        or not 0 <= float(probability) <= 1
    ):
        raise ValueError("jev_action_probability_invalid")
    boundary = completion.get("decision_boundary")
    expected_boundary = {
        "action_yes_min": JevResolver.ACTION_YES_MIN,
        "action_no_max": JevResolver.ACTION_NO_MAX,
        "choice_confidence_min": JevResolver.CHOICE_CONFIDENCE_MIN,
        "choice_margin_min": JevResolver.CHOICE_MARGIN_MIN,
        "calibrated": False,
        "use": "semantic_triage_only_not_accuracy_or_write_authorization",
    }
    if boundary != expected_boundary:
        raise ValueError("jev_decision_boundary_invalid")
    probability = float(probability)
    expected_signal = (
        "yes" if probability >= JevResolver.ACTION_YES_MIN
        else "no" if probability <= JevResolver.ACTION_NO_MAX
        else "uncertain"
    )
    if completion.get("action_signal") != expected_signal:
        raise ValueError("jev_action_signal_conflict")
    if completion.get("semantic_intent") != ("follow_up" if expected_signal == "yes" else "uncertain"):
        raise ValueError("jev_semantic_intent_conflict")
    statuses = {"new", "change", "complete", "notice", "uncertain"}
    if completion.get("item_status") not in statuses:
        raise ValueError("jev_item_status_invalid")
    numeric_fields = (
        "item_status_confidence", "item_status_margin", "due_date_confidence", "due_date_margin"
    )
    for field in numeric_fields:
        value = completion.get(field)
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or not 0 <= float(value) <= 1
        ):
            raise ValueError("jev_" + field + "_invalid")
    candidate_map = {str(item["key"]): item for item in date_candidates}
    due_key = completion.get("due_candidate_key")
    if due_key in {"none", "uncertain"}:
        if "due_date" in completion or "due_iso" in completion:
            raise ValueError("jev_date_not_in_candidate_set")
        selected = None
    else:
        selected = candidate_map.get(str(due_key))
        if selected is None:
            raise ValueError("jev_date_not_in_candidate_set")
        if (
            completion.get("due_date") != selected.get("due_date")
            or completion.get("due_iso") != selected.get("due_iso")
        ):
            raise ValueError("jev_date_candidate_conflict")
    expected_business = {str(item["key"]) for item in business_candidates}
    raw_business = completion.get("business_matches", [])
    if not isinstance(raw_business, list):
        raise ValueError("jev_business_matches_invalid")
    business_matches: list[dict[str, Any]] = []
    seen_business: set[str] = set()
    for item in raw_business:
        if not isinstance(item, dict) or set(item) != {"candidate_key", "score", "confidence"}:
            raise ValueError("jev_business_match_invalid")
        key = str(item["candidate_key"])
        score = item["score"]
        confidence = item["confidence"]
        if (
            key not in expected_business
            or key in seen_business
            or isinstance(score, bool)
            or not isinstance(score, (int, float))
            or not math.isfinite(float(score))
            or not 0 <= float(score) <= 2
            or isinstance(confidence, bool)
            or not isinstance(confidence, (int, float))
            or not math.isfinite(float(confidence))
            or not 0 <= float(confidence) <= 1
        ):
            raise ValueError("jev_business_match_invalid")
        seen_business.add(key)
        business_matches.append({
            "candidate_key": key,
            "score": float(score),
            "confidence": float(confidence),
        })
    if seen_business != expected_business:
        raise ValueError("jev_business_candidate_result_mismatch")
    if "usage" in completion and not isinstance(completion["usage"], dict):
        raise ValueError("jev_usage_invalid")
    return {
        "action_probability": probability,
        "action_signal": expected_signal,
        "semantic_intent": completion["semantic_intent"],
        "item_status": completion["item_status"],
        "item_status_confidence": float(completion["item_status_confidence"]),
        "item_status_margin": float(completion["item_status_margin"]),
        "due_candidate_key": str(due_key),
        "due_date_confidence": float(completion["due_date_confidence"]),
        "due_date_margin": float(completion["due_date_margin"]),
        "selected_due_candidate": dict(selected) if selected else None,
        "business_matches": business_matches,
        "decision_boundary": dict(boundary),
    }


class EventProcessor:
    """Apply the existing planner to event messages with explicit write gates."""

    def __init__(
        self,
        runtime_config: dict[str, Any],
        self_user: dict[str, str],
        *,
        mode: str = "shadow",
        write_ledger: bool = False,
        allow_external_writes: bool = False,
        dws: DwsClient | None = None,
        writer: WorkWriter | None = None,
        llm_resolver: CommandLlmResolver | DeepSeekLlmResolver | HermesCodexLlmResolver | JevResolver | None = None,
        existing_actions: Sequence[dict[str, Any]] | None = None,
        business_candidate_pool_issue: str | None = None,
        business_candidate_query_complete: bool | None = None,
        resource_dir: str = "/root/.hermes/cache/scratch/dingtalk_auto_todo_events",
        semantic_fallback: Any = None,
        date_role_fallback_resolver: Any = None,
        allow_simulated_candidates: bool = False,
        title_resolver: Any = None,
        source_order_provenance: str | None = None,
        deadline_clarifications_path: str | Path | None = None,
        supervised_rollout: bool = False,
    ):
        if mode not in {"shadow", "test", "production"}:
            raise ValueError("invalid event worker mode")
        if allow_external_writes and mode != "production":
            raise ValueError("external_writes_require_production_mode")
        if allow_external_writes and not write_ledger:
            raise ValueError("external_writes_require_ledger")
        if allow_simulated_candidates and (mode != "shadow" or allow_external_writes or write_ledger):
            raise ValueError("simulated_candidates_require_read_only_shadow")
        if deadline_clarifications_path is not None and (
            mode != "shadow" or write_ledger or allow_external_writes
        ):
            raise ValueError("deadline_clarifications_require_read_only_shadow")
        self.supervised_rollout = bool(supervised_rollout)
        has_v2_new_policy = (
            isinstance(llm_resolver, JevV2Resolver)
            and getattr(llm_resolver, "new_policy", False)
        )
        if self.supervised_rollout and mode != "production":
            raise ValueError("supervised_rollout_requires_production_jev_v2_new_policy")
        if self.supervised_rollout and not has_v2_new_policy:
            if semantic_fallback is not None or date_role_fallback_resolver is not None or title_resolver is not None:
                raise ValueError("supervised_native_fallbacks_require_jev_v2_new_policy")
            raise ValueError("supervised_rollout_requires_production_jev_v2_new_policy")
        if mode == "production" and semantic_fallback is not None:
            if not self.supervised_rollout:
                raise ValueError("semantic_fallback_requires_supervised_rollout")
            if not has_v2_new_policy:
                raise ValueError("supervised_native_fallbacks_require_jev_v2_new_policy")
        self.deadline_clarification: DeadlineClarification | None = load_deadline_clarification(
            deadline_clarifications_path
        )
        self.runtime_config = runtime_config
        self.config = build_planner_config(runtime_config)
        self.self_user = self_user
        self.live_ids: dict[str, str] = {}
        if self_user.get("name") and self_user.get("user_id"):
            self.live_ids[str(self_user["name"])] = str(self_user["user_id"])
        self.mode = mode
        self.write_ledger = bool(write_ledger)
        self.allow_external_writes = bool(allow_external_writes)
        self.dws = dws if dws is not None else DwsClient(
            confirmed_official_writes=(
                mode == "production" and self.allow_external_writes and self.write_ledger
            )
        )
        self.writer = writer
        self.writer_extension_supported = True
        self.writer_extension_error: str | None = None
        if mode == "production" and self.write_ledger:
            writer_type = type(writer)
            runtime_writer_class = WorkWriter
            durable_writer = bool(getattr(writer, "is_durable_work_writer", False)) or (
                writer_type.__module__ == "workflows.polardb_work_writer.writer"
                and writer_type.__name__ == "WorkWriter"
            ) or (
                isinstance(runtime_writer_class, type)
                and isinstance(writer, runtime_writer_class)
            )
            if writer is None:
                self.writer_extension_supported = False
                self.writer_extension_error = "durable_writer_unavailable"
            elif durable_writer:
                probe = getattr(writer, "contract_info", None)
                try:
                    info = probe() if callable(probe) else None
                except Exception:
                    info = None
                required_ops = {
                    "claim_child_action", "save_assignment_audit", "link_external_task", "save_action_result",
                }
                if (
                    not isinstance(info, dict)
                    or info.get("contract_version") != "polardb-work-writer/0.2.0"
                    or info.get("schema_supported") is not True
                    or not required_ops.issubset(set(info.get("operations") or []))
                ):
                    self.writer_extension_supported = False
                    self.writer_extension_error = "writer_extension_contract_unsupported"
        self.llm_resolver = llm_resolver
        self.semantic_fallback = semantic_fallback
        if date_role_fallback_resolver is not None and (
            mode != "shadow" or write_ledger or allow_external_writes
        ):
            if mode != "production" or not self.supervised_rollout:
                raise ValueError("date_role_fallback_requires_read_only_shadow")
            if not has_v2_new_policy:
                raise ValueError("supervised_native_fallbacks_require_jev_v2_new_policy")
        self.date_role_fallback_resolver = date_role_fallback_resolver
        if title_resolver is not None and (mode != "shadow" or write_ledger or allow_external_writes):
            if mode != "production" or not self.supervised_rollout:
                raise ValueError("title_fields_require_read_only_shadow")
            if not has_v2_new_policy:
                raise ValueError("supervised_native_fallbacks_require_jev_v2_new_policy")
        self.title_resolver = title_resolver
        self.allow_simulated_candidates = bool(allow_simulated_candidates)
        self.source_order_provenance = (
            COMPLETE_DWS_PAGE_ORDER
            if source_order_provenance == COMPLETE_DWS_PAGE_ORDER
            else "unverified_input_order"
        )
        self._arrived_context: list[dict[str, Any]] = []
        self._arrival_sequence = 0
        self._conversation_arrival_counts: dict[str, int] = {}
        self.business_candidate_pool_issue = business_candidate_pool_issue
        self.business_candidate_query_complete = business_candidate_query_complete
        self.resource_dir = Path(resource_dir)
        self.group_map = {
            str(row.get("conversation_id")): str(row.get("group_name"))
            for row in runtime_config.get("config_group", [])
            if (
                str(row.get("purpose") or "") == "todo_scan"
                and row.get("conversation_id")
            )
        }
        self.existing_actions: list[dict[str, Any]] = list(existing_actions or [])
        self._business_search_cache: dict[str, list[dict[str, Any]]] = {}
        self._date_role_fallback_sources: set[tuple[str, str]] = set()
        self.failed_conversations: set[str] = set()
        self.stats = {
            "received": 0,
            "target_messages": 0,
            "ignored": 0,
            "review": 0,
            "create": 0,
            "llm_calls": 0,
            "external_todos_created": 0,
            "attachments_uploaded": 0,
            "ledger_writes": 0,
            "errors": 0,
            "unmatched_events": 0,
            "unmatched_conversations": [],
            "semantic_fallback_calls": 0,
            "semantic_fallback_succeeded": 0,
            "semantic_fallback_failed": 0,
            "semantic_fallback_latency_ms_total": 0.0,
            "date_role_fallback_calls": 0,
            "date_role_fallback_succeeded": 0,
            "date_role_fallback_failed": 0,
            "date_role_fallback_latency_ms_total": 0.0,
            "title_field_calls": 0,
            "title_field_succeeded": 0,
            "title_field_failed": 0,
            "arrived_context_messages": 0,
            "self_authored_pure_mention_ignores": 0,
            "self_notice_artifact_ignores": 0,
            "human_clarified_sources": 0,
            "arrival_order_provenance": self.source_order_provenance,
        }

    def process_batch(self, payloads: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for payload in payloads:
            self.stats["received"] += 1
            try:
                event = normalize_event(payload)
            except EventFormatError:
                self.stats["errors"] += 1
                continue
            if not should_process_group(event["conversation_id"], self.group_map):
                self.stats["unmatched_events"] += 1
                unmatched = self.stats["unmatched_conversations"]
                if event["conversation_id"] not in unmatched and len(unmatched) < 20:
                    unmatched.append(event["conversation_id"])
                continue
            event["group"] = self.group_map[event["conversation_id"]]
            normalized.append(event)
        events = dedupe_events(normalized)
        self.stats["target_messages"] += len(events)
        if not events:
            return []

        plans: list[dict[str, Any]] = []
        for event in events:
            source_event = event
            attachment_titles = _source_file_titles(source_event)
            attachment_only = _is_attachment_only_source(source_event, attachment_titles)
            causal_notice, causal_notice_status = (
                self._causal_notice_for_attachment(source_event)
                if attachment_only
                else (None, "not_attachment_only")
            )
            human_confirmation_evidence: dict[str, str] | None = None
            plan_result = plan_batch(
                {
                    "config": self.config,
                    "self_user": self.self_user,
                    "existing_actions": self.existing_actions,
                    "messages": [event],
                },
                mode="test",
                run_date=self._run_date(event),
            )
            plan = plan_result["plans"][0]
            if plan.get("reason") == "source_already_processed":
                plan["_source_already_processed"] = True
            self._reconcile_source_link_readback(event, plan)
            composite_candidate = detect_composite_candidate(_as_text(event.get("text") or event.get("content")))
            composite_components: list[dict[str, Any]] | None = None
            composite_prepare_error: str | None = None
            if composite_candidate is not None:
                try:
                    composite_components = prepare_child_components(composite_candidate, self._run_date(event))
                except ValueError as exc:
                    composite_prepare_error = str(exc)
            verified_self_sender = _verified_message_sender(event, self.self_user)
            hard_ignore = False
            causal_context_eligible = False
            if plan.get("reason") == "source_already_processed":
                hard_ignore = True
            elif event.get("context_status") != "complete":
                plan["decision"] = "REVIEW"
                plan["reason"] = "source_context_incomplete"
                plan["warnings"] = list(dict.fromkeys([
                    *(plan.get("warnings") or []), "source_context_incomplete"
                ]))
                hard_ignore = True
            elif event.get("thread_context_status") == "partial":
                if plan.get("decision") == "CREATE":
                    plan["decision"] = "REVIEW"
                    plan["reason"] = "source_topic_context_incomplete"
                plan["warnings"] = list(dict.fromkeys([
                    *(plan.get("warnings") or []), "source_topic_context_incomplete"
                ]))
                hard_ignore = True
            elif _image_only_unreadable(event):
                plan["decision"] = "REVIEW"
                plan["reason"] = "image_content_not_recognized"
                plan["missing_fields"] = list(dict.fromkeys([
                    *(plan.get("missing_fields") or []), "image_content"
                ]))
                plan["warnings"] = list(dict.fromkeys([
                    *(plan.get("warnings") or []), "image_content_not_recognized"
                ]))
                hard_ignore = True
            elif verified_self_sender and plan.get("reason") == "self_authored_notice":
                plan["decision"] = "IGNORE"
                hard_ignore = True
            elif verified_self_sender and plan.get("reason") != "source_already_processed" and _is_pure_person_mention_only(event):
                plan["decision"] = "IGNORE"
                plan["reason"] = "self_authored_pure_person_mention"
                hard_ignore = True
                self.stats["self_authored_pure_mention_ignores"] += 1
            elif verified_self_sender and plan.get("reason") != "source_already_processed":
                supporting_notice = self._arrived_notice_supporting_artifact(event)
                if supporting_notice:
                    plan["decision"] = "IGNORE"
                    plan["reason"] = "self_notice_supporting_artifact"
                    hard_ignore = True
                    self.stats["self_notice_artifact_ignores"] += 1
            elif attachment_only:
                if causal_notice is None:
                    ambiguous_context = causal_notice_status in {
                        "attachment_filename_ambiguous", "attachment_order_unverified",
                        "attachment_conversation_unverified", "attachment_sender_unverified",
                        "attachment_time_unverified", "attachment_notice_ambiguous",
                    }
                    self._mark_attachment_context_review(
                        plan,
                        "attachment_context_ambiguous" if ambiguous_context else "attachment_context_unresolved",
                    )
                    hard_ignore = True
                else:
                    causal_scope = self._causal_notice_scope(causal_notice)
                    if causal_scope == "outside_scope":
                        plan["decision"] = "IGNORE"
                        plan["reason"] = "causal_notice_outside_self_scope"
                        self._clear_attachment_filename_inference(plan)
                        hard_ignore = True
                    elif causal_scope == "ambiguous":
                        self._mark_attachment_context_review(plan, "causal_notice_scope_ambiguous")
                        hard_ignore = True
                    elif causal_scope == "unresolved":
                        self._mark_attachment_context_review(plan, "causal_notice_scope_unresolved")
                        hard_ignore = True
                    else:
                        causal_context_eligible = True
                        event = self._event_with_causal_notice(source_event, causal_notice)
                        plan = plan_batch(
                            {
                                "config": self.config,
                                "self_user": self.self_user,
                                "existing_actions": self.existing_actions,
                                "messages": [event],
                            },
                            mode="test",
                            run_date=self._run_date(event),
                        )["plans"][0]
                        if plan.get("reason") == "source_already_processed":
                            hard_ignore = True
                        elif verified_self_sender and plan.get("reason") == "self_authored_notice":
                            plan["decision"] = "IGNORE"
                            hard_ignore = True
            elif plan.get("reason") == "department_scope_non_local":
                # This deterministic planner result already establishes that
                # the source is outside the current workgroup's scope.
                hard_ignore = True
            else:
                retrieval_instruction = bool(_RETRIEVAL_INSTRUCTION_RE.search(
                    _as_text(event.get("text") or event.get("content"))
                ))
                instruction_scope = _instruction_clause_scope(event, self.self_user)
                if instruction_scope == "outside_scope" and not retrieval_instruction:
                    plan["decision"] = "IGNORE"
                    plan["reason"] = "direct_other_assignment_outside_self_scope"
                    self._clear_assignment_scope_fields(plan)
                    hard_ignore = True
                elif instruction_scope == "ambiguous":
                    plan["decision"] = "REVIEW"
                    plan["reason"] = "assignment_scope_ambiguous"
                    plan["warnings"] = list(dict.fromkeys([
                        *(plan.get("warnings") or []), "assignment_scope_ambiguous"
                    ]))
                    self._clear_assignment_scope_fields(plan)
                    hard_ignore = True
            if composite_candidate is not None and not hard_ignore:
                if composite_prepare_error:
                    plan["decision"] = "REVIEW"
                    plan["reason"] = "composite_subtasks_incomplete"
                    plan["subtask_decomposition"] = {
                        "contract_version": CHILD_ACTION_CONTRACT_VERSION,
                        "status": "review",
                        "errors": [composite_prepare_error],
                    }
                    plan["warnings"] = list(dict.fromkeys([
                        *(plan.get("warnings") or []), "composite_subtasks_incomplete",
                    ]))
                    hard_ignore = True
                else:
                    plan["subtask_decomposition"] = {
                        "contract_version": CHILD_ACTION_CONTRACT_VERSION,
                        "status": "prepared",
                        "components": composite_components,
                    }
            contextual_followup = None
            if not hard_ignore:
                contextual_followup = self._contextual_retrieval_followup(event, plan)
                if contextual_followup:
                    plan = contextual_followup["plan"]
                    hard_ignore = True
                elif (
                    plan.get("reason") not in {"source_already_processed", "business_already_processed"}
                    and len(list(_RETRIEVAL_INSTRUCTION_RE.finditer(
                        _as_text(event.get("text") or event.get("content"))
                    ))) == 1
                ):
                    plan["decision"] = "REVIEW"
                    plan["reason"] = "contextual_retrieval_context_unresolved"
                    plan["warnings"] = list(dict.fromkeys([
                        *(plan.get("warnings") or []), "contextual_retrieval_context_unresolved"
                    ]))
                    hard_ignore = True
            jev_semantic_only = isinstance(self.llm_resolver, JevResolver)
            jev_v2 = isinstance(self.llm_resolver, JevV2Resolver)
            candidate_plan = (
                contextual_followup["candidate_plan"] if contextual_followup else plan
            )
            planned_event = (
                contextual_followup["planned_event"] if contextual_followup else event
            )
            if jev_v2 and not hard_ignore and getattr(self.llm_resolver, "new_policy", False):
                # Candidate recall must use the same program-owned route as the
                # eventual applicable plan. The initial legacy plan may assign
                # all-workgroup messages to self before policy routing.
                candidate_probe = dict(event, _jev_v2_applicable=True)
                candidate_plan = plan_batch(
                    {
                        "config": self.config,
                        "self_user": self.self_user,
                        "existing_actions": self.existing_actions,
                        "messages": [candidate_probe],
                    },
                    mode="test",
                    run_date=self._run_date(event),
                )["plans"][0]
            if not hard_ignore and jev_semantic_only and "semantic_intent_needs_llm" in (plan.get("warnings") or []):
                route_probe = dict(event)
                route_probe["_llm_semantic_intent"] = "follow_up"
                candidate_plan = plan_batch(
                    {
                        "config": self.config,
                        "self_user": self.self_user,
                        "existing_actions": self.existing_actions,
                        "messages": [route_probe],
                    },
                    mode="test",
                    run_date=self._run_date(event),
                )["plans"][0]
                if candidate_plan.get("decision") == "IGNORE" and candidate_plan.get("reason") == "business_already_processed":
                    plan = candidate_plan
            llm_completion: dict[str, Any] | None = None
            semantic_evidence: dict[str, Any] | None = None
            semantic_fallback_authorized = False
            semantic_fallback_title: str | None = None
            date_candidates: list[dict[str, str]] = []
            date_candidate_error: str | None = None
            jev_redaction_counts: dict[str, dict[str, int]] | None = None
            if not hard_ignore:
                try:
                    date_candidates = extract_due_candidates(
                        str(
                            (contextual_followup["planned_event"] if contextual_followup else event).get("text")
                            or (contextual_followup["planned_event"] if contextual_followup else event).get("content")
                            or ""
                        ),
                        self._run_date(event),
                        include_context=bool(getattr(self.llm_resolver, "new_policy", False)),
                    )
                except ValueError as exc:
                    date_candidate_error = str(exc)
                    plan["decision"] = "REVIEW"
                    plan["reason"] = "invalid_source_date_candidate"
                    plan["warnings"] = list(plan.get("warnings") or []) + ["invalid_source_date_candidate"]

            semantic_context = plan.get("semantic_context") or {}
            llm_scope_is_eligible = semantic_context.get("target_scope") in {
                "managed_workgroup",
                "all_workgroups",
                "explicit_self_assignment",
            }
            warnings = plan.get("warnings") or []
            business_candidates: list[dict[str, str]] = []
            business_candidate_overflow = False
            if jev_semantic_only and not hard_ignore and plan.get("decision") != "IGNORE":
                if self.business_candidate_pool_issue:
                    plan["decision"] = "REVIEW"
                    plan["reason"] = self.business_candidate_pool_issue
                    plan["warnings"] = list(plan.get("warnings") or []) + [self.business_candidate_pool_issue]
                else:
                    business_candidates, business_candidate_overflow = recall_business_candidates(
                        candidate_plan, self.existing_actions, allow_simulated=self.allow_simulated_candidates
                    )
            if jev_v2 and not hard_ignore and (plan.get("decision") != "IGNORE" or (getattr(self.llm_resolver, "new_policy", False) and plan.get("reason") not in {"source_already_processed", "business_already_processed", "test_message", "completion_report", "acknowledgement_only", "self_authored_notice", "self_authored_pure_person_mention", "self_notice_supporting_artifact"})):
                if self.business_candidate_query_complete is not True or self.business_candidate_pool_issue:
                    plan["decision"] = "REVIEW"
                    plan["reason"] = "business_candidate_query_incomplete"
                else:
                    candidates, overflow = recall_business_candidates(candidate_plan, self.existing_actions, allow_simulated=self.allow_simulated_candidates)
                    business_candidates = candidates
                    business_candidate_overflow = overflow
                    if overflow:
                        plan["decision"] = "REVIEW"
                        plan["reason"] = "business_candidate_pool_overflow"
                    elif (event.get("sender_id") and str(event.get("sender_id")) == str(self.self_user.get("user_id"))):
                        plan["decision"] = "IGNORE" if getattr(self.llm_resolver, "new_policy", False) else "REVIEW"
                        plan["reason"] = "jev_v2_self_authored_dispatch" if getattr(self.llm_resolver, "new_policy", False) else "jev_v2_scope_or_owner_unresolved"
                    elif not getattr(self.llm_resolver, "new_policy", False) and (not plan.get("route_module") or not plan.get("route_owner") or not llm_scope_is_eligible):
                        plan["decision"] = "REVIEW"
                        plan["reason"] = "jev_v2_scope_or_owner_unresolved"
                    elif (not getattr(self.llm_resolver, "new_policy", False) and ((set(plan.get("missing_fields") or []) - {"due_date", "route_owner", "title", "workgroup_scope"}) or any(w not in {"due_date_missing", "date_missing", "semantic_intent_needs_llm", "title_needs_llm", "action_signal_missing", "self_authored_requires_review", "workgroup_scope_unconfirmed", "responsibility_route_missing", "active_routing_not_loaded"} for w in (plan.get("warnings") or [])))) or date_candidate_error:
                        plan["decision"] = "REVIEW"
                        plan["reason"] = "jev_v2_plan_incomplete"
                    else:
                        try:
                            semantic_event = dict(event)
                            if getattr(self.llm_resolver, "new_policy", False):
                                source_text = str(event.get("text") or event.get("content") or "")
                                names = _at_names(event)
                                self_name = str(self.self_user.get("name") or self.self_user.get("display_name") or "").strip()
                                mentioned_self = bool(self_name and (self_name in names or ("@" + self_name) in source_text))
                                semantic_event["_jev_v2_policy_context"] = {"workgroup_role_mentioned": any(x in source_text for x in ("工班长", "班长")),"all_employee_mentioned":_has_all_scope_mention(event,source_text),"self_directly_mentioned":mentioned_self,"other_person_mentioned":bool(names and not mentioned_self),"broad_workgroup_mentioned":any(x in source_text for x in ("各工班","全体工班","本工班","各班组","工班人员","工班成员","铁炉西工班"))}
                            semantic_plan = dict(plan)
                            if semantic_plan.get("decision") == "IGNORE": semantic_plan["decision"] = "REVIEW"
                            new_policy = getattr(self.llm_resolver, "new_policy", False)
                            semantic_date_candidates = []
                            for candidate in date_candidates:
                                fields = ("key", "due_date", "due_iso")
                                if new_policy:
                                    fields += ("role", "source_span", "source_span_start", "source_span_end")
                                semantic_date_candidates.append({key: candidate[key] for key in fields})
                            self.stats["llm_calls"] += 1
                            judgments = self.llm_resolver.resolve(semantic_event, semantic_plan, date_candidates=semantic_date_candidates, business_candidates=candidates)
                            probability = judgments.get("prospective_action")
                            scope_probability = judgments.get("applies_to_workgroup")
                            followup_probability = judgments.get("needs_follow_up")
                            source_text = str(event.get("text") or event.get("content") or "")
                            referenced_context_missing = bool(re.search(r"(?:具体|详细)(?:要求|内容|事项).{0,20}(?:见|查看|详见|参见).{0,20}(?:[A-ZＡ-Ｚ]列|附件|文件|表格)", source_text))
                            policy_context = semantic_event.get("_jev_v2_policy_context") or {}
                            deterministic_scope = bool(policy_context.get("broad_workgroup_mentioned") or policy_context.get("self_directly_mentioned"))
                            is_new_policy = getattr(self.llm_resolver, "new_policy", False)
                            date_role_evidence = []
                            if is_new_policy:
                                model_role_by_key = {
                                    str(item.get("candidate_key")): item
                                    for item in judgments.get("date_role_judgments", [])
                                    if isinstance(item, dict)
                                }
                                allowed_roles = {"deadline", "effective", "event", "historical", "unknown"}
                                for candidate in date_candidates:
                                    parser_role = candidate.get("role", "unknown")
                                    key = str(candidate.get("key"))
                                    model_role = model_role_by_key.get(key)
                                    resolved_role = parser_role
                                    role_origin = "parser"
                                    role_confidence = None
                                    role_margin = None
                                    model_choice = None
                                    if isinstance(model_role, dict):
                                        model_choice = model_role.get("model_choice")
                                        role_confidence = model_role.get("confidence")
                                        role_margin = model_role.get("margin")
                                        confidence_ok = (
                                            not isinstance(role_confidence, bool)
                                            and isinstance(role_confidence, (int, float))
                                            and math.isfinite(float(role_confidence))
                                            and float(role_confidence) >= JevResolver.CHOICE_CONFIDENCE_MIN
                                        )
                                        margin_ok = (
                                            not isinstance(role_margin, bool)
                                            and isinstance(role_margin, (int, float))
                                            and math.isfinite(float(role_margin))
                                            and float(role_margin) >= JevResolver.CHOICE_MARGIN_MIN
                                        )
                                        model_resolved = model_role.get("resolved_role")
                                        if (
                                            parser_role == "unknown"
                                            and model_role.get("parser_role") == "unknown"
                                            and model_role.get("role_origin") == "jev_v2"
                                            and model_resolved in allowed_roles
                                            and confidence_ok
                                            and margin_ok
                                        ):
                                            resolved_role = model_resolved
                                            candidate["role"] = model_resolved
                                            role_origin = "jev_v2"
                                        elif parser_role == "unknown":
                                            role_origin = "parser_unresolved"
                                    date_role_evidence.append({
                                        "candidate_key": key,
                                        "due_date": candidate.get("due_date"),
                                        "source_span": candidate.get("source_span"),
                                        "source_span_start": candidate.get("source_span_start"),
                                        "source_span_end": candidate.get("source_span_end"),
                                        "parser_role": parser_role,
                                        "resolved_role": resolved_role,
                                        "role_origin": role_origin,
                                        "model_choice": model_choice,
                                        "confidence": role_confidence,
                                        "margin": role_margin,
                                    })
                            plan["semantic_evidence"] = {"policy":"jev_scope_followup_v2","applies_to_workgroup":scope_probability,"scope_basis":"explicit_workgroup_or_self_target" if deterministic_scope else "model_semantics","needs_follow_up":followup_probability,"prospective_action":probability,"source_clause_choice":judgments.get("source_clause_choice"),"source_clause_confidence":judgments.get("source_clause_confidence"),"due_date_choice":judgments.get("due_date_choice"),"due_date_confidence":judgments.get("due_date_confidence"),"date_role_judgments":date_role_evidence}
                            same = [judgments.get("same_work_" + str(candidate.get("key"))) for candidate in candidates]
                            has_legacy_compound = (
                                not is_new_policy
                                and not composite_components
                                and judgments.get("source_clause_choice") not in {"none", None}
                                and bool(re.search(r"[；;\\n]", str(event.get("text") or "")))
                            )
                            if ((not is_new_policy and judgments.get("source_clause_choice") == "uncertain") or has_legacy_compound or judgments.get("source_clause_overflow")):
                                plan["decision"], plan["reason"] = "REVIEW", "compound_requires_split"
                            elif is_new_policy and referenced_context_missing:
                                plan["decision"], plan["reason"] = "REVIEW", "jev_v2_referenced_context_missing"
                                plan["missing_fields"] = list(dict.fromkeys([*(plan.get("missing_fields") or []), "referenced_content"]))
                            elif probability is None or isinstance(probability, bool) or not math.isfinite(float(probability)):
                                plan["decision"], plan["reason"] = "REVIEW", "jev_v2_action_uncertain"
                            elif getattr(self.llm_resolver, "new_policy", False) and (scope_probability is None or followup_probability is None or ((0.25 < float(scope_probability) < 0.75) and not deterministic_scope) or (0.25 < float(followup_probability) < 0.75)):
                                plan["decision"], plan["reason"] = "REVIEW", "jev_v2_scope_or_followup_uncertain"
                                if is_new_policy and not deterministic_scope and float(scope_probability) <= 0.25 and 0.25 < float(followup_probability) < 0.75:
                                    plan["reason"] = "jev_v2_scope_context_unverified"
                                    plan["missing_fields"] = list(dict.fromkeys([*(plan.get("missing_fields") or []), "workgroup_scope", "follow_up_need"]))
                            elif getattr(self.llm_resolver, "new_policy", False) and float(scope_probability) <= 0.25 and not deterministic_scope:
                                plan["decision"], plan["reason"] = "IGNORE", "jev_v2_not_applicable"
                            elif ((getattr(self.llm_resolver, "new_policy", False) and float(followup_probability) <= 0.25) or (not getattr(self.llm_resolver, "new_policy", False) and probability <= 0.25)):
                                plan["decision"], plan["reason"] = "IGNORE", "jev_v2_no_prospective_action"
                            elif not getattr(self.llm_resolver, "new_policy", False) and probability < 0.75:
                                plan["decision"], plan["reason"] = "REVIEW", "jev_v2_action_uncertain"
                            elif candidates:
                                if any(value is None or 0.25 < value < 0.75 for value in same):
                                    plan["decision"], plan["reason"] = "REVIEW", "jev_v2_business_match_uncertain"
                                elif any(value >= 0.75 for value in same):
                                    plan["decision"], plan["reason"] = "IGNORE", "jev_v2_same_work_already_exists"
                                else:
                                    plan["decision"], plan["reason"] = "CREATE", "jev_v2_new_work"
                            else:
                                plan["decision"], plan["reason"] = "CREATE", "jev_v2_new_work"
                            if plan["decision"] == "CREATE":
                                replanned_event = dict(event)
                                replanned_event["_llm_semantic_intent"] = "follow_up"
                                if getattr(self.llm_resolver, "new_policy", False):
                                    replanned_event["_jev_v2_applicable"] = True
                                replanned_event["_jev_v2_authorized_shadow_create"] = True
                                replanned_event["_due_date_selection_resolved"] = True
                                date_choice = judgments.get("due_date_choice")
                                if composite_components:
                                    # Each child has its own source-proven date; the parent has no synthetic deadline.
                                    date_choice = "none"
                                    replanned_event.pop("due_date", None)
                                    replanned_event.pop("due_iso", None)
                                    plan["due_date"] = None
                                    plan.pop("due_iso", None)
                                    if plan.get("semantic_evidence") is not None:
                                        plan["semantic_evidence"]["due_date_basis"] = "explicit_component_deadlines"
                                elif getattr(self.llm_resolver, "new_policy", False):
                                    requested_date_choice = date_choice
                                    date_choice, date_role_error = _resolve_new_policy_due_key(
                                        date_candidates, source_text
                                    )
                                    clarification_id_matches = bool(
                                        self.deadline_clarification is not None
                                        and str(event.get("message_id") or "")
                                        == self.deadline_clarification.source_message_id
                                    )
                                    if (
                                        clarification_id_matches
                                        and self.deadline_clarification is not None
                                        and not self.deadline_clarification.matches(event)
                                    ):
                                        date_role_error = "deadline_clarification_source_mismatch"
                                    elif (
                                        date_role_error == "jev_v2_competing_deadline_context"
                                        and self.deadline_clarification is not None
                                        and self.deadline_clarification.matches(event)
                                        and plan.get("route_module") == "hazardous_waste"
                                        and sum(candidate.get("role") == "deadline" for candidate in date_candidates) == 1
                                        and all(
                                            candidate.get("role") in {"deadline", "effective", "event", "historical"}
                                            for candidate in date_candidates
                                        )
                                    ):
                                        human_confirmation_evidence = self.deadline_clarification.public_evidence()
                                        replanned_event["due_date"] = self.deadline_clarification.confirmed_due_date
                                        replanned_event["due_iso"] = self.deadline_clarification.confirmed_due_iso
                                        date_role_error = None
                                    if date_role_error:
                                        plan["decision"] = "REVIEW"
                                        plan["reason"] = date_role_error
                                        plan["missing_fields"] = list(dict.fromkeys([*(plan.get("missing_fields") or []), "due_date"]))
                                        plan["due_date"] = None
                                        plan["due_iso"] = None
                                        _apply_configured_hazard_owner(plan, self.config)
                                        replanned_event.pop("due_date", None)
                                        replanned_event.pop("due_iso", None)
                                    elif human_confirmation_evidence is not None:
                                        # Human evidence selects only the date. The route and all
                                        # action/scope/same-work gates remain program/model-owned.
                                        pass
                                    elif date_choice is None:
                                        replanned_event.pop("due_date", None)
                                        replanned_event.pop("due_iso", None)
                                        if plan.get("semantic_evidence") is not None:
                                            roles = {candidate.get("role") for candidate in date_candidates}
                                            if not roles:
                                                basis = "no_source_deadline"
                                            elif roles == {"effective"}:
                                                basis = "effective_time_not_deadline"
                                            elif roles == {"event"}:
                                                basis = "scheduled_event_not_deadline"
                                            elif roles == {"historical"}:
                                                basis = "historical_dates_not_deadlines"
                                            else:
                                                basis = "source_dates_not_deadlines"
                                            plan["semantic_evidence"]["due_date_basis"] = basis
                                    else:
                                        selected_date = next(
                                            (item for item in date_candidates if item["key"] == date_choice), None
                                        )
                                        if selected_date is None:
                                            raise LlmError("jev_v2_date_choice_invalid")
                                        replanned_event["due_date"] = selected_date["due_date"]
                                        replanned_event["due_iso"] = selected_date["due_iso"]
                                        if plan.get("semantic_evidence") is not None:
                                            plan["semantic_evidence"]["selected_due_key"] = date_choice
                                            plan["semantic_evidence"]["due_date_basis"] = (
                                                "single_source_deadline_context"
                                                if requested_date_choice != date_choice
                                                else "selected_source_deadline"
                                            )
                                elif date_choice == "uncertain":
                                    plan["decision"], plan["reason"] = "REVIEW", "jev_v2_date_choice_uncertain"
                                elif date_candidates and date_choice is None:
                                    plan["decision"], plan["reason"] = "REVIEW", "jev_v2_date_choice_missing"
                                elif date_choice == "none" or date_choice is None:
                                    replanned_event.pop("due_date", None)
                                    replanned_event.pop("due_iso", None)
                                else:
                                    selected_date = next((item for item in date_candidates if item["key"] == date_choice), None)
                                    if selected_date is None:
                                        raise LlmError("jev_v2_date_choice_invalid")
                                    replanned_event["due_date"] = selected_date["due_date"]
                                    replanned_event["due_iso"] = selected_date["due_iso"]
                                if (
                                    plan.get("decision") == "CREATE"
                                    and plan.get("route_module") == "hazardous_waste"
                                    and not self.config.get("assign_to_self_only")
                                    and not _as_text((self.config.get("responsibility_rules") or {}).get("hazardous_waste"))
                                    and not _as_text(plan.get("route_owner"))
                                ):
                                    plan["decision"] = "REVIEW"
                                    if (plan.get("assignment_audit") or {}).get("rule_id") == "F":
                                        # JEV may classify the action, but it cannot replace the
                                        # current-period handover gate or its specific review reason.
                                        plan["reason"] = "confirmed_area_handover_unverified"
                                        plan["missing_fields"] = list(dict.fromkeys([*(plan.get("missing_fields") or []), "current_area_owner"]))
                                    else:
                                        plan["reason"] = "authoritative_hazardous_waste_owner_unavailable"
                                        plan["missing_fields"] = list(dict.fromkeys([*(plan.get("missing_fields") or []), "route_owner"]))
                                    _apply_configured_hazard_owner(plan, self.config)
                                elif plan.get("decision") == "CREATE":
                                    semantic_evidence = plan.get("semantic_evidence")
                                    planned_event = dict(replanned_event)
                                    plan = plan_batch({"config": self.config, "self_user": self.self_user,
                                    "existing_actions": self.existing_actions, "messages": [replanned_event]},
                                    mode="test", run_date=self._run_date(event))["plans"][0]
                                    title_compiler_failed = False
                                    if plan.get("decision") == "REVIEW" and self.title_resolver and getattr(self.llm_resolver, "new_policy", False):
                                        missing = set(plan.get("missing_fields") or [])
                                        warnings = set(plan.get("warnings") or [])
                                        if missing == {"title"} and warnings <= {"title_requires_context"}:
                                            try:
                                                self.stats["title_field_calls"] += 1
                                                compiled = self.title_resolver.resolve(replanned_event, plan)
                                                redacted_text, _ = build_title_field_context(
                                                    replanned_event,
                                                    max_source_chars=int(getattr(self.title_resolver, "max_source_chars", 4000)),
                                                )
                                                compiled = validate_title_field_result(compiled, redacted_text)
                                                title, spans = compiled["title"], compiled["evidence_spans"]
                                                if not isinstance(title, str) or not 0 < len(title.strip()) <= 80 or "请知悉" in title:
                                                    raise ValueError("title_compiler_invalid_title")
                                                titled = dict(replanned_event, title=title.strip(), _jev_v2_title_verified=True)
                                                titled_plan = plan_batch({"config": self.config, "self_user": self.self_user, "existing_actions": self.existing_actions, "messages": [titled]}, mode="test", run_date=self._run_date(event))["plans"][0]
                                                if titled_plan.get("decision") == "CREATE":
                                                    plan = titled_plan
                                                    planned_event = dict(titled)
                                                    plan["title_field_evidence"] = {
                                                        "source": "title_field_resolver",
                                                        "title": title.strip(),
                                                        "evidence_spans": list(compiled["evidence_spans"]),
                                                    }
                                                    self.stats["title_field_succeeded"] += 1
                                                else:
                                                    self.stats["title_field_failed"] += 1
                                                    title_compiler_failed = True
                                                    plan["decision"] = "REVIEW"
                                                    plan["reason"] = "title_compiler_replan_incomplete"
                                            except Exception as exc:
                                                self.stats["title_field_failed"] += 1
                                                title_compiler_failed = True
                                                plan["decision"] = "REVIEW"
                                                plan["reason"] = "title_compiler_invalid"
                                                title_error = _safe_resolver_failure(exc)
                                                plan["title_compiler_error"] = title_error["code"]
                                                plan["title_field_error"] = title_error
                                    if semantic_evidence is not None: plan["semantic_evidence"] = semantic_evidence

                                    if plan.get("decision") != "CREATE":
                                        plan["decision"] = "REVIEW"
                                        if not title_compiler_failed:
                                            plan["reason"] = "jev_v2_replan_incomplete"
                                    else:
                                        plan["warnings"] = list(plan.get("warnings") or []) + ["jev_v2_uncalibrated_shadow_only"]
                                        plan["decision"] = "CREATE"
                                        plan["reason"] = "jev_v2_new_work_shadow_only"
                        except Exception as exc:
                            plan["decision"] = "REVIEW"
                            jev_error = _safe_resolver_failure(exc)
                            plan["reason"] = (
                                jev_error["code"]
                                if jev_error["code"] in {"jev_v2_source_incomplete", "jev_v2_source_invalid"}
                                else "jev_v2_resolution_failed"
                            )
                            plan["jev_error_code"] = jev_error["code"]
                            plan["jev_error_stage"] = jev_error["stage"]
                            if getattr(self.llm_resolver, "new_policy", False) and any(
                                candidate.get("role") == "unknown" for candidate in date_candidates
                            ):
                                plan["due_date"] = None
                                plan["due_iso"] = None
                                plan["missing_fields"] = list(dict.fromkeys([*(plan.get("missing_fields") or []), "due_date"]))
                                _apply_configured_hazard_owner(plan, self.config)
                needs_resolution = False
            if jev_v2 and not hard_ignore and not getattr(self.llm_resolver, "new_policy", False) and (not plan.get("route_module") or not plan.get("route_owner")):
                plan["decision"] = "REVIEW"
                plan["reason"] = "jev_v2_scope_or_owner_unresolved"
            if jev_v2:
                warnings = []
                jev_semantic_only = False
            needs_resolution = (not jev_v2) and ("semantic_intent_needs_llm" in warnings or (
                not jev_semantic_only and "title_needs_llm" in warnings
            ) or (
                jev_semantic_only
                and plan.get("decision") in {"CREATE", "REVIEW"}
                and llm_scope_is_eligible
            ))
            if business_candidate_overflow:
                plan["decision"] = "REVIEW"
                plan["reason"] = "business_candidate_pool_overflow"
                plan["warnings"] = list(plan.get("warnings") or []) + ["business_candidate_pool_overflow"]
            if (
                not hard_ignore
                and
                self.llm_resolver
                and not isinstance(self.llm_resolver, JevV2Resolver)
                and not self.business_candidate_pool_issue
                and not date_candidate_error
                and not business_candidate_overflow
                and plan.get("decision") != "IGNORE"
                and llm_scope_is_eligible
                and needs_resolution
            ):
                self.stats["llm_calls"] += 1
                try:
                    if jev_semantic_only:
                        completion = self.llm_resolver.resolve(
                            event,
                            plan,
                            date_candidates=date_candidates,
                            business_candidates=business_candidates,
                        )
                    else:
                        completion = self.llm_resolver.resolve(event, plan)
                    accepted_completion: dict[str, Any] = {}
                    llm_warnings: list[str] = []
                    if jev_semantic_only:
                        jev = validate_jev_evaluation(
                            completion, date_candidates, business_candidates
                        )
                        semantic_evidence = {
                            key: jev[key]
                            for key in (
                                "action_probability", "action_signal", "item_status",
                                "item_status_confidence", "item_status_margin",
                                "due_candidate_key", "due_date_confidence", "due_date_margin",
                                "business_matches", "decision_boundary",
                            )
                        }
                        semantic_evidence["business_scores_use"] = "triage_only_never_merge_or_authorize_create"
                        if jev["action_signal"] != "yes":
                            plan["decision"] = "REVIEW"
                            plan["reason"] = "jev_follow_up_signal_unresolved"
                        elif jev["item_status"] == "uncertain":
                            plan["decision"] = "REVIEW"
                            plan["reason"] = "jev_item_status_uncertain"
                        elif jev["item_status"] in {"change", "complete"}:
                            plan["decision"] = "REVIEW"
                            plan["reason"] = "jev_existing_item_state_requires_review"
                        elif jev["due_candidate_key"] == "uncertain":
                            plan["decision"] = "REVIEW"
                            plan["reason"] = "jev_due_date_choice_uncertain"
                        else:
                            enriched = dict(event)
                            planned_event = enriched
                            enriched["_llm_semantic_intent"] = "follow_up"
                            enriched["_due_date_selection_resolved"] = True
                            if jev["selected_due_candidate"]:
                                enriched["due_date"] = jev["selected_due_candidate"]["due_date"]
                                enriched["due_iso"] = jev["selected_due_candidate"]["due_iso"]
                            else:
                                enriched.pop("due_date", None)
                                enriched.pop("due_iso", None)
                            plan = plan_batch(
                                {
                                    "config": self.config,
                                    "self_user": self.self_user,
                                    "existing_actions": self.existing_actions,
                                    "messages": [enriched],
                                },
                                mode="test",
                                run_date=self._run_date(event),
                            )["plans"][0]
                            if jev["item_status"] == "notice" and plan.get("decision") == "CREATE":
                                plan["decision"] = "REVIEW"
                                plan["reason"] = "jev_notice_conflicts_with_task_plan"
                            match_reason = None
                            if not business_candidates:
                                match_reason = "business_candidate_pool_missing"
                            else:
                                business_scores = sorted(
                                    jev["business_matches"], key=lambda item: item["score"], reverse=True
                                )
                                if any(item["confidence"] < JevResolver.CHOICE_CONFIDENCE_MIN for item in business_scores):
                                    match_reason = "business_match_low_confidence"
                                elif business_scores and business_scores[0]["score"] > 0.25:
                                    if (
                                        len(business_scores) > 1
                                        and business_scores[0]["score"] - business_scores[1]["score"] < 0.25
                                    ):
                                        match_reason = "business_candidate_scores_close"
                                    elif business_scores[0]["score"] >= 1.75:
                                        match_reason = "business_duplicate_requires_review"
                                    else:
                                        match_reason = "business_similarity_ambiguous"
                                elif business_scores:
                                    match_reason = "business_candidate_no_match_requires_review"
                            if match_reason:
                                plan["decision"] = "REVIEW"
                                plan["reason"] = match_reason
                        accepted_completion["semantic_intent"] = jev["semantic_intent"]
                    else:
                        for key in ("title", "semantic_intent"):
                            if completion.get(key):
                                accepted_completion[key] = completion[key]
                        if completion.get("due_date"):
                            expected_due_date = parse_due_date(
                                str(event.get("text") or event.get("content") or ""),
                                self._run_date(event),
                            )
                            if completion["due_date"] == expected_due_date:
                                accepted_completion["due_date"] = completion["due_date"]
                            else:
                                llm_warnings.append("llm_due_date_not_explicit_in_source")
                        if accepted_completion:
                            enriched = dict(event)
                            planned_event = enriched
                            for key in ("title", "due_date"):
                                if accepted_completion.get(key):
                                    enriched[key] = accepted_completion[key]
                            if accepted_completion.get("semantic_intent"):
                                enriched["_llm_semantic_intent"] = accepted_completion["semantic_intent"]
                            plan = plan_batch(
                                {
                                    "config": self.config,
                                    "self_user": self.self_user,
                                    "existing_actions": self.existing_actions,
                                    "messages": [enriched],
                                },
                                mode="test",
                                run_date=self._run_date(event),
                            )["plans"][0]
                    llm_completion = accepted_completion
                    if llm_warnings:
                        plan["warnings"] = list(plan.get("warnings") or []) + llm_warnings
                except Exception as exc:
                    plan["decision"] = "REVIEW"
                    resolver_failure = _safe_resolver_failure(exc)
                    plan["llm_resolver_error"] = resolver_failure
                    plan["reason"] = (
                        resolver_failure["code"]
                        if resolver_failure["code"] in {"jev_v2_source_incomplete", "jev_v2_source_invalid"}
                        else "llm_resolution_failed"
                    )
                    plan["warnings"] = list(plan.get("warnings") or []) + ["llm_resolution_failed"]
                finally:
                    if jev_semantic_only:
                        jev_redaction_counts = self._safe_jev_redaction_counts(self.llm_resolver)
            if (
                plan.get("decision") in {"IGNORE", "REVIEW"}
                and llm_scope_is_eligible
                and "semantic_intent_needs_llm" in (plan.get("warnings") or [])
                and not getattr(self.llm_resolver, "new_policy", False)
                and not (jev_semantic_only and self.business_candidate_pool_issue)
            ):
                plan["decision"] = "REVIEW"
                if not self.llm_resolver:
                    plan["reason"] = "llm_resolution_required"
                    plan["warnings"] = list(plan.get("warnings") or []) + ["llm_resolver_unavailable"]
                elif plan.get("llm_resolver_error") or any(
                    str(warning).startswith("llm_failed:") for warning in (plan.get("warnings") or [])
                ):
                    plan["reason"] = "llm_resolution_failed"
                else:
                    plan["reason"] = "llm_resolution_incomplete"
            if (not hard_ignore and self.semantic_fallback and isinstance(self.llm_resolver, JevV2Resolver)
                    and getattr(self.llm_resolver, "new_policy", False)
                    and _has_substantive_jev_text(event.get("text") or event.get("content"))
                    and not re.fullmatch(r"\s*(?:\[附件\]|附件|图片|表情|<[^>]+>)\s*", str(event.get("text") or ""), re.I)
                    and plan.get("decision") == "REVIEW"
                    and plan.get("reason") in {"jev_v2_scope_or_followup_uncertain", "jev_v2_scope_context_unverified", "jev_v2_action_uncertain", "jev_v2_date_choice_uncertain", "jev_v2_business_match_uncertain"}
                    and _scope_fallback_context_is_complete(
                        event, plan, candidate_plan, self.semantic_fallback, self.self_user
                    )):
                self.stats["semantic_fallback_calls"] += 1
                fallback_valid = False
                fallback = None
                try:
                    import time
                    fallback_started = time.perf_counter()
                    fallback = self.semantic_fallback.resolve_context(event, plan, date_candidates=date_candidates, business_candidates=business_candidates)
                    self.stats["semantic_fallback_latency_ms_total"] += (time.perf_counter() - fallback_started) * 1000
                    fallback, semantic_context = _validate_semantic_fallback_result(
                        fallback,
                        event,
                        plan,
                        date_candidates,
                        business_candidates,
                        self.semantic_fallback,
                    )
                    fallback_valid = True
                    self.stats["semantic_fallback_succeeded"] += 1
                    original_reason = plan.get("reason")
                    plan["fallback_semantic_evidence"] = {"source":"luna_reasoning", "trigger_reason":original_reason, "original_jev_signal":(plan.get("semantic_evidence") or {}).get("prospective_action"), **fallback}
                    if fallback["applicability"] == "not_applicable" or fallback["followup"] == "not_required":
                        plan["decision"], plan["reason"] = "IGNORE", "jev_v2_semantic_fallback_not_applicable"
                    elif (fallback["applicability"] == "applicable" and fallback["followup"] == "required"
                          and self.business_candidate_query_complete is True
                          and not self.business_candidate_pool_issue
                          and set(fallback["same_work"]) == {str(c.get("key")) for c in business_candidates}
                          and all(v == "different" for v in fallback["same_work"].values())):
                        title = next((c for c in semantic_context.local_title_candidates if str(c.get("key")) == str(fallback["title_span"])), None)
                        source_text = str(event.get("text") or event.get("content") or "")
                        semantic_fallback_authorized = True
                        if len(source_text) <= 80 and fallback["title_span"] is not None and title is not None:
                            semantic_fallback_title = str(title.get("text") or "").strip() or None
                        requested_key = fallback["due_key"]
                        candidate_keys = {str(candidate.get("key")) for candidate in date_candidates}
                        if requested_key not in {"none", "unknown"} and str(requested_key) not in candidate_keys:
                            date_choice, date_role_error = None, "deadline_selection_unresolved"
                        else:
                            date_choice, date_role_error = _resolve_new_policy_due_key(
                                date_candidates, source_text
                            )
                            if requested_key == "unknown" and date_choice is not None:
                                date_choice = None
                                date_role_error = "deadline_selection_unresolved"
                            if date_role_error:
                                # Keep the accepted business judgment local and let the
                                # bounded date-role stage resolve its remaining gate.
                                plan["decision"] = "REVIEW"
                                plan["reason"] = date_role_error
                                plan["due_selection_error"] = date_role_error
                                plan["due_date"] = None
                                plan["due_iso"] = None
                                plan["missing_fields"] = list(dict.fromkeys([
                                    *(plan.get("missing_fields") or []), "due_date"
                                ]))
                                _apply_configured_hazard_owner(plan, self.config)
                            else:
                                replanned_event = dict(event)
                                replanned_event["_llm_semantic_intent"] = "follow_up"
                                replanned_event["_jev_v2_applicable"] = True
                                replanned_event["_jev_v2_authorized_shadow_create"] = True
                                replanned_event["_due_date_selection_resolved"] = True
                                if semantic_fallback_title:
                                    replanned_event["title"] = semantic_fallback_title
                                    replanned_event["_jev_v2_title_verified"] = True
                                if date_choice is None:
                                    replanned_event.pop("due_date", None)
                                    replanned_event.pop("due_iso", None)
                                else:
                                    selected = next((
                                        candidate for candidate in date_candidates
                                        if str(candidate.get("key")) == date_choice
                                    ), None)
                                    if selected is None:
                                        raise ValueError("semantic_fallback_due_candidate_missing")
                                    replanned_event["due_date"] = selected.get("due_date")
                                    replanned_event["due_iso"] = selected.get("due_iso")
                                planned_event = dict(replanned_event)
                                original_evidence = plan.get("semantic_evidence")
                                fallback_evidence = plan.get("fallback_semantic_evidence")
                                replanned = plan_batch({
                                    "config": self.config,
                                    "self_user": self.self_user,
                                    "existing_actions": self.existing_actions,
                                    "messages": [replanned_event],
                                }, mode="test", run_date=self._run_date(event))["plans"][0]
                                if original_evidence is not None:
                                    replanned["semantic_evidence"] = original_evidence
                                if fallback_evidence is not None:
                                    replanned["fallback_semantic_evidence"] = fallback_evidence
                                replanned["warnings"] = list(dict.fromkeys([
                                    *(replanned.get("warnings") or []), "semantic_fallback_shadow_only"
                                ]))
    
                                title_compile_failed = False
                                if (
                                    replanned.get("decision") == "REVIEW"
                                    and len(source_text) > 80
                                    and self.title_resolver
                                    and set(replanned.get("missing_fields") or []) == {"title"}
                                    and set(replanned.get("warnings") or []) <= {
                                        "title_requires_context", "semantic_fallback_shadow_only"
                                    }
                                    and _as_text(replanned.get("route_module"))
                                    and _as_text(replanned.get("route_owner"))
                                ):
                                    try:
                                        self.stats["title_field_calls"] += 1
                                        compiled = self.title_resolver.resolve(replanned_event, replanned)
                                        redacted_text, _ = build_title_field_context(
                                            replanned_event,
                                            max_source_chars=int(getattr(
                                                self.title_resolver, "max_source_chars", 4000
                                            )),
                                        )
                                        compiled = validate_title_field_result(compiled, redacted_text)
                                        compiled_title = compiled["title"]
                                        if (
                                            not isinstance(compiled_title, str)
                                            or not 0 < len(compiled_title.strip()) <= 80
                                            or "请知悉" in compiled_title
                                        ):
                                            raise ValueError("title_compiler_invalid_title")
                                        titled_event = dict(
                                            replanned_event,
                                            title=compiled_title.strip(),
                                            _jev_v2_title_verified=True,
                                        )
                                        titled = plan_batch({
                                            "config": self.config,
                                            "self_user": self.self_user,
                                            "existing_actions": self.existing_actions,
                                            "messages": [titled_event],
                                        }, mode="test", run_date=self._run_date(event))["plans"][0]
                                        if original_evidence is not None:
                                            titled["semantic_evidence"] = original_evidence
                                        if fallback_evidence is not None:
                                            titled["fallback_semantic_evidence"] = fallback_evidence
                                        titled["warnings"] = list(dict.fromkeys([
                                            *(titled.get("warnings") or []),
                                            "semantic_fallback_shadow_only",
                                        ]))
                                        if titled.get("decision") == "CREATE":
                                            replanned = titled
                                            planned_event = dict(titled_event)
                                            replanned["title_field_evidence"] = {
                                                "source": "title_field_resolver",
                                                "title": compiled_title.strip(),
                                                "evidence_spans": list(compiled["evidence_spans"]),
                                            }
                                            self.stats["title_field_succeeded"] += 1
                                        else:
                                            self.stats["title_field_failed"] += 1
                                            title_compile_failed = True
                                            titled["decision"] = "REVIEW"
                                            titled["reason"] = "title_compiler_replan_incomplete"
                                            replanned = titled
                                    except Exception as exc:
                                        self.stats["title_field_failed"] += 1
                                        title_compile_failed = True
                                        replanned["decision"] = "REVIEW"
                                        replanned["reason"] = "title_compiler_invalid"
                                        title_error = _safe_resolver_failure(exc)
                                        replanned["title_compiler_error"] = title_error["code"]
                                        replanned["title_field_error"] = title_error
    
                                if replanned.get("decision") == "CREATE":
                                    replanned["warnings"] = list(dict.fromkeys([
                                        *(replanned.get("warnings") or []),
                                        "jev_v2_uncalibrated_shadow_only",
                                    ]))
                                    replanned["reason"] = "jev_v2_new_work_shadow_only"
                                elif replanned.get("decision") == "REVIEW":
                                    if not title_compile_failed and not replanned.get("reason"):
                                        if set(replanned.get("missing_fields") or []) == {"title"}:
                                            replanned["reason"] = "title_requires_context"
                                        else:
                                            replanned["reason"] = "semantic_fallback_replan_incomplete"
                                plan = replanned
                except Exception as exc:
                    if not fallback_valid:
                        self.stats["semantic_fallback_failed"] += 1
                    _record_resolver_failure(plan, "semantic_fallback_error", exc)
            if (
                not hard_ignore
                and self.date_role_fallback_resolver is not None
                and isinstance(self.llm_resolver, JevV2Resolver)
                and getattr(self.llm_resolver, "new_policy", False)
            ):
                unknown_role_candidates = _date_role_fallback_candidates_if_eligible(
                    event,
                    plan,
                    candidate_plan,
                    date_candidates,
                    self.date_role_fallback_resolver,
                    self.self_user,
                    business_candidate_query_complete=self.business_candidate_query_complete,
                    business_candidate_pool_issue=self.business_candidate_pool_issue,
                    business_candidate_overflow=business_candidate_overflow,
                    date_candidate_error=date_candidate_error,
                )
                source_identity = (
                    _as_text(event.get("conversation_id")),
                    _as_text(event.get("message_id")),
                )
                if unknown_role_candidates and source_identity not in self._date_role_fallback_sources:
                    # Count the source before dispatch so repeated processor calls
                    # cannot retry this reasoning fallback for the same message.
                    self._date_role_fallback_sources.add(source_identity)
                    self.stats["date_role_fallback_calls"] += 1
                    import time
                    date_role_started = time.perf_counter()
                    date_role_adapter_valid = False
                    date_role_latency_recorded = False
                    try:
                        raw_roles = self.date_role_fallback_resolver.resolve_roles(
                            event,
                            date_candidates=unknown_role_candidates,
                        )
                        max_source_chars = int(getattr(
                            self.date_role_fallback_resolver, "max_source_chars", 4000
                        ))
                        role_result = validate_date_role_fallback_result(
                            raw_roles,
                            event,
                            unknown_role_candidates,
                            max_source_chars=max_source_chars,
                        )
                        date_role_adapter_valid = True
                        self.stats["date_role_fallback_succeeded"] += 1
                        self.stats["date_role_fallback_latency_ms_total"] += (
                            time.perf_counter() - date_role_started
                        ) * 1000
                        date_role_latency_recorded = True
                        plan["date_role_fallback_evidence"] = role_result
                        resolved_roles = {row["key"]: row["role"] for row in role_result["roles"]}
                        resolved_candidates = [dict(candidate) for candidate in date_candidates]
                        for candidate in resolved_candidates:
                            key = str(candidate.get("key"))
                            if candidate.get("role") == "unknown" and key in resolved_roles:
                                candidate["role"] = resolved_roles[key]
                        date_candidates = resolved_candidates
                        source_text = str(event.get("text") or event.get("content") or "")
                        date_choice, date_role_error = _resolve_new_policy_due_key(
                            resolved_candidates, source_text
                        )
                        if date_role_error:
                            plan["decision"] = "REVIEW"
                            plan["reason"] = date_role_error
                            plan["due_date"] = None
                            plan["due_iso"] = None
                            plan["missing_fields"] = list(dict.fromkeys([
                                *(plan.get("missing_fields") or []), "due_date"
                            ]))
                            _apply_configured_hazard_owner(plan, self.config)
                        else:
                            replanned_event = dict(event)
                            replanned_event["_llm_semantic_intent"] = "follow_up"
                            replanned_event["_jev_v2_applicable"] = True
                            replanned_event["_jev_v2_authorized_shadow_create"] = True
                            replanned_event["_due_date_selection_resolved"] = True
                            if semantic_fallback_authorized and semantic_fallback_title:
                                replanned_event["title"] = semantic_fallback_title
                                replanned_event["_jev_v2_title_verified"] = True
                            if date_choice is None:
                                replanned_event.pop("due_date", None)
                                replanned_event.pop("due_iso", None)
                            else:
                                selected = next((
                                    candidate for candidate in resolved_candidates
                                    if str(candidate.get("key")) == date_choice
                                ), None)
                                if selected is None:
                                    raise ValueError("date_role_fallback_candidate_missing")
                                replanned_event["due_date"] = selected.get("due_date")
                                replanned_event["due_iso"] = selected.get("due_iso")
                            planned_event = dict(replanned_event)
                            original_semantic_evidence = plan.get("semantic_evidence")
                            original_fallback_semantic_evidence = plan.get("fallback_semantic_evidence")
                            replanned = plan_batch({
                                "config": self.config,
                                "self_user": self.self_user,
                                "existing_actions": self.existing_actions,
                                "messages": [replanned_event],
                            }, mode="test", run_date=self._run_date(event))["plans"][0]
                            if original_semantic_evidence is not None:
                                replanned["semantic_evidence"] = original_semantic_evidence
                            if original_fallback_semantic_evidence is not None:
                                replanned["fallback_semantic_evidence"] = original_fallback_semantic_evidence
                            replanned["date_role_fallback_evidence"] = role_result
                            replanned["warnings"] = list(dict.fromkeys([
                                *(replanned.get("warnings") or []),
                                "date_role_fallback_shadow_only",
                            ]))
                            if semantic_fallback_authorized:
                                replanned["warnings"] = list(dict.fromkeys([
                                    *(replanned.get("warnings") or []),
                                    "semantic_fallback_shadow_only",
                                ]))

                            title_compile_failed = False
                            if (
                                replanned.get("decision") == "REVIEW"
                                and self.title_resolver
                                and set(replanned.get("missing_fields") or []) == {"title"}
                                and set(replanned.get("warnings") or []) <= {
                                    "title_requires_context",
                                    "date_role_fallback_shadow_only",
                                    "semantic_fallback_shadow_only",
                                }
                            ):
                                try:
                                    self.stats["title_field_calls"] += 1
                                    compiled = self.title_resolver.resolve(replanned_event, replanned)
                                    redacted_text, _ = build_title_field_context(
                                        replanned_event,
                                        max_source_chars=int(getattr(
                                            self.title_resolver, "max_source_chars", 4000
                                        )),
                                    )
                                    compiled = validate_title_field_result(compiled, redacted_text)
                                    title = compiled["title"]
                                    if (
                                        not isinstance(title, str)
                                        or not 0 < len(title.strip()) <= 80
                                        or "请知悉" in title
                                    ):
                                        raise ValueError("title_compiler_invalid_title")
                                    titled_event = dict(
                                        replanned_event,
                                        title=title.strip(),
                                        _jev_v2_title_verified=True,
                                    )
                                    titled = plan_batch({
                                        "config": self.config,
                                        "self_user": self.self_user,
                                        "existing_actions": self.existing_actions,
                                        "messages": [titled_event],
                                    }, mode="test", run_date=self._run_date(event))["plans"][0]
                                    if titled.get("decision") == "CREATE":
                                        replanned = titled
                                        planned_event = dict(titled_event)
                                        replanned["title_field_evidence"] = {
                                            "source": "title_field_resolver",
                                            "title": title.strip(),
                                            "evidence_spans": list(compiled["evidence_spans"]),
                                        }
                                        self.stats["title_field_succeeded"] += 1
                                    else:
                                        self.stats["title_field_failed"] += 1
                                        title_compile_failed = True
                                        titled["decision"] = "REVIEW"
                                        titled["reason"] = "title_compiler_replan_incomplete"
                                        replanned = titled
                                except Exception as exc:
                                    self.stats["title_field_failed"] += 1
                                    title_compile_failed = True
                                    replanned["decision"] = "REVIEW"
                                    replanned["reason"] = "title_compiler_invalid"
                                    title_error = _safe_resolver_failure(exc)
                                    replanned["title_compiler_error"] = title_error["code"]
                                    replanned["title_field_error"] = title_error
                            if original_semantic_evidence is not None:
                                replanned["semantic_evidence"] = original_semantic_evidence
                            if original_fallback_semantic_evidence is not None:
                                replanned["fallback_semantic_evidence"] = original_fallback_semantic_evidence
                            replanned["date_role_fallback_evidence"] = role_result
                            replanned["warnings"] = list(dict.fromkeys([
                                *(replanned.get("warnings") or []),
                                "date_role_fallback_shadow_only",
                            ]))
                            if semantic_fallback_authorized:
                                replanned["warnings"] = list(dict.fromkeys([
                                    *(replanned.get("warnings") or []),
                                    "semantic_fallback_shadow_only",
                                ]))
                            if replanned.get("decision") == "CREATE":
                                replanned["warnings"] = list(dict.fromkeys([
                                    *(replanned.get("warnings") or []),
                                    "jev_v2_uncalibrated_shadow_only",
                                ]))
                                replanned["reason"] = "jev_v2_new_work_shadow_only"
                            else:
                                replanned["decision"] = "REVIEW"
                                if not title_compile_failed:
                                    replanned["reason"] = replanned.get("reason") or "date_role_fallback_replan_incomplete"
                            plan = replanned
                    except Exception as exc:
                        if not date_role_adapter_valid:
                            self.stats["date_role_fallback_failed"] += 1
                        else:
                            plan["decision"] = "REVIEW"
                            plan["reason"] = plan.get("reason") or "date_role_fallback_replan_failed"
                        _record_resolver_failure(plan, "date_role_fallback_error", exc)
                    finally:
                        if not date_role_latency_recorded:
                            self.stats["date_role_fallback_latency_ms_total"] += (
                                time.perf_counter() - date_role_started
                            ) * 1000
            if self.mode == "production" and plan.get("decision") == "CREATE":
                owner = str(plan.get("route_owner") or "").strip()
                validated_plan = plan
                production_event = dict(planned_event)
                if jev_v2 and getattr(self.llm_resolver, "new_policy", False):
                    production_event.pop("_jev_v2_authorized_shadow_create", None)
                    production_event["_jev_v2_applicable"] = True
                    production_event["_llm_semantic_intent"] = "follow_up"
                    production_event["_due_date_selection_resolved"] = True
                    if validated_plan.get("title"):
                        production_event["title"] = validated_plan["title"]
                        production_event["_jev_v2_title_verified"] = True
                    if validated_plan.get("due_date") and validated_plan.get("due_iso"):
                        production_event["due_date"] = validated_plan["due_date"]
                        production_event["due_iso"] = validated_plan["due_iso"]
                    else:
                        production_event.pop("due_date", None)
                        production_event.pop("due_iso", None)
                try:
                    executor_names = list(dict.fromkeys(
                        plan.get("routed_executor_names")
                        or plan.get("executor_names")
                        or ([owner] if owner else [])
                    ))
                    if owner and not plan.get("routed_executor_names") and self.self_user.get("name") not in executor_names:
                        executor_names.append(str(self.self_user["name"]))
                    if not executor_names:
                        raise DwsError("external_executor_identity_missing")
                    current_user_name = str(self.self_user.get("name") or "")
                    current_user_id = str(self.self_user.get("user_id") or "")
                    names_to_resolve = [name for name in executor_names if name != current_user_name]
                    resolved_ids = self.dws.resolve_exact(names_to_resolve) if names_to_resolve else {}
                    if set(resolved_ids) != set(names_to_resolve):
                        raise DwsError("external_executor_identity_incomplete")
                    complete_ids = dict(resolved_ids)
                    if current_user_name in executor_names:
                        if not current_user_name or not current_user_id:
                            raise DwsError("current_user_identity_missing")
                        complete_ids[current_user_name] = current_user_id
                    if set(complete_ids) != set(executor_names):
                        raise DwsError("external_executor_identity_incomplete")
                    resolved_values = [str(complete_ids[name]).strip() for name in executor_names]
                    if any(not value for value in resolved_values) or len(set(resolved_values)) != len(resolved_values):
                        raise DwsError("external_executor_identity_not_unique")
                    self.live_ids.update(complete_ids)
                    production_plan = plan_batch(
                        {
                            "config": self.config,
                            "self_user": self.self_user,
                            "existing_actions": self.existing_actions,
                            "messages": [production_event],
                        },
                        mode="production",
                        run_date=self._run_date(event),
                        live_ids=self.live_ids,
                    )["plans"][0]
                    for field in ("title", "due_date", "due_iso"):
                        if field in validated_plan:
                            production_plan[field] = validated_plan[field]
                        elif field in {"due_date", "due_iso"}:
                            production_plan.pop(field, None)
                    for field in (
                        "semantic_evidence",
                        "fallback_semantic_evidence",
                        "date_role_fallback_evidence",
                        "title_field_evidence",
                    ):
                        if field in validated_plan:
                            production_plan[field] = validated_plan[field]
                    production_plan["allow_executor_namespace_fallback"] = True
                    production_plan["identity_resolution_evidence"] = {
                        "method": "dws_get_self_and_exact_contact_search",
                        "verified_names": list(executor_names),
                        "all_ids_unique": True,
                    }
                    plan = production_plan
                except (RuntimeError, OSError, ValueError):
                    plan["decision"] = "REVIEW"
                    plan["reason"] = plan.get("reason") or "external_executor_identity_lookup_failed"
                    plan["executor_ids"] = []
                    plan["executor_names"] = []
                    plan["warnings"] = list(plan.get("warnings") or []) + ["external_executor_identity_lookup_failed"]
            if composite_candidate is not None and composite_components is not None and plan.get("decision") == "CREATE":
                names = [str(value).strip() for value in (plan.get("executor_names") or []) if str(value).strip()]
                ids = [str(value).strip() for value in (plan.get("executor_ids") or []) if str(value).strip()]
                business_scope = str(plan.get("business_key") or "").strip()
                title = str(plan.get("title") or "").strip()
                if (
                    not business_scope or len(business_scope) > 64
                    or not title or len(title) > 80
                    or not names or not ids or len(names) != len(ids)
                    or len(set(names)) != len(names) or len(set(ids)) != len(ids)
                ):
                    plan["decision"] = "REVIEW"
                    plan["reason"] = "composite_parent_plan_incomplete"
                    plan["subtask_decomposition"] = {
                        "contract_version": CHILD_ACTION_CONTRACT_VERSION,
                        "status": "review",
                        "errors": ["parent_scope_title_or_executor_missing"],
                    }
                else:
                    composite_components = [
                        {**row, "executor_names": names, "executor_ids": ids}
                        for row in composite_components
                    ]
                    plan["due_date"] = None
                    plan.pop("due_iso", None)
                    plan["subtask_decomposition"] = {
                        "contract_version": CHILD_ACTION_CONTRACT_VERSION,
                        "status": "ready",
                        "components": composite_components,
                    }
            if llm_completion:
                plan["llm_used"] = True
                plan["llm_fields"] = sorted(llm_completion)
            if semantic_evidence is not None:
                if jev_redaction_counts is not None:
                    semantic_evidence["redaction_counts"] = jev_redaction_counts
                plan["semantic_evidence"] = semantic_evidence
            if jev_redaction_counts is not None:
                plan["redaction_counts"] = jev_redaction_counts
            if plan.get("decision") == "REVIEW":
                _apply_configured_hazard_owner(plan, self.config)
                if jev_v2 and getattr(self.llm_resolver, "new_policy", False):
                    human_confirmation_evidence = _apply_review_field_integrity(
                        plan,
                        event,
                        date_candidates,
                        self.deadline_clarification,
                        human_confirmation_evidence,
                    ) or human_confirmation_evidence
            if human_confirmation_evidence is not None:
                plan["human_confirmation_evidence"] = human_confirmation_evidence
                self.stats["human_clarified_sources"] += 1
            if contextual_followup:
                plan["context_followup_evidence"] = contextual_followup["evidence"]
                plan["title"] = contextual_followup["plan"]["title"]
                plan["due_date"] = None
                plan.pop("due_iso", None)
                plan["detail"] = contextual_followup["detail"]
                plan["comment"] = contextual_followup["comment"]
                if plan.get("decision") == "CREATE":
                    plan["reason"] = "contextual_retrieval_followup"

            if (
                attachment_only
                and causal_notice is not None
                and causal_context_eligible
                and plan.get("reason") != "source_already_processed"
            ):
                safe_title = _strip_opaque_resource_metadata(plan.get("title"))
                normalized_title = re.sub(
                    r"[^\w\u4e00-\u9fff]+",
                    "",
                    unicodedata.normalize("NFKC", safe_title).casefold(),
                )
                bare_file_titles: set[str] = set()
                for file_name, stem in attachment_titles:
                    if stem:
                        bare_file_titles.add(stem)
                    normalized_name = re.sub(
                        r"[^\w\u4e00-\u9fff]+",
                        "",
                        unicodedata.normalize("NFKC", file_name).casefold(),
                    )
                    if normalized_name:
                        bare_file_titles.add(normalized_name)
                if not normalized_title or normalized_title in bare_file_titles:
                    plan["decision"] = "REVIEW"
                    plan["reason"] = "attachment_task_title_unresolved"
                    plan["title"] = ""
                    plan.pop("business_key", None)
                    plan["executor_ids"] = []
                    plan["executor_names"] = []
                    plan["missing_fields"] = list(dict.fromkeys([
                        *(plan.get("missing_fields") or []), "title"
                    ]))
                else:
                    plan["title"] = safe_title
                source_comment = build_source_comment(source_event)
                prior_comment = build_source_comment({
                    **causal_notice,
                    "group": source_event.get("group"),
                })
                causal_detail = (
                    source_comment
                    + "\n\n关联前序通知（用于任务范围和内容上下文；任务来源身份仍为当前附件消息）：\n"
                    + prior_comment
                )
                plan["detail"] = causal_detail
                plan["comment"] = causal_detail

            if (self.config.get("assign_to_self_only") is True
                    and plan.get("decision") == "CREATE"
                    and _possible_cross_group_repeat(source_event, self._arrived_context)):
                plan["decision"] = "REVIEW"
                plan["reason"] = "cross_group_source_possible_duplicate"
                plan["warnings"] = list(dict.fromkeys([
                    *(plan.get("warnings") or []), "cross_group_source_possible_duplicate",
                ]))

            duplicate_task_id = None
            if self.mode == "production" and self.write_ledger and not self.writer_extension_supported:
                plan["decision"] = "REVIEW"
                plan["reason"] = self.writer_extension_error or "writer_extension_contract_unsupported"
                plan["warnings"] = list(dict.fromkeys([
                    *(plan.get("warnings") or []), "writer_extension_contract_unsupported",
                ]))
                self.failed_conversations.add(source_event["conversation_id"])
            if (
                self.mode == "production"
                and self.allow_external_writes
                and plan.get("decision") == "CREATE"
            ):
                try:
                    duplicate_task_id = self._find_existing_business_todo(source_event, plan)
                except (BusinessCandidateError, DwsError, RuntimeError, OSError, ValueError):
                    self.stats["errors"] += 1
                    plan["decision"] = "REVIEW"
                    plan["reason"] = "business_search_incomplete"
                    plan["warnings"] = list(dict.fromkeys([
                        *(plan.get("warnings") or []), "business_search_incomplete"
                    ]))
                if duplicate_task_id:
                    # Keep a source-keyed action row and link it to the verified
                    # existing todo. The durable writer must preserve its
                    # cross-source business-key uniqueness contract as well.
                    plan["business_duplicate_task_id"] = duplicate_task_id

            self._remember_arrived_message(source_event, plan)
            plans.append(plan)
            self._count_plan(plan)
            ledger_action = None
            source_already_processed = bool(plan.pop("_source_already_processed", False)) or (
                plan.get("decision") == "IGNORE"
                and plan.get("reason") == "source_already_processed"
            )
            writer_ready_for_write = not (
                self.mode == "production" and self.write_ledger and not self.writer_extension_supported
            )
            if self.write_ledger and self.writer and writer_ready_for_write and not source_already_processed:
                ledger_action = self._write_ledger(source_event, plan)
            elif not source_already_processed and writer_ready_for_write:
                self._remember_action(source_event, plan)
            should_resume_children = bool(
                composite_components is not None
                and self.mode == "production"
                and self.allow_external_writes
                and self.write_ledger
                and self.writer
                and self.writer_extension_supported
                and (
                    source_already_processed
                    or (
                        ledger_action
                        and not ledger_action.get("created")
                        and ledger_action.get("external_task_id")
                        and not ledger_action.get("duplicate_of_action_id")
                    )
                )
            )
            if should_resume_children:
                prior_decision = str(plan.get("decision") or "REVIEW")
                resume_result = self._resume_composite_children(source_event, composite_components)
                plan["subtask_execution"] = resume_result
                if resume_result.get("status") != "COMPLETED":
                    if prior_decision == "CREATE":
                        self.stats["create"] = max(0, self.stats["create"] - 1)
                    elif prior_decision == "IGNORE":
                        self.stats["ignored"] = max(0, self.stats["ignored"] - 1)
                    if prior_decision != "REVIEW":
                        self.stats["review"] += 1
                    plan["decision"] = "REVIEW"
                    plan["reason"] = "composite_child_recovery_incomplete"
                    self.failed_conversations.add(source_event["conversation_id"])
                elif not source_already_processed:
                    if prior_decision == "CREATE":
                        self.stats["create"] = max(0, self.stats["create"] - 1)
                        self.stats["ignored"] += 1
                    plan["decision"] = "IGNORE"
                    plan["reason"] = "composite_parent_already_linked"
            if ledger_action and ledger_action.get("reason") == "assignment_audit_persistence_failed":
                if plan.get("decision") == "CREATE":
                    self.stats["create"] = max(0, self.stats["create"] - 1)
                    self.stats["review"] += 1
                plan["decision"] = "REVIEW"
                plan["reason"] = "assignment_audit_persistence_failed"
                self.failed_conversations.add(source_event["conversation_id"])
            if (
                self.allow_external_writes
                and self.write_ledger
                and plan.get("decision") == "CREATE"
                and ledger_action
                and ledger_action.get("duplicate_of_action_id")
                and not duplicate_task_id
            ):
                self.stats["errors"] += 1
                self.failed_conversations.add(source_event["conversation_id"])
                self.stats["create"] = max(0, self.stats["create"] - 1)
                self.stats["review"] += 1
                plan["decision"] = "REVIEW"
                plan["reason"] = "business_claim_duplicate_unverified"
                plan["warnings"] = list(dict.fromkeys([
                    *(plan.get("warnings") or []), "business_claim_duplicate_unverified",
                ]))
            if (
                self.allow_external_writes
                and self.write_ledger
                and plan.get("decision") == "CREATE"
                and plan.get("business_key")
                and ledger_action
                and not ledger_action.get("action_id")
                and not ledger_action.get("created")
            ):
                self.stats["errors"] += 1
                self.failed_conversations.add(source_event["conversation_id"])
                self.stats["create"] = max(0, self.stats["create"] - 1)
                self.stats["review"] += 1
                plan["decision"] = "REVIEW"
                plan["reason"] = "business_claim_source_action_missing"
                plan["warnings"] = list(dict.fromkeys([
                    *(plan.get("warnings") or []), "business_claim_source_action_missing",
                ]))
            if duplicate_task_id:
                if not (
                    self.write_ledger
                    and self.writer
                    and ledger_action
                    and ledger_action.get("action_id")
                    and ledger_action.get("reason") != "assignment_audit_persistence_failed"
                ):
                    self.stats["errors"] += 1
                    self.failed_conversations.add(source_event["conversation_id"])
                    if self.stats["create"]:
                        self.stats["create"] -= 1
                    self.stats["review"] += 1
                    plan["decision"] = "REVIEW"
                    plan["reason"] = "business_duplicate_source_link_unavailable"
                else:
                    try:
                        duplicate_action_id = ledger_action.get("duplicate_of_action_id")
                        canonical_action_id = ledger_action.get("canonical_action_id") or duplicate_action_id
                        canonical_task_id = ledger_action.get("canonical_external_task_id")
                        if duplicate_action_id:
                            if canonical_task_id and canonical_task_id != duplicate_task_id:
                                raise DwsError("business_canonical_task_id_conflict")
                            if not canonical_task_id:
                                self._persist_created_task(
                                    ledger_action, duplicate_task_id, action_id=canonical_action_id
                                )
                            self._persist_action_status(ledger_action["action_id"], "IGNORED", None)
                        else:
                            if ledger_action.get("effective_external_task_id") not in (None, "", duplicate_task_id):
                                raise DwsError("business_source_task_id_conflict")
                            if ledger_action.get("effective_external_task_id") != duplicate_task_id:
                                self._persist_created_task(ledger_action, duplicate_task_id)
                        plan["external_task_id"] = duplicate_task_id
                        plan["decision"] = "IGNORE"
                        plan["reason"] = "business_todo_already_exists"
                        if self.stats["create"]:
                            self.stats["create"] -= 1
                        self.stats["ignored"] += 1
                        for action in self.existing_actions:
                            if (
                                action.get("conversation_id") == source_event["conversation_id"]
                                and action.get("message_id") == source_event["message_id"]
                            ):
                                action["effective_external_task_id"] = duplicate_task_id
                                action["canonical_action_id"] = canonical_action_id if duplicate_action_id else action.get("action_id")
                                if duplicate_action_id:
                                    action.pop("external_task_id", None)
                                    action["duplicate_of_action_id"] = duplicate_action_id
                                else:
                                    action["external_task_id"] = duplicate_task_id
                                action["status"] = "IGNORED"
                    except (RuntimeError, OSError, ValueError):
                        self.stats["errors"] += 1
                        self.failed_conversations.add(source_event["conversation_id"])
                        if self.stats["create"]:
                            self.stats["create"] -= 1
                        self.stats["review"] += 1
                        plan["decision"] = "REVIEW"
                        plan["reason"] = "business_duplicate_source_link_failed"
            if (
                self.allow_external_writes
                and self.write_ledger
                and plan.get("decision") == "CREATE"
                and ledger_action
                and ledger_action.get("created")
                and not ledger_action.get("action_id")
            ):
                self.stats["create"] = max(0, self.stats["create"] - 1)
                self.stats["review"] += 1
                plan["decision"] = "REVIEW"
                plan["reason"] = "durable_create_claim_missing_action_id"
                plan["warnings"] = list(plan.get("warnings") or []) + ["durable_create_claim_missing_action_id"]
            if (
                self.allow_external_writes
                and self.mode in {"test", "production"}
                and plan.get("decision") == "CREATE"
                and (not self.write_ledger or bool(ledger_action and ledger_action.get("created")))
            ):
                comment_state_callback = None
                if self.write_ledger and self.writer and ledger_action and ledger_action.get("action_id"):
                    def comment_state_callback(value: str | None) -> None:
                        self._persist_action_status(ledger_action["action_id"], "EXTERNAL_CREATED", value)
                        ledger_action["last_error"] = value
                try:
                    creation_action_id = ledger_action.get("action_id") if ledger_action else None
                    if ledger_action and self.write_ledger:
                        ledger_action["last_error"] = "create_write_started"
                    task_id = self._write_external(
                        source_event,
                        plan,
                        action_id=creation_action_id,
                        claim_acquired=bool(ledger_action and ledger_action.get("created") is True),
                        test_only=self.mode == "test" and not self.write_ledger,
                        on_created=(
                            (lambda value: self._persist_created_task(ledger_action, value))
                            if self.write_ledger and self.writer and ledger_action and ledger_action.get("action_id")
                            else None
                        ),
                        on_comment_state=comment_state_callback,
                    )
                    plan["external_task_id"] = task_id
                    if composite_components is not None and ledger_action and ledger_action.get("action_id"):
                        child_result = run_durable_child_actions(
                            parent_action_id=ledger_action["action_id"],
                            parent_task_id=task_id,
                            conversation_id=source_event["conversation_id"],
                            message_id=source_event["message_id"],
                            business_scope=str(plan.get("business_key") or ""),
                            executor_ids=plan.get("executor_ids") or [],
                            executor_names=plan.get("executor_names") or [],
                            components=composite_components,
                            writer=self.writer,
                            dws=self.dws,
                            allow_external_writes=True,
                        )
                        plan["subtask_execution"] = child_result
                        if child_result.get("status") != "COMPLETED":
                            self._persist_action_status(
                                ledger_action["action_id"], "EXTERNAL_CREATED", "composite_child_actions_incomplete"
                            )
                            self.failed_conversations.add(source_event["conversation_id"])
                            self.stats["errors"] += 1
                            self.stats["create"] = max(0, self.stats["create"] - 1)
                            self.stats["review"] += 1
                            plan["decision"] = "REVIEW"
                            plan["reason"] = "composite_child_actions_incomplete"
                        else:
                            self._persist_action_status(ledger_action["action_id"], "EXTERNAL_CREATED", None)
                except (RuntimeError, OSError, ValueError) as exc:
                    self.stats["errors"] += 1
                    self.failed_conversations.add(source_event["conversation_id"])
                    if self.stats["create"]:
                        self.stats["create"] -= 1
                    self.stats["review"] += 1
                    plan["decision"] = "REVIEW"
                    plan["warnings"] = list(plan.get("warnings") or []) + [str(exc)]
                    if self.write_ledger and self.writer and ledger_action and ledger_action.get("action_id"):
                        marker = ledger_action.get("last_error")
                        task_id = getattr(exc, "task_id", None)
                        if task_id:
                            plan["external_task_id"] = task_id
                            last_error = f"create_link_outcome_unknown:{task_id}"
                        else:
                            last_error = marker if marker == "source_comment_write_started" else str(exc)[:4000]
                        try:
                            self._persist_action_status(
                                ledger_action["action_id"],
                                "CLAIMED" if marker == "create_write_started" and not task_id else "REVIEW",
                                "create_outcome_unknown" if marker == "create_write_started" and not task_id else last_error,
                                external_task_id=task_id,
                            )
                        except (RuntimeError, OSError, ValueError):
                            pass

        if self.write_ledger and self.writer and self.writer_extension_supported:
            self._advance_cursors(events)
        return plans

    @staticmethod
    def _run_date(event: dict[str, Any]) -> date:
        value = event.get("create_time")
        if value:
            return _local_datetime(value).date()
        return datetime.now(CHINA_TZ).date()

    def _count_plan(self, plan: dict[str, Any]) -> None:
        decision = str(plan.get("decision") or "REVIEW").lower()
        key = "ignored" if decision == "ignore" else decision
        if key in self.stats:
            self.stats[key] += 1

    def _find_existing_business_todo(
        self, event: dict[str, Any], plan: dict[str, Any]
    ) -> str | None:
        if self.dws is None:
            raise BusinessCandidateError("business_search_adapter_unavailable")
        query = str(plan.get("title") or "").strip()
        if not query:
            raise BusinessCandidateError("business_search_title_missing")
        if query not in self._business_search_cache:
            search = getattr(self.dws, "search_todos", None)
            if not callable(search):
                raise BusinessCandidateError("business_search_adapter_unavailable")
            rows = search(query)
            if not isinstance(rows, list):
                raise BusinessCandidateError("business_search_result_invalid")
            if len(rows) > 50:
                raise BusinessCandidateError("business_search_candidate_limit_exceeded")
            normalized_rows: list[dict[str, Any]] = []
            seen_ids: set[str] = set()
            for row in rows:
                if not isinstance(row, dict):
                    raise BusinessCandidateError("business_search_result_invalid")
                task_id = str(row.get("taskId") or row.get("todoTaskId") or "").strip()
                if not task_id:
                    raise BusinessCandidateError("business_search_task_id_missing")
                if task_id in seen_ids:
                    continue
                seen_ids.add(task_id)
                normalized_rows.append({**row, "taskId": task_id})
            self._business_search_cache[query] = normalized_rows

        for row in self._business_search_cache[query]:
            task_id = row["taskId"]
            detail = self.dws.get_todo(task_id)
            if not isinstance(detail, dict):
                raise BusinessCandidateError("business_candidate_detail_incomplete")
            returned_id = str(detail.get("taskId") or detail.get("todoTaskId") or "").strip()
            if returned_id != task_id:
                raise BusinessCandidateError("business_candidate_task_id_mismatch")
            from business_dedupe import normalize_business_text
            if normalize_business_text(detail.get("subject")) != normalize_business_text(plan.get("title")):
                continue
            is_done = detail.get("isDone")
            if not isinstance(is_done, bool):
                raise BusinessCandidateError("business_candidate_status_readback_incomplete")
            if is_done:
                continue
            comments_reader = getattr(self.dws, "list_task_comments", None)
            if not callable(comments_reader):
                raise BusinessCandidateError("business_search_comment_reader_unavailable")
            due_value = detail.get("dueTime") or detail.get("due")
            if due_value not in (None, ""):
                from business_dedupe import _timestamp_ms
                due_ms = _timestamp_ms(due_value)
                if due_ms is None:
                    raise BusinessCandidateError("business_candidate_deadline_readback_invalid")
                due_date = datetime.fromtimestamp(due_ms / 1000, tz=CHINA_TZ).date()
                if due_date < datetime.now(CHINA_TZ).date():
                    continue
            comments = comments_reader(task_id)
            if not isinstance(comments, list):
                raise BusinessCandidateError("business_candidate_source_comments_incomplete")
            if candidate_matches_business(
                task_id=task_id,
                detail=detail,
                comments=comments,
                plan=plan,
                event=event,
                today=datetime.now(CHINA_TZ).date(),
            ):
                return task_id
        return None

    def _reconcile_source_link_readback(
        self, event: dict[str, Any], plan: dict[str, Any]
    ) -> None:
        if self.mode != "production" or plan.get("reason") != "source_already_processed" or self.dws is None:
            return
        linked = [
            row for row in self.existing_actions
            if isinstance(row, dict)
            and _as_text(row.get("conversation_id")) == _as_text(event.get("conversation_id"))
            and _as_text(row.get("message_id")) == _as_text(event.get("message_id"))
            and row.get("action_key", "todo") == "todo"
            and row.get("parent_action_id") in (None, "")
            and row.get("duplicate_of_action_id") in (None, "")
            and _as_text(row.get("external_task_id"))
        ]
        if not linked:
            return
        if len(linked) != 1:
            plan["decision"] = "REVIEW"
            plan["reason"] = "linked_task_readback_ambiguous"
            return
        task_id = _as_text(linked[0].get("external_task_id"))
        try:
            detail = self.dws.get_todo(task_id)
            returned_id = _as_text(detail.get("taskId") or detail.get("todoTaskId")) if isinstance(detail, dict) else ""
            if returned_id != task_id:
                raise DwsError("linked taskId mismatch")
        except (RuntimeError, OSError, ValueError):
            plan["decision"] = "REVIEW"
            plan["reason"] = "linked_task_readback_unverified"
            plan["external_task_id"] = task_id
            return
        plan["linked_task_readback"] = {"task_id_verified": True}
        plan["external_task_id"] = task_id

    def _resume_composite_children(
        self, event: dict[str, Any], components: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Resume only child actions whose exact source parent is already durable."""
        source_rows = [
            row for row in self.existing_actions
            if isinstance(row, dict)
            and row.get("conversation_id") == event.get("conversation_id")
            and row.get("message_id") == event.get("message_id")
            and row.get("action_key", "todo") == "todo"
        ]
        if len(source_rows) != 1:
            return {"status": "REVIEW", "created": 0, "resumed": 0, "errors": ["composite_parent_action_unavailable"]}
        parent = source_rows[0]
        if parent.get("duplicate_of_action_id") not in (None, ""):
            return {"status": "REVIEW", "created": 0, "resumed": 0, "errors": ["composite_parent_is_duplicate"]}
        action_id = parent.get("action_id")
        task_id = str(parent.get("external_task_id") or "").strip()
        business_scope = str(parent.get("business_key") or "").strip()
        if (
            isinstance(action_id, bool) or not isinstance(action_id, int) or action_id <= 0
            or not task_id or len(task_id) > 128
            or not business_scope or len(business_scope) > 64
        ):
            return {"status": "REVIEW", "created": 0, "resumed": 0, "errors": ["composite_parent_link_incomplete"]}
        audit = parent.get("rule_audit")
        if isinstance(audit, (bytes, bytearray)):
            audit = audit.decode("utf-8")
        if isinstance(audit, str):
            try:
                audit = json.loads(audit)
            except json.JSONDecodeError:
                audit = None
        if not isinstance(audit, dict) or audit.get("awareness_only") is True:
            return {"status": "REVIEW", "created": 0, "resumed": 0, "errors": ["composite_assignment_audit_unavailable"]}
        names = [str(audit.get("primary_owner") or "").strip()]
        names.extend(str(name).strip() for name in (audit.get("collaborators") or []) if str(name).strip())
        names = [name for name in names if name]
        if not names or len(names) != len(set(names)):
            return {"status": "REVIEW", "created": 0, "resumed": 0, "errors": ["composite_executor_names_unavailable"]}
        try:
            detail = self.dws.get_todo(task_id)
            returned_id = str(detail.get("taskId") or detail.get("todoTaskId") or "").strip() if isinstance(detail, dict) else ""
            parent_id = str(detail.get("parentId") or detail.get("parentTaskId") or "").strip() if isinstance(detail, dict) else ""
            if returned_id != task_id or parent_id:
                raise DwsError("composite_parent_detail_mismatch")
            comments = self.dws.list_task_comments(task_id)
            expected_comment = build_source_comment(event)
            if not isinstance(comments, list) or not any(
                isinstance(row, dict) and row.get("content") == expected_comment for row in comments
            ):
                return {"status": "REVIEW", "created": 0, "resumed": 0, "errors": ["composite_parent_source_comment_unverified"]}
        except (RuntimeError, OSError, ValueError):
            return {"status": "REVIEW", "created": 0, "resumed": 0, "errors": ["composite_parent_readback_incomplete"]}
        current_name = str(self.self_user.get("name") or "")
        current_id = str(self.self_user.get("user_id") or "")
        try:
            unresolved_names = [name for name in names if name != current_name]
            resolved = self.dws.resolve_exact(unresolved_names) if unresolved_names else {}
            if set(resolved) != set(unresolved_names):
                raise DwsError("composite_executor_identity_incomplete")
            ids_by_name = {str(name): str(value).strip() for name, value in resolved.items()}
            if current_name in names:
                if not current_id:
                    raise DwsError("composite_current_user_identity_missing")
                ids_by_name[current_name] = current_id
            executor_ids = [ids_by_name[name] for name in names]
            if any(not value for value in executor_ids) or len(executor_ids) != len(set(executor_ids)):
                raise DwsError("composite_executor_identity_not_unique")
        except (RuntimeError, OSError, ValueError, KeyError):
            return {"status": "REVIEW", "created": 0, "resumed": 0, "errors": ["composite_executor_identity_unverified"]}
        result = run_durable_child_actions(
            parent_action_id=action_id,
            parent_task_id=task_id,
            conversation_id=str(event["conversation_id"]),
            message_id=str(event["message_id"]),
            business_scope=business_scope,
            executor_ids=executor_ids,
            executor_names=names,
            components=components,
            writer=self.writer,
            dws=self.dws,
            allow_external_writes=True,
        )
        try:
            self._persist_action_status(
                action_id,
                "EXTERNAL_CREATED",
                None if result.get("status") == "COMPLETED" else "composite_child_actions_incomplete",
            )
        except (RuntimeError, OSError, ValueError):
            return {"status": "REVIEW", "created": result.get("created", 0),
                    "resumed": result.get("resumed", 0), "errors": ["composite_parent_status_persist_failed"]}
        return result

    def _causal_notice_for_attachment(
        self, event: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, str]:
        """Find one strictly preceding, same-source-author notice for a file-only event."""
        titles = _source_file_titles(event)
        if not _is_attachment_only_source(event, titles):
            return None, "not_attachment_only"
        if not titles:
            return None, "attachment_filename_missing"
        if len(titles) != 1:
            return None, "attachment_filename_ambiguous"
        if self.source_order_provenance != COMPLETE_DWS_PAGE_ORDER:
            return None, "attachment_order_unverified"

        conversation_id = _as_text(event.get("conversation_id"))
        configured_group = _as_text(self.group_map.get(conversation_id))
        group = _as_text(event.get("group"))
        sender_id = _as_text(event.get("sender_id"))
        message_id = _as_text(event.get("message_id"))
        if not conversation_id or not configured_group or group != configured_group:
            return None, "attachment_conversation_unverified"
        if not sender_id or not message_id:
            return None, "attachment_sender_unverified"
        try:
            current_time = _local_datetime(event.get("create_time"))
        except (TypeError, ValueError):
            return None, "attachment_time_unverified"

        _title, title_stem = titles[0]
        matches: list[dict[str, Any]] = []
        explicit_refs = _source_reference_ids(event)
        for context in event.get("related_contexts") or []:
            if not isinstance(context, dict):
                continue
            context_id = _as_text(context.get("message_id"))
            context_conversation = _as_text(context.get("conversation_id"))
            context_sender = _as_text(context.get("sender_id"))
            context_text = _as_text(context.get("text"))
            if (
                not context_id
                or context_id not in explicit_refs
                or context_conversation != conversation_id
                or not sender_id
                or context_sender != sender_id
                or not context_text
                or not any(term in context_text for term in ACTION_TERMS)
                or not _file_title_matches_notice(title_stem, context_text)
            ):
                continue
            try:
                context_time = _local_datetime(context.get("create_time"))
            except (TypeError, ValueError):
                continue
            age_seconds = (current_time - context_time).total_seconds()
            if not 0 < age_seconds <= CAUSAL_NOTICE_MAX_AGE_SECONDS:
                continue
            matches.append({
                "conversation_id": conversation_id,
                "message_id": context_id,
                "group": configured_group,
                "sender_id": context_sender,
                "sender": _as_text(context.get("sender")),
                "create_time": _as_text(context.get("create_time")),
                "text": context_text,
                "resource_refs": list(context.get("resource_refs") or []),
                "at_users": [],
                "causal_notice_candidate": True,
            })
        for prior in reversed(self._arrived_context):
            if (
                prior.get("causal_notice_candidate") is not True
                or prior.get("conversation_id") != conversation_id
                or prior.get("group") != configured_group
                or not _as_text(prior.get("message_id"))
                or _as_text(prior.get("message_id")) == message_id
                or not _as_text(prior.get("sender_id"))
                or _as_text(prior.get("sender_id")) != sender_id
            ):
                continue
            try:
                prior_time = _local_datetime(prior.get("create_time"))
            except (TypeError, ValueError):
                continue
            age_seconds = (current_time - prior_time).total_seconds()
            if not 0 < age_seconds <= CAUSAL_NOTICE_MAX_AGE_SECONDS:
                continue
            if _file_title_matches_notice(title_stem, _as_text(prior.get("text"))):
                matches.append(prior)

        unique_matches = {
            (_as_text(row.get("conversation_id")), _as_text(row.get("message_id"))): row
            for row in matches
        }
        if len(unique_matches) == 1:
            return next(iter(unique_matches.values())), "matched"
        if len(unique_matches) > 1:
            return None, "attachment_notice_ambiguous"
        return None, "attachment_notice_missing"

    def _causal_notice_scope(self, notice: dict[str, Any]) -> str:
        """Classify only the notice's program-evidenced assignment scope."""
        text = _as_text(notice.get("text"))
        group = _as_text(notice.get("group"))
        instruction_scope = _instruction_clause_scope(notice, self.self_user)
        inferred_scope = infer_semantic_context(text, group).get("target_scope")

        if instruction_scope != "unresolved":
            return instruction_scope
        if inferred_scope == "department_only":
            return "outside_scope"
        # A configured source conversation that the existing router already
        # recognizes as the managed workgroup retains its current scope rule.
        if inferred_scope == "managed_workgroup":
            return "eligible"
        return "unresolved"

    @staticmethod
    def _event_with_causal_notice(
        event: dict[str, Any], notice: dict[str, Any]
    ) -> dict[str, Any]:
        """Give planners notice text while preserving the file message identity and payload."""
        contextual = dict(event)
        contextual["text"] = _as_text(notice.get("text"))
        contextual["at_users"] = notice.get("at_users") or []
        contextual["create_time"] = notice.get("create_time") or event.get("create_time")
        return contextual

    @staticmethod
    def _clear_assignment_scope_fields(plan: dict[str, Any]) -> None:
        plan["due_date"] = None
        plan["due_iso"] = None
        plan["route_owner"] = None
        plan["route_module"] = None
        plan["matrix_route_owner"] = None
        plan["assignment_target"] = None
        plan["assignment_basis"] = None
        plan["executor_ids"] = []
        plan["executor_names"] = []

    @staticmethod
    def _mark_attachment_context_review(plan: dict[str, Any], reason: str) -> None:
        plan["decision"] = "REVIEW"
        plan["reason"] = reason
        EventProcessor._clear_attachment_filename_inference(plan)
        plan["missing_fields"] = list(dict.fromkeys([
            *(plan.get("missing_fields") or []), "title", "workgroup_scope"
        ]))
        plan["warnings"] = list(dict.fromkeys([
            *(plan.get("warnings") or []), "attachment_context_unresolved"
        ]))

    @staticmethod
    def _clear_attachment_filename_inference(plan: dict[str, Any]) -> None:
        plan["title"] = ""
        plan["due_date"] = None
        plan["due_iso"] = None
        plan["route_owner"] = None
        plan["route_module"] = None
        plan["matrix_route_owner"] = None
        plan["assignment_target"] = None
        plan["assignment_basis"] = None
        plan["matched_signals"] = []
        plan["semantic_context"] = {
            "canonical_workgroup": None,
            "target_scope": "unresolved_attachment_context",
            "targeted_workgroups": [],
            "source_group_is_local": False,
        }
        plan.pop("business_key", None)
        plan["executor_ids"] = []
        plan["executor_names"] = []

    def _arrived_notice_supporting_artifact(self, event: dict[str, Any]) -> dict[str, Any] | None:
        if not _verified_message_sender(event, self.self_user):
            return None
        titles = _source_file_titles(event)
        if not _is_attachment_only_source(event, titles):
            return None
        conversation_id = _as_text(event.get("conversation_id"))
        group = _as_text(event.get("group"))
        sender_id = _as_text(event.get("sender_id"))
        if not conversation_id or not group or not sender_id:
            return None
        try:
            current_time = _local_datetime(event.get("create_time"))
        except (TypeError, ValueError):
            return None
        current_conversation_sequence = self._conversation_arrival_counts.get(conversation_id, 0) + 1
        current_references = _source_reference_ids(event)

        for prior in reversed(self._arrived_context):
            if not prior.get("self_authored_notice"):
                continue
            if (
                prior.get("conversation_id") != conversation_id
                or prior.get("group") != group
                or prior.get("sender_verified_self") is not True
            ):
                continue
            sequence_gap = current_conversation_sequence - int(prior.get("conversation_sequence", 0))
            if not 0 < sequence_gap <= ARRIVED_NOTICE_MAX_CONVERSATION_GAP:
                continue
            try:
                prior_time = _local_datetime(prior.get("create_time"))
            except (TypeError, ValueError):
                continue
            age_seconds = (current_time - prior_time).total_seconds()
            if age_seconds < 0 or age_seconds > ARRIVED_NOTICE_MAX_AGE_SECONDS:
                continue
            if age_seconds == 0 and self.source_order_provenance != COMPLETE_DWS_PAGE_ORDER:
                continue

            explicit_reference = str(prior.get("message_id") or "") in current_references
            title_match = any(
                _file_title_matches_notice(title_stem, str(prior.get("text") or ""))
                for _title, title_stem in titles
            )
            if explicit_reference or title_match:
                return prior
        return None

    def _contextual_retrieval_followup(
        self,
        event: dict[str, Any],
        original_plan: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Build one local task from scoped source evidence and a later retrieval instruction."""
        conversation_id = _as_text(event.get("conversation_id"))
        group = _as_text(event.get("group"))
        sender_id = _as_text(event.get("sender_id"))
        configured_group = _as_text(self.group_map.get(conversation_id))
        self_name = _as_text(self.self_user.get("name") or self.self_user.get("display_name"))
        if (
            self.source_order_provenance != COMPLETE_DWS_PAGE_ORDER
            or not conversation_id
            or not configured_group
            or group != configured_group
            or not _as_text(event.get("message_id"))
            or not sender_id
            or not _as_text(self.self_user.get("user_id"))
            or not self_name
            or original_plan.get("decision") == "IGNORE"
            or original_plan.get("reason") in {"source_already_processed", "business_already_processed"}
        ):
            return None
        try:
            current_time = _local_datetime(event.get("create_time"))
        except (TypeError, ValueError):
            return None
        current_sequence = self._conversation_arrival_counts.get(conversation_id, 0) + 1
        candidates: list[dict[str, Any]] = []
        for prior in self._arrived_context:
            if (
                prior.get("conversation_id") != conversation_id
                or prior.get("group") != group
                or not isinstance(prior.get("inspection_candidate"), dict)
            ):
                continue
            try:
                prior_time = _local_datetime(prior.get("create_time"))
            except (TypeError, ValueError):
                continue
            gap = current_sequence - int(prior.get("conversation_sequence", 0))
            age = (current_time - prior_time).total_seconds()
            if (
                gap <= 0
                or age <= 0
                or age > CONTEXT_FOLLOWUP_MAX_AGE_SECONDS
            ):
                continue
            status = prior.get("inspection_status")
            prior_sender_id = _as_text(prior.get("sender_id"))
            if isinstance(status, dict) and prior_sender_id:
                candidates.append(prior)
        if len(candidates) != 1:
            return None
        prior = candidates[0]
        prior_sequence = int(prior.get("conversation_sequence", 0))
        prior_sender_id = _as_text(prior.get("sender_id"))
        if not prior_sender_id or prior_sender_id == sender_id:
            return None
        status = prior["inspection_status"]
        item = _as_text(status.get("item"))
        if not item or not _as_text(prior.get("text")):
            return None
        for row in self._arrived_context:
            if (
                row.get("conversation_id") != conversation_id
                or row.get("group") != group
                or int(row.get("conversation_sequence", 0)) <= prior_sequence
                or int(row.get("conversation_sequence", 0)) >= current_sequence
            ):
                continue
            row_text = _as_text(row.get("text"))
            if (
                isinstance(row.get("inspection_candidate"), dict)
                or _terminal_update_applies_to_managed_item(row_text, item, group)
            ):
                return None

        instruction = _retrieval_instruction(
            _as_text(event.get("text") or event.get("content")), item
        )
        if instruction is None:
            return None
        site = _as_text(status.get("workgroup_prefix")) or _inspection_group_label(group)
        prior_text = _as_text(prior.get("text"))
        site_prefix = site
        title = (
            instruction["meeting_condition"]
            + "取回"
            + site_prefix
            + status["inspection_action"]
            + item
        )
        if not 0 < len(title) <= 80:
            return None

        focused_event = dict(event)
        focused_event["text"] = instruction["instruction_evidence"]
        focused_event.pop("content", None)
        focused_event.pop("due_date", None)
        focused_event.pop("due_iso", None)
        focused_event.update(
            {
                "title": title,
                "_llm_semantic_intent": "follow_up",
                "_jev_v2_applicable": True,
                "_jev_v2_title_verified": True,
                "_due_date_selection_resolved": True,
                "_contextual_followup_verified": CONTEXTUAL_FOLLOWUP_VERIFIED_TOKEN,
            }
        )
        focused_plan = plan_batch(
            {
                "config": self.config,
                "self_user": self.self_user,
                "existing_actions": self.existing_actions,
                "messages": [focused_event],
            },
            mode="test",
            run_date=self._run_date(event),
        )["plans"][0]
        if focused_plan.get("reason") != "business_already_processed":
            if self.business_candidate_query_complete is not True or self.business_candidate_pool_issue:
                focused_plan["decision"] = "REVIEW"
                focused_plan["reason"] = (
                    self.business_candidate_pool_issue
                    or "business_candidate_query_incomplete"
                )
                focused_plan["warnings"] = list(dict.fromkeys([
                    *(focused_plan.get("warnings") or []), focused_plan["reason"]
                ]))
            elif focused_plan.get("decision") == "CREATE":
                candidates, overflow = recall_business_candidates(
                    focused_plan,
                    self.existing_actions,
                    allow_simulated=self.allow_simulated_candidates,
                )
                if overflow:
                    focused_plan["decision"] = "REVIEW"
                    focused_plan["reason"] = "business_candidate_pool_overflow"
                    focused_plan["warnings"] = list(dict.fromkeys([
                        *(focused_plan.get("warnings") or []), "business_candidate_pool_overflow"
                    ]))
                elif candidates:
                    focused_plan["decision"] = "REVIEW"
                    focused_plan["reason"] = "contextual_followup_candidates_present"
                    focused_plan["warnings"] = list(dict.fromkeys([
                        *(focused_plan.get("warnings") or []), "contextual_followup_candidates_present"
                    ]))

        prior_source = {
            "conversation_id": prior["conversation_id"],
            "message_id": prior["message_id"],
            "group": prior["group"],
            "create_time": prior["create_time"],
            "sender_id": prior.get("sender_id"),
            "sender": prior.get("sender"),
            "text": prior_text,
            "evidence_span": status["unfinished_evidence"],
            "resource_refs": list(prior.get("resource_refs") or []),
        }
        instruction_source = {
            "conversation_id": conversation_id,
            "message_id": _as_text(event.get("message_id")),
            "group": group,
            "create_time": _as_text(event.get("create_time")),
            "sender_id": sender_id,
            "sender": _as_text(event.get("sender")),
            "text": _as_text(event.get("text") or event.get("content")),
            "evidence_span": instruction["instruction_evidence"],
            "resource_refs": list(event.get("resource_refs") or []),
        }
        evidence = {
            "policy": "managed_workgroup_unfinished_inspection_retrieval_v1",
            "derived": {
                "item": item,
                "workgroup_prefix": site,
                "scope_basis": status["scope_basis"],
                "inspection_action": status["inspection_action"],
                "retrieval_action": instruction["action"],
                "event_condition": instruction["meeting_condition"],
                "due_date_basis": "event_condition_is_not_a_calendar_deadline",
            },
            "prior_source": prior_source,
            "instruction_source": instruction_source,
        }
        full_comment = _as_text(original_plan.get("comment"))
        causal_note = (
            "\n关联的未完成事项来源：\n"
            f"群：{prior_source['group']}\n"
            f"发送人：{prior_source['sender'] or '未标明'}\n"
            f"消息时间：{prior_source['create_time']}\n"
            f"messageId：{prior_source['message_id']}\n"
            f"原文：\n{prior_text}\n"
            f"关联证据：{status['unfinished_evidence']}"
        )
        focused_plan.update(
            {
                "title": title,
                "due_date": None,
                "due_iso": None,
                "detail": full_comment,
                "comment": full_comment + causal_note,
                "context_followup_evidence": evidence,
            }
        )
        if focused_plan.get("decision") == "CREATE":
            focused_plan["reason"] = "contextual_retrieval_followup"
        return {
            "plan": focused_plan,
            "candidate_plan": focused_plan,
            "planned_event": focused_event,
            "detail": full_comment,
            "comment": full_comment + causal_note,
            "evidence": evidence,
        }

    def _remember_arrived_message(self, event: dict[str, Any], plan: dict[str, Any]) -> None:
        conversation_id = _as_text(event.get("conversation_id"))
        message_id = _as_text(event.get("message_id"))
        if not conversation_id or not message_id:
            return
        identity = (conversation_id, message_id)
        if any((row.get("conversation_id"), row.get("message_id")) == identity for row in self._arrived_context):
            return
        self._arrival_sequence += 1
        conversation_sequence = self._conversation_arrival_counts.get(conversation_id, 0) + 1
        self._conversation_arrival_counts[conversation_id] = conversation_sequence
        verified_self = _verified_message_sender(event, self.self_user)
        self_notice = bool(verified_self and plan.get("reason") == "self_authored_notice")
        text = _as_text(event.get("text") or event.get("content"))
        sender_id = _as_text(event.get("sender_id"))
        group = _as_text(event.get("group"))
        allow_unscoped = verified_self and group in LOCAL_WORKGROUP_SOURCE_GROUPS
        inspection_candidate = _unfinished_inspection_status(
            text,
            group,
            allow_unscoped=allow_unscoped,
        )
        inspection_status = inspection_candidate
        file_titles = _source_file_titles(event)
        attachment_only = _is_attachment_only_source(event, file_titles)
        direct_self_assignment, _assignment_basis = _self_assignment(event, self.self_user)
        direct_other_assignment = _has_explicit_other_assignment(event, self.self_user)
        inferred_scope = infer_semantic_context(text, group).get("target_scope")
        causal_scope_signal = bool(
            direct_self_assignment
            or direct_other_assignment
            or _explicit_managed_workgroup_scope(text)
            or inferred_scope in {"department_only", "managed_workgroup"}
        )
        causal_notice_candidate = bool(
            sender_id
            and text
            and not attachment_only
            and causal_scope_signal
            and any(term in text for term in ACTION_TERMS)
        )
        retain_inspection_context = bool(
            inspection_candidate or _INSPECTION_CONTEXT_RE.search(text)
        )
        self._arrived_context.append({
            "conversation_id": conversation_id,
            "message_id": message_id,
            "group": _as_text(event.get("group")),
            "sender_verified_self": verified_self,
            "create_time": _as_text(event.get("create_time")),
            "sender_id": _as_text(event.get("sender_id")),
            "sender": _as_text(event.get("sender")),
            "planned_decision": plan.get("decision"),
            "text": text[:4000] if self_notice or retain_inspection_context or causal_notice_candidate else "",
            "resource_refs": list(event.get("resource_refs") or [])[:16]
            if self_notice or retain_inspection_context else [],
            "at_users": list(event.get("at_users") or [])[:16] if causal_notice_candidate else [],
            "inspection_candidate": inspection_candidate,
            "inspection_status": inspection_status,
            "arrival_sequence": self._arrival_sequence,
            "conversation_sequence": conversation_sequence,
            "self_authored_notice": self_notice and not attachment_only,
            "causal_notice_candidate": causal_notice_candidate,
        })
        if len(self._arrived_context) > ARRIVED_CONTEXT_LIMIT:
            self._arrived_context = self._arrived_context[-ARRIVED_CONTEXT_LIMIT:]
        self.stats["arrived_context_messages"] = len(self._arrived_context)

    @staticmethod
    def _safe_jev_redaction_counts(resolver: Any) -> dict[str, dict[str, int]]:
        """Copy only allowlisted nonnegative counters into an auditable plan."""
        categories = ("resource_ids", "phone_numbers", "person_mentions", "embedded_ids")
        scopes = ("message", "business_candidates", "total")
        source = getattr(resolver, "last_redaction_counts", None)
        if not isinstance(source, dict):
            source = {}
        result: dict[str, dict[str, int]] = {}
        for scope in scopes:
            values = source.get(scope)
            if not isinstance(values, dict):
                values = {}
            result[scope] = {
                category: value if type(value) is int and value >= 0 else 0
                for category in categories
                for value in [values.get(category)]
            }
        return result

    def _remember_action(self, event: dict[str, Any], plan: dict[str, Any]) -> None:
        identity = (event.get("conversation_id"), event.get("message_id"))
        if any(
            (row.get("conversation_id"), row.get("message_id")) == identity
            for row in self.existing_actions
        ):
            return
        action = {
            "conversation_id": event.get("conversation_id"),
            "message_id": event.get("message_id"),
            "action_key": "todo",
        }
        if plan.get("source_key"):
            action["source_key"] = plan["source_key"]
        if plan.get("business_key"):
            action["business_key"] = plan["business_key"]
        self.existing_actions.append(action)

    def _write_ledger(self, event: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
        if not self.writer:
            return {"created": False, "reason": "writer_unavailable"}
        self.writer.execute(
            {
                "operation": "record_message",
                "conversation_id": event["conversation_id"],
                "message_id": event["message_id"],
                "group_name": event["group"],
                "sender_id": event.get("sender_id"),
                "sender_name": event.get("sender"),
                "message_time": event.get("create_time"),
                "text": event.get("text"),
                "msg_type": event.get("msg_type"),
                "resource_refs": event.get("resource_refs"),
                "quoted_message": event.get("quoted_message"),
                "raw_payload": event.get("raw_payload"),
                "source": "hermes_event",
            }
        )
        self.stats["ledger_writes"] += 1
        decision = str(plan.get("decision") or "REVIEW")
        status = {"CREATE": "CLAIMED", "REVIEW": "REVIEW", "IGNORE": "IGNORED"}.get(decision, "REVIEW")
        action = self.writer.execute(
            {
                "operation": "claim_action",
                "conversation_id": event["conversation_id"],
                "message_id": event["message_id"],
                "action_key": "todo",
                # Keep the global unique business claim; the writer records a
                # separate source row with duplicate_of_action_id on collision.
                "business_key": plan.get("business_key"),
                "status": status,
            }
        )
        self.stats["ledger_writes"] += 1
        if self.mode == "production" and action.get("action_id"):
            try:
                audit = persistable_assignment_audit(plan)
                self.writer.execute({
                    "operation": "save_assignment_audit",
                    "action_id": action["action_id"],
                    "assignment_audit": audit,
                })
                if status == "REVIEW" and action.get("created") and plan.get("reason"):
                    self.writer.execute({
                        "operation": "save_action_result",
                        "action_id": action["action_id"],
                        "status": "REVIEW",
                        "last_error": str(plan["reason"])[:200],
                    })
                action["rule_audit"] = audit
                if isinstance(action.get("row"), dict):
                    action["row"]["rule_audit"] = audit
                self.stats["ledger_writes"] += 1
            except (RuntimeError, OSError, ValueError, TypeError):
                action = {
                    **action,
                    "created": False,
                    "reason": "assignment_audit_persistence_failed",
                }
        self._remember_action(event, plan)
        for row in self.existing_actions:
            if (
                row.get("conversation_id") == event["conversation_id"]
                and row.get("message_id") == event["message_id"]
            ):
                if action.get("action_id"):
                    row["action_id"] = action["action_id"]
                action_row = action.get("row") if isinstance(action.get("row"), dict) else {}
                for field in (
                    "external_task_id", "business_key", "duplicate_of_action_id", "parent_action_id",
                    "rule_audit", "last_error",
                ):
                    if field in action_row:
                        row[field] = action_row[field]
                row["status"] = status
        return action

    def _persist_created_task(
        self, ledger_action: dict[str, Any], task_id: str, *, action_id: Any | None = None
    ) -> None:
        """Link the stable remote ID before any comment, attachment, or read-back work."""
        target_action_id = action_id or ledger_action.get("action_id")
        assert self.writer is not None and target_action_id
        result = self.writer.execute(
            {
                "operation": "link_external_task",
                "action_id": target_action_id,
                "status": "EXTERNAL_CREATED",
                "external_task_id": task_id,
            }
        )
        if (not isinstance(result, dict) or result.get("ok") is not True
                or result.get("committed") is not True
                or str(result.get("action_id")) != str(target_action_id)
                or result.get("row", {}).get("external_task_id") != task_id):
            raise RuntimeError("external_task_link_unconfirmed")
        self.stats["ledger_writes"] += 1
        for row in self.existing_actions:
            if row.get("action_id") == target_action_id:
                row["external_task_id"] = task_id
                row["status"] = "EXTERNAL_CREATED"

    def _persist_action_status(
        self,
        action_id: Any,
        status: str,
        last_error: str | None,
        *,
        external_task_id: str | None = None,
        require_confirmed: bool = False,
    ) -> None:
        assert self.writer is not None and action_id
        request = {
            "operation": "save_action_result",
            "action_id": action_id,
            "status": status,
            "last_error": last_error,
        }
        if external_task_id is not None:
            request["external_task_id"] = external_task_id
        result = self.writer.execute(request)
        if require_confirmed and (
            not isinstance(result, dict) or result.get("ok") is not True
            or result.get("committed") is not True
            or str(result.get("action_id")) != str(action_id)
            or result.get("row", {}).get("status") != status
        ):
            raise RuntimeError("action_status_write_unconfirmed")
        self.stats["ledger_writes"] += 1
        for row in self.existing_actions:
            if row.get("action_id") == action_id:
                row["status"] = status
                row["last_error"] = last_error
                if external_task_id is not None:
                    row["external_task_id"] = external_task_id

    def _write_external(
        self,
        event: dict[str, Any],
        plan: dict[str, Any],
        *,
        action_id: Any | None,
        claim_acquired: bool,
        test_only: bool,
        on_created: Callable[[str], None] | None = None,
        on_comment_state: Callable[[str | None], None] | None = None,
    ) -> str:
        # Search results can become stale as soon as an external task is created.
        self._business_search_cache.clear()
        if not test_only and on_created is None:
            raise ValueError("durable_create_link_callback_required")
        if not test_only:
            self._persist_action_status(action_id, "CLAIMED", "create_write_started", require_confirmed=True)
        task_id = create_claimed_task(
            action_id=action_id,
            claim_acquired=claim_acquired,
            create=lambda: self.dws.create_todo(plan, priority=int(plan.get("priority", 20))),
            link=None if test_only else (
                lambda _action_id, created_task_id: on_created(created_task_id)
                if on_created else None
            ),
            test_only=test_only,
        )
        add_verified_source_comment(
            self.dws,
            task_id,
            plan["comment"],
            check_existing=False,
            on_attempt=(
                lambda: on_comment_state("source_comment_write_started")
                if on_comment_state else None
            ),
            on_verified=(lambda: on_comment_state(None) if on_comment_state else None),
        )
        for resource_id in _resource_ids(event):
            resource_root = _private_resource_directory(self.resource_dir)
            source_workspace = hashlib.sha256(
                (str(event["conversation_id"]) + "\0" + str(event["message_id"])).encode("utf-8")
            ).hexdigest()
            source_directory = resource_root / source_workspace
            if source_directory.is_symlink():
                raise DwsError("resource_workspace_symlink_rejected")
            source_directory.mkdir(mode=0o700, parents=True, exist_ok=True)
            source_directory.chmod(0o700)
            downloaded = self.dws.download_message_resource(
                resource_id=resource_id,
                message_id=event["message_id"],
                conversation_id=event["conversation_id"],
                workdir=str(source_directory),
            )
            integrity = _download_integrity(downloaded.get("path"), self.resource_dir)
            ledger_record = {
                "recorded_at": datetime.now(CHINA_TZ).isoformat(),
                "source_conversation_id": event["conversation_id"],
                "source_message_id": event["message_id"],
                "resource_id": resource_id,
                **integrity,
            }
            _append_download_ledger(self.resource_dir, {**ledger_record, "status": "download_verified"})
            attachment_receipt = self.dws.add_attachment(task_id, downloaded["path"])
            self.dws.verify_attachment(task_id, downloaded["file_name"])
            attachment_ids = (
                attachment_receipt.get("attachment_ids")
                if isinstance(attachment_receipt, dict) else None
            )
            _append_download_ledger(self.resource_dir, {
                **ledger_record,
                "status": "attachment_verified",
                "task_id": task_id,
                "attachment_ids": list(attachment_ids or []),
            })
            self.stats["attachments_uploaded"] += 1
        detail = self.dws.get_todo(task_id)
        returned_id = _as_text(detail.get("taskId") or detail.get("todoTaskId")) if isinstance(detail, dict) else ""
        if returned_id != task_id:
            raise DwsError("todo_readback_task_id_mismatch")
        mismatches = verify_todo(detail, plan)
        if mismatches:
            raise DwsError("todo_readback_mismatch:" + ",".join(mismatches))
        self.stats["external_todos_created"] += 1
        return task_id

    def _advance_cursors(self, events: list[dict[str, Any]]) -> None:
        if not self.writer:
            return
        for conversation_id, group_name in self.group_map.items():
            if conversation_id in self.failed_conversations:
                continue
            group_events = [event for event in events if event["conversation_id"] == conversation_id]
            cursor = _max_cursor(group_events)
            if not cursor:
                continue
            self.writer.execute(
                {
                    "operation": "advance_scan_state",
                    "group_name": group_name,
                    "conversation_id": conversation_id,
                    "last_msg_time": cursor[0],
                    "last_msg_id": cursor[1],
                }
            )
            self.stats["ledger_writes"] += 1


def _parse_not_before_argument(value: str) -> datetime:
    try:
        return parse_not_before(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be RFC3339 with an explicit +08:00 offset") from None


def summary_result(result: dict[str, Any]) -> dict[str, Any]:
    """Keep listener subprocess output aggregate-only and free of source content."""
    summary_keys = (
        "status", "reason", "groups_attempted", "groups_complete", "messages",
        "messages_read", "writes", "external_todos_created", "group_messages_sent",
        "initial_cutoff", "parity_observer", "history_replay_complete",
    )
    output = {key: result[key] for key in summary_keys if key in result}
    stats = result.get("stats")
    if isinstance(stats, dict):
        output["stats"] = {
            key: value for key, value in stats.items()
            if isinstance(value, (int, float, bool))
        }
    parity = result.get("parity_observer")
    if isinstance(parity, dict):
        safe_parity = {
            key: value for key, value in parity.items()
            if key in {"enabled", "history_messages_added", "complete_history_scans", "error"}
            and isinstance(value, (int, bool))
        }
        output["parity_observer"] = safe_parity
    return output


def validate_history_options(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    """Apply the same write and resolver gates to one-shot and resident runs."""
    if args.allow_external_writes and args.mode != "production":
        parser.error("external todo writes require --mode production")
    if args.allow_external_writes and not args.write_ledger:
        parser.error("external todo writes require --write-ledger")
    if args.llm_jev and args.jev_v2_policy:
        parser.error("--llm-jev and --jev-v2-policy select different resolvers")
    native_fallbacks = any((args.semantic_fallback, args.title_fields, args.date_role_fallback))
    if native_fallbacks and not args.jev_v2_policy:
        parser.error("native fallback flags require --jev-v2-policy")
    if args.supervised_rollout:
        if args.mode != "production" or not args.jev_v2_policy:
            parser.error("--supervised-rollout requires --mode production and --jev-v2-policy")
        if not native_fallbacks:
            parser.error("--supervised-rollout requires at least one native fallback flag")
    if args.mode == "production" and native_fallbacks and not args.supervised_rollout:
        parser.error("production native fallbacks require --supervised-rollout")


def _parse_todo_lifecycle_argument(value: str) -> dict[str, str]:
    try:
        payload = json.loads(value)
        return validate_queued_todo_event(payload)
    except (json.JSONDecodeError, TypeError, ValueError):
        raise argparse.ArgumentTypeError("invalid TODO lifecycle event reference") from None


def build_history_poller(args: argparse.Namespace) -> OneShotHistoryPoller:
    """Construct the existing history poller and its providers exactly once."""
    writer = None
    if args.write_ledger:
        configure_writer_environment()
        writer = WorkWriter()
    if args.jev_v2_policy:
        resolver = JevV2Resolver(new_policy=True)
    else:
        resolver = JevResolver() if args.llm_jev else None
    semantic_fallback = SemanticFallbackResolver() if args.semantic_fallback else None
    title_resolver = TitleFieldResolver() if args.title_fields else None
    date_role_fallback_resolver = DateRoleFallbackResolver() if args.date_role_fallback else None
    return OneShotHistoryPoller(
        adapter=WorkQueryAdapter(),
        reader=DingTalkReader(),
        dws=DwsClient(
            confirmed_official_writes=(
                args.mode == "production" and args.allow_external_writes and args.write_ledger
            )
        ),
        writer=writer,
        resolver=resolver,
        semantic_fallback=semantic_fallback,
        title_resolver=title_resolver,
        date_role_fallback_resolver=date_role_fallback_resolver,
        supervised_rollout=args.supervised_rollout,
        not_before=args.not_before,
        daily_recovery=bool(getattr(args, "daily_recovery", False)),
        mode=args.mode,
        write_ledger=args.write_ledger,
        allow_external_writes=args.allow_external_writes,
        todo_lifecycle_events=getattr(args, "todo_lifecycle_event", []),
        history_replay_state=(
            _load_pending_history_state(getattr(args, "pending_event_state_dir", None))
        ),
    )


def _load_pending_history_state(state_dir: str | None) -> Any:
    if not state_dir:
        return None
    from listener_engine import AtomicPendingState

    return AtomicPendingState(state_dir)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="One-shot historical DingTalk todo poller")
    parser.add_argument("--once", action="store_true", required=True, help="read configured history once and exit")
    parser.add_argument(
        "--summary-only", action="store_true",
        help="emit aggregate outcome fields without source messages or plans",
    )
    parser.add_argument(
        "--daily-recovery", action="store_true",
        help="allow this one-shot history catchup during the 17:30 recovery minute",
    )
    parser.add_argument("--mode", choices=("shadow", "test", "production"), default="shadow")
    parser.add_argument("--write-ledger", action="store_true", help="persist source/action/cursor state")
    parser.add_argument("--allow-external-writes", action="store_true", help="explicitly allow production todo writes")
    parser.add_argument("--llm-jev", action="store_true", help="use bounded Jev follow-up semantics")
    parser.add_argument("--jev-v2-policy", action="store_true", help="use the JevV2 new-policy resolver")
    parser.add_argument("--semantic-fallback", action="store_true", help="enable bounded native semantic fallback")
    parser.add_argument("--title-fields", action="store_true", help="enable bounded native title-field resolution")
    parser.add_argument("--date-role-fallback", action="store_true", help="enable bounded native date-role resolution")
    parser.add_argument(
        "--supervised-rollout", action="store_true",
        help="separately authorize native semantic/date/title resolvers in production with JevV2 new policy",
    )
    parser.add_argument(
        "--not-before", type=_parse_not_before_argument, metavar="RFC3339+08:00",
        help="collector floor; never read or infer from older source timestamps",
    )
    parser.add_argument("--resource-dir", default="/root/.hermes/cache/scratch/dingtalk_auto_todo_events")
    parser.add_argument(
        "--todo-lifecycle-event", action="append", default=[], type=_parse_todo_lifecycle_argument,
        help="private queued TODO event reference (stable IDs only)",
    )
    parser.add_argument(
        "--pending-event-state-dir",
        help="shared private listener state for event-triggered history replay",
    )
    args = parser.parse_args(argv)
    validate_history_options(args, parser)
    poller = build_history_poller(args)
    try:
        result = poller.run_once()
    except Exception as exc:
        result = {
            "status": "REVIEW",
            "reason": "poll_failed:" + type(exc).__name__,
            "messages": 0,
            "writes": 0,
            "external_todos_created": 0,
            "group_messages_sent": 0,
            "initial_cutoff": args.not_before.isoformat() if args.not_before else None,
        }
    if args.summary_only:
        result = summary_result(result)
    print(json.dumps(result, ensure_ascii=False, default=str), flush=True)
    return 2 if result.get("status") == "REVIEW" else 0


if __name__ == "__main__":
    raise SystemExit(main())
