"""Isolated opt-in Jev v2 narrow prospective-action resolver."""
from __future__ import annotations
import json
import math
import os
import urllib.request
import urllib.error
from typing import Any
import re
from llm_adapter import JevResolver
from llm_adapter import LlmError, _has_unredacted_jev_content, _redact_jev_text, _empty_redaction_counts, _message_mention_names

class JevV2Resolver:
    """One bounded Noul judgment; transport injectable and no implicit activation."""
    endpoint = "https://api.typesafe.ai/v1/systemone"
    model = "jev-1.14.0"
    MAX_SOURCE_CHARS = 4000
    def __init__(self, api_key: str | None = None, *, timeout: float = 20, transport=None, new_policy: bool = False):
        self.api_key = (api_key or os.environ.get("TYPESAFE_API_KEY", "")).strip()
        self.new_policy = bool(new_policy)
        self.timeout = min(max(float(timeout), 1.0), 60.0)
        self.transport = transport
        self.last_redaction_counts = _empty_redaction_counts()
    def _post(self, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        if _has_unredacted_jev_content(body.decode("utf-8")):
            raise LlmError("jev_redaction_incomplete")
        if self.transport is not None:
            return self.transport(payload, self.timeout)
        if not self.api_key:
            raise LlmError("jev_api_key_unavailable")
        request = urllib.request.Request(self.endpoint, data=body, headers={"Authorization": "Bearer " + self.api_key, "Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                result = json.loads(response.read(65536).decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise LlmError("jev_transport_failed:" + type(exc).__name__) from None
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise LlmError("jev_invalid_response:" + type(exc).__name__) from None
        if not isinstance(result, dict): raise LlmError("jev_invalid_response:object_required")
        return result
    def resolve(self, message: dict[str, Any], plan: dict[str, Any], *, date_candidates=(), business_candidates=()) -> dict[str, Any]:
        if not isinstance(plan, dict) or plan.get("decision") not in {"CREATE", "REVIEW"}: raise LlmError("jev_v2_ineligible_plan")
        original_text = message.get("text") or ""
        if not isinstance(original_text, str):
            raise LlmError("jev_v2_source_invalid")
        if len(original_text) > self.MAX_SOURCE_CHARS:
            raise LlmError("jev_v2_source_incomplete")
        mention_names = _message_mention_names(message)
        text, counts = _redact_jev_text(original_text, mention_names)
        text, extra_ids = re.subn(r"(?i)(任务\s*ID\s*[:=]\s*)[A-Za-z0-9._-]+", r"\1[TASK_ID]", text)
        counts["embedded_ids"] += extra_ids
        self.last_redaction_counts = {"message": counts, "business_candidates": dict(_empty_redaction_counts()["business_candidates"]), "total": dict(counts)}
        candidates=[]
        for i,row in enumerate(business_candidates):
            if not isinstance(row,dict) or not row.get("text"): raise LlmError("jev_v2_candidate_invalid")
            redacted,row_counts=_redact_jev_text(str(row["text"])[:800]); candidates.append({"key":f"business_{i}","text":redacted})
            for category,count in row_counts.items():
                self.last_redaction_counts["business_candidates"][category] += count
                self.last_redaction_counts["total"][category] += count
        raw_dates = list(date_candidates)
        normalized_dates = []
        date_roles = {}
        date_spans = {}
        date_offsets = {}
        for candidate in raw_dates:
            allowed_shapes = (
                {"key", "due_date", "due_iso"},
                {"key", "due_date", "due_iso", "role"},
                {"key", "due_date", "due_iso", "role", "source_span"},
                {"key", "due_date", "due_iso", "role", "source_span", "source_span_start", "source_span_end"},
            )
            if not isinstance(candidate, dict) or set(candidate) not in allowed_shapes:
                raise LlmError("jev_date_candidate_invalid")
            role = candidate.get("role")
            if role is not None:
                if role not in {"deadline", "effective", "event", "historical", "unknown"}:
                    raise LlmError("jev_date_candidate_invalid")
                date_roles[str(candidate["key"])] = role
            if "source_span" in candidate:
                if not isinstance(candidate["source_span"], str) or not candidate["source_span"].strip() or len(candidate["source_span"]) > 300:
                    raise LlmError("jev_date_candidate_invalid")
                date_spans[str(candidate["key"])] = candidate["source_span"]
            if "source_span_start" in candidate or "source_span_end" in candidate:
                start, end = candidate.get("source_span_start"), candidate.get("source_span_end")
                if (
                    isinstance(start, bool) or not isinstance(start, int) or start < 0
                    or isinstance(end, bool) or not isinstance(end, int) or end <= start
                    or "source_span" not in candidate
                ):
                    raise LlmError("jev_date_candidate_invalid")
                date_offsets[str(candidate["key"])] = (start, end)
            normalized_dates.append({key: candidate[key] for key in ("key", "due_date", "due_iso")})
        dates = JevResolver._date_candidates(normalized_dates)
        role_descriptions = {
            "deadline": "程序识别为原文明确的任务截止日期",
            "effective": "程序识别为生效或状态变更时间，不是任务截止日期",
            "event": "程序识别为另行安排的检查、核查、审计、会议等事件时间，不是当前任务截止日期",
            "historical": "程序识别为联单或处置历史记录日期，不是新任务截止日期",
            "unknown": "程序无法确认日期用途，不能据此设任务截止日期",
        }
        date_options = {
            item["key"]: (
                f"{role_descriptions[date_roles[item['key']]]}：{item['due_iso']}"
                if item["key"] in date_roles
                else f"程序候选日期 {item['due_iso']}"
            )
            for item in dates
        }
        date_options.update({"none": "原文/程序没有可用截止日期，或明确不设截止日期", "uncertain": "无法可靠选择截止日期"})
        clauses = [part.strip() for part in re.split(r"[；;\n]+", original_text) if part.strip()]
        if not clauses and original_text: clauses = [original_text]
        clause_map = {f"clause_{i}": part for i, part in enumerate(clauses[:12])}
        # If splitting exceeds the bounded candidate set, never hide unrepresented source text.
        clause_overflow = len(clauses) > 12
        clause_options = {}
        for key, value in clause_map.items():
            redacted_clause, clause_counts = _redact_jev_text(value, mention_names)
            redacted_clause, clause_ids = re.subn(r"(?i)(任务\s*ID\s*[:=]\s*)[A-Za-z0-9._-]+", r"\1[TASK_ID]", redacted_clause)
            counts["embedded_ids"] += clause_ids
            for category, count in clause_counts.items():
                if category in counts:
                    counts[category] += count
            clause_options[key] = redacted_clause
        self.last_redaction_counts["message"] = dict(counts)
        self.last_redaction_counts["total"] = dict(counts)
        for category, count in self.last_redaction_counts["business_candidates"].items():
            self.last_redaction_counts["total"][category] += count
        clause_options.update({"none":"未选择任何条款", "uncertain":"无法可靠选择条款"})
        state={"text":text,"business_candidates":candidates,"date_candidates":dates}
        date_role_candidates = []
        if self.new_policy:
            for key, role in date_roles.items():
                if role != "unknown" or key not in date_spans:
                    continue
                local_span, span_counts = _redact_jev_text(date_spans[key], mention_names)
                local_span, span_ids = re.subn(r"(?i)(任务\s*ID\s*[:=]\s*)[A-Za-z0-9._-]+", r"\1[TASK_ID]", local_span)
                span_counts["embedded_ids"] += span_ids
                for category, count in span_counts.items():
                    counts[category] += count
                # A candidate may have been parsed from a part of the body that
                # Jev does not receive. Keep that role unknown instead of asking
                # for a judgment without its source evidence.
                span_offsets = date_offsets.get(key)
                if span_offsets is not None:
                    source_visible = span_offsets[1] <= len(original_text)
                else:
                    source_visible = local_span in text
                if not source_visible or local_span not in text:
                    continue
                date_role_candidates.append({"key": key, "source_span": local_span})
        if date_role_candidates:
            state["date_role_candidates"] = date_role_candidates
            self.last_redaction_counts["message"] = dict(counts)
            self.last_redaction_counts["total"] = dict(counts)
            for category, count in self.last_redaction_counts["business_candidates"].items():
                self.last_redaction_counts["total"][category] += count
        policy_context=message.get("_jev_v2_policy_context")
        if self.new_policy and isinstance(policy_context,dict):
            allowed_context={"workgroup_role_mentioned","all_employee_mentioned","self_directly_mentioned","other_person_mentioned","broad_workgroup_mentioned"}
            if set(policy_context)-allowed_context or any(not isinstance(value,bool) for value in policy_context.values()): raise LlmError("jev_v2_policy_context_invalid")
            state["policy_context"]={key:bool(policy_context.get(key,False)) for key in sorted(allowed_context)}
        questions={}
        if self.new_policy:
            questions.update({"applies_to_workgroup":{"type":"noul","instructions":"Does the substantive work in state.text apply to the managed 铁炉西工班 or 李林骁? Read state.policy_context: broad_workgroup_mentioned and all_employee_mentioned are positive applicability evidence; workgroup_role_mentioned addresses 工班长; self_directly_mentioned supports self applicability. A named-other-only task does not apply personally. Apply these rules: broad workgroup/all-employee work may apply without personal mention; department-clerk or department-only work does not; explicit named assignment to another person does not create a task for 李林骁; self-authored dispatch is not re-created. Do not infer applicability from room alone. A directly named workgroup or self/role target is in scope unless source wording clearly excludes them. Answer near 0.5 only for consequential unresolved scope/context.","criteria":{"true":"适用于铁炉西工班/李林骁，包括工班范围通知和工班长角色指派","false":"仅科室/内勤事项、明确指定其他人员，或无关事项"}},
              "needs_follow_up":{"type":"noul","instructions":"Does the applicable matter warrant implementation/follow-up by the workgroup or 李林骁? Include concrete tasks, safety reminders, policy/frequency changes, termination of duty arrangements, relevant hazardous-waste lists/procedures, requests to report/check whether work is complete, and operational notices requiring subsequent action. A notice/知悉/提醒 or question asking for status can be a follow-up; do not require imperative phrasing. No explicit deadline is required. Exclude acknowledgements/completion reports from the sender and purely informational content with no implementation need. A clear multi-clause matter remains actionable: preserve all source wording; independent tasks can be split by deterministic code, otherwise review specifically.","criteria":{"true":"有落实、核对、传达、调整或后续提醒需要","false":"无需后续落实，纯信息/回执/完成反馈"}}})
        questions.update({"prospective_action":{"type":"noul","instructions":"Does this message clearly request or describe a specific task still to be done? Judge only source semantics; do not infer routing, ownership, dates, or authorize writes.","criteria":{"true":"明确存在仍需执行的具体工作","false":"没有未来待执行工作"}}})
        if not self.new_policy:
            questions["source_clause"]={"type":"choice","instructions":"从原文条款中选择完整相关条款；保留期限和约束。多项独立行动不可只选一项，应选择 uncertain。只选择候选，不生成文本。","criteria":clause_options}
        if dates: questions["due_date"]={"type":"choice","instructions":"仅选择程序提供的 date_* 候选或 none/uncertain；不得生成或改写日期。只有原文明确把日期作为完成、反馈或提交期限时才可选择该候选；生效、开始、终止、停止、暂停、启用、恢复、另行安排的检查/审计/会议等事件时间和明确标注的联单/处置历史日期不是新任务期限，候选用途不清时选 uncertain。程序提供的候选角色标签用于区分期限、状态变更、安排事件、历史记录和用途不明日期；若角色不确定，不得选它作为任务期限。","criteria":date_options}
        if date_role_candidates:
            role_options = {
                "deadline": "原文明示该候选日期是完成、反馈、提交或报送当前待办的期限",
                "effective": "候选日期表示开始、生效、终止、暂停、恢复等状态或安排变更时间，不是当前待办期限",
                "event": "候选日期表示另行安排的检查、核查、审计、会议等事件时间，不是当前待办完成期限",
                "historical": "候选日期表示已经发生的历史事件、处置过程、交易记录或过去的时间区间，不是当前待办期限",
                "unknown": "结合完整原文仍无法确定候选日期的语义用途",
            }
            for candidate in date_role_candidates:
                key = candidate["key"]
                questions[f"date_role_{key}"] = {
                    "type": "choice",
                    "instructions": (
                        f"只判断候选 {key} 的语义时间角色。根据 state.date_role_candidates 中相同 key 的本地日期原文片段定位，"
                        "并结合完整 state.text 判断。不要把后续录入、核对或反馈任务误当成日期本身的角色。"
                        "独立安排的检查、核查、审计、会议等发生时间属于 event，不是当前待办的完成期限。"
                        "不要生成或改写日历日期、责任人、动作、任务或任务标识。若语义不清，选择 unknown。"
                    ),
                    "criteria": role_options,
                }
        for c in candidates: questions["same_work_"+c["key"]]={"type":"noul","instructions":f"Is state.text about the same task as state.business_candidates[{int(c['key'].split('_')[1])}].text? Judge only task identity.","criteria":{"true":"same work item","false":"not the same work item"}}
        payload={"model":self.model,"state":json.dumps(state,ensure_ascii=False,separators=(",",":")),"questions":questions}
        if _has_unredacted_jev_content(payload["state"]): raise LlmError("jev_redaction_incomplete")
        response=self._post(payload); answers=response.get("answers") if isinstance(response,dict) else None
        if not isinstance(answers,dict) or set(answers)!=set(questions): raise LlmError("jev_v2_answer_schema_invalid")
        output={}
        for key,answer in answers.items():
            if key in {"due_date","source_clause"} or key.startswith("date_role_"):
                if not isinstance(answer,dict) or answer.get("type")!="choice" or set(answer)!={"type","choice","probabilities","confidence"}: raise LlmError("jev_v2_answer_schema_invalid")
                continue
            if not isinstance(answer,dict) or answer.get("type")!="noul" or set(answer)!={"type","noul"}: raise LlmError("jev_v2_answer_schema_invalid")
            value=answer["noul"]
            if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(float(value)) or not 0<=float(value)<=1: raise LlmError("jev_v2_probability_invalid")
            output[key]=float(value)
        if "source_clause" in answers:
            choice, confidence, margin = JevResolver._choice(answers["source_clause"], clause_options, "source_clause")
            output.update(source_clause_choice=choice, source_clause_confidence=confidence, source_clause_margin=margin)
            output["source_clause_overflow"] = clause_overflow
            if choice in clause_map: output["source_clause"] = clause_map[choice]
        elif self.new_policy:
            output.update(source_clause_choice="none", source_clause_confidence=1.0, source_clause_margin=1.0, source_clause_overflow=clause_overflow)
        if "due_date" in answers:
            choice, confidence, margin = JevResolver._choice(answers["due_date"], date_options, "due_date")
            output["due_date_choice"] = choice
            output["due_date_confidence"] = confidence
            output["due_date_margin"] = margin
        if date_role_candidates:
            role_options = questions[next(key for key in questions if key.startswith("date_role_"))]["criteria"]
            role_judgments = []
            for candidate in date_role_candidates:
                key = candidate["key"]
                question_key = f"date_role_{key}"
                choice, confidence, margin = JevResolver._choice(
                    answers[question_key], role_options, question_key
                )
                role_judgments.append({
                    "candidate_key": key,
                    "parser_role": "unknown",
                    "model_choice": choice,
                    "resolved_role": "unknown" if choice == "uncertain" else choice,
                    "role_origin": "parser" if choice == "uncertain" else "jev_v2",
                    "confidence": confidence,
                    "margin": margin,
                })
            output["date_role_judgments"] = role_judgments
        return output
