"""Real EventProcessor + sequential replay tests for semantic fallback causality."""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from event_worker import EventProcessor
from jev_v2 import JevV2Resolver
from replay import replay_event_worker_day
from test_jev_v2 import _runtime


class Fallback:
    def __init__(self, *, applicability="applicable", followup="required", due_key="none",
                 same_work=None, title_span="title_0", evidence="请各工班完成库存核对"):
        self.response = {"applicability": applicability, "followup": followup,
                         "title_span": title_span, "due_key": due_key,
                         "same_work": same_work or {}, "evidence_spans": [evidence]}
        self.calls = []
        self.last_title_candidates = []

    def resolve_context(self, event, plan, *, date_candidates=(), business_candidates=()):
        self.calls.append((event, plan, list(date_candidates), list(business_candidates)))
        self.last_title_candidates = [{"key": "title_0", "text": plan.get("title") or event["text"]}]
        return self.response


def _jev_uncertain(request, timeout):
    questions = request["questions"]
    answers = {"applies_to_workgroup": {"type": "noul", "noul": .5},
               "needs_follow_up": {"type": "noul", "noul": .5},
               "prospective_action": {"type": "noul", "noul": .99}}
    for name in questions:
        if name.startswith("same_work_"):
            answers[name] = {"type": "noul", "noul": .5}
    for name, question in questions.items():
        if question["type"] == "choice":
            opts = question["criteria"]
            if name.startswith("date_role_"):
                state = json.loads(request["state"])
                candidate_key = name.removeprefix("date_role_")
                next(row for row in state["date_role_candidates"] if row["key"] == candidate_key)
                choice = "unknown"
            else:
                choice = "uncertain" if "uncertain" in opts else "none"
            answers[name] = {"type": "choice", "choice": choice,
                "probabilities": {key: .9 if key == choice else .1 / (len(opts) - 1) for key in opts}, "confidence": .9}
    return {"answers": answers}


def _payload(messages, resolver, fallback, config=None):
    cfg, self_user = _runtime()
    return replay_event_worker_day({"source_day": "2026-09-30", "config": config or cfg,
        "self_user": self_user, "business_candidate_query_complete": True,
        "existing_actions": [], "messages": messages}, resolver=resolver, semantic_fallback=fallback)


def _msg(mid, text, minute=0):
    return {"conversation_id": "c1", "message_id": mid, "group": "物资调配室",
        "sender_id": "OTHER", "sender": "王萌", "create_time": f"2026-09-30 10:{minute:02d}:00", "text": text}


def test_same_text_with_different_message_ids_uses_semantic_duplicate_gate():
    jev_calls = []
    def transport(request, timeout):
        jev_calls.append(request)
        return _jev_uncertain(request, timeout)
    resolver = JevV2Resolver(new_policy=True, transport=transport)
    fallback = Fallback(evidence="请各工班完成库存核对")
    original_resolve = fallback.resolve_context

    def resolve_same_work(event, plan, *, date_candidates=(), business_candidates=()):
        response = original_resolve(
            event, plan, date_candidates=date_candidates, business_candidates=business_candidates
        )
        if business_candidates:
            return {
                **response,
                "same_work": {row["key"]: "same" for row in business_candidates},
            }
        return response

    fallback.resolve_context = resolve_same_work
    first = _msg("first", "请各工班完成库存核对工作。")
    second = _msg("second", "请各工班完成库存核对工作。", 1)
    result = _payload([first, second], resolver, fallback)

    assert result["plans"][0]["decision"] == "CREATE"
    assert result["plans"][1]["decision"] == "REVIEW"
    assert result["plans"][1]["reason"] == "jev_v2_scope_or_followup_uncertain"
    assert len(jev_calls) == 2
    assert len(fallback.calls) == 2
    assert any(a.get("message_id") == "first" and a.get("simulated") for a in result["simulated_ledger"])
    assert result["summary"]["writes_performed"] == result["summary"]["external_todos_created"] == 0
    assert result["plans"][1]["fallback_semantic_evidence"]["same_work"] == {"business_0": "same"}


def test_semantically_same_different_text_receives_simulated_candidate_and_suppresses_duplicate():
    resolver = JevV2Resolver(new_policy=True, transport=_jev_uncertain)
    def semantic_fallback(event, plan, *, date_candidates=(), business_candidates=()):
        fallback.calls.append((event, plan, list(date_candidates), list(business_candidates)))
        if event["message_id"] == "first":
            assert not business_candidates
            fallback.response["same_work"] = {}
            fallback.response["evidence_spans"] = [first["text"]]
        else:
            assert business_candidates
            candidate_key = business_candidates[0]["key"]
            fallback.response["same_work"] = {candidate_key: "same"}
            fallback.response["evidence_spans"] = [second["text"]]
        fallback.last_title_candidates = [{"key": "title_0", "text": plan.get("title") or event["text"]}]
        return fallback.response
    fallback = Fallback(evidence="请各工班完成库存核对")
    fallback.resolve_context = semantic_fallback
    first = _msg("first", "请各工班完成库存核对工作。")
    second = _msg("second", "库存核对这项工作也请各工班完成。", 1)
    result = _payload([first, second], resolver, fallback)

    assert result["plans"][0]["decision"] == "CREATE"
    assert fallback.calls[0][3] == []
    assert result["plans"][1]["decision"] == "REVIEW"
    assert result["plans"][1]["reason"] == "jev_v2_scope_or_followup_uncertain"
    assert result["plans"][1]["fallback_semantic_evidence"]["same_work"] == {fallback.calls[1][3][0]["key"]: "same"}
    assert len(fallback.calls) == 2
    assert fallback.calls[1][3] and fallback.calls[1][3][0].get("text") == first["text"]
    assert fallback.calls[1][3][0].get("key")
    assert result["simulated_ledger"] and result["simulated_ledger"][0].get("message_id") == "first"
    assert result["summary"]["writes_performed"] == result["summary"]["external_todos_created"] == 0


def test_simulated_business_candidates_require_explicit_replay_opt_in():
    from event_worker import recall_business_candidates
    plan = {"route_module": "materials", "route_owner": "苗笑天", "business_key": "new"}
    simulated = {"business_key": "old", "route_module": "materials", "route_owner": "苗笑天",
        "status": "SUCCESS", "simulated": True, "simulation_id": "sim-1",
        "source_text": "核查库存", "message_time": "2026-09-30 09:00:00"}
    assert recall_business_candidates(plan, [simulated])[0] == []
    assert recall_business_candidates(plan, [{**simulated, "simulated": False}], allow_simulated=True)[0] == []
    assert recall_business_candidates(plan, [simulated], allow_simulated=True)[0]
    linked = {**simulated, "simulated": False, "external_task_id": "ext-1"}
    assert recall_business_candidates(plan, [linked])[0]


def test_replay_nontrigger_does_not_call_fallback():
    def not_applicable(request, timeout):
        result = _jev_uncertain(request, timeout)
        result["answers"]["applies_to_workgroup"]["noul"] = .1
        result["answers"]["needs_follow_up"]["noul"] = .1
        return result
    resolver = JevV2Resolver(new_policy=True, transport=not_applicable)
    fallback = Fallback()
    result = _payload([_msg("notice", "通知：会议安排已调整。")], resolver, fallback)
    assert result["summary"]["semantic_fallback_calls"] == 0
    assert fallback.calls == []


def test_replay_uncertain_due_with_explicit_deadline_stays_review():
    resolver = JevV2Resolver(new_policy=True, transport=_jev_uncertain)
    fallback = Fallback(due_key="unknown", evidence="请各工班在10月2日前完成库存核对")
    result = _payload([_msg("due", "请各工班在10月2日前完成库存核对工作。")], resolver, fallback)
    assert len(fallback.calls) == 1
    assert result["plans"][0]["decision"] == "REVIEW"
    assert result["plans"][0]["due_date"] == "2026-10-02"
    assert result["plans"][0]["due_iso"] == "2026-10-02T18:00:00+08:00"
    assert result["plans"][0]["due_role_unresolved"] is False


def test_replay_referenced_missing_context_does_not_escalate_fallback():
    resolver = JevV2Resolver(new_policy=True, transport=_jev_uncertain)
    fallback = Fallback(evidence="请各工班完成库存核对")
    result = _payload([_msg("missing", "请各工班查阅，具体要求见R列并完成库存核对工作。")], resolver, fallback)
    assert result["plans"][0]["reason"] == "jev_v2_referenced_context_missing"
    assert result["summary"]["semantic_fallback_calls"] == 0
    assert fallback.calls == []


def test_complete_workgroup_text_semantic_uncertainty_can_create_without_invented_due_date():
    resolver = JevV2Resolver(new_policy=True, transport=_jev_uncertain)
    fallback = Fallback(evidence="请各工班落实终止值守安排", due_key="none", title_span="title_0")
    result = _payload([_msg("termination", "请各工班落实终止值守安排。")], resolver, fallback)
    assert len(fallback.calls) == 1
    assert result["plans"][0]["decision"] == "CREATE"
    assert result["plans"][0]["title"] == "请各工班落实终止值守安排"
    assert result["plans"][0].get("due_date") is None
    assert result["plans"][0]["route_owner"] == "李林骁"


def test_scope_context_unverified_complete_notice_reaches_fallback():
    text = "郑州市防汛抗旱指挥部办公室定于9月30日18时终止汛期防汛值班。请知悉。"
    resolver = JevV2Resolver(new_policy=True, transport=_jev_uncertain)
    fallback = Fallback(evidence=text)
    result = _payload([_msg("full-notice", text)], resolver, fallback)
    assert len(fallback.calls) == 1
    assert result["plans"][0]["decision"] == "CREATE"
    assert result["plans"][0]["due_date"] is None


def test_attachment_and_missing_context_scope_warnings_do_not_escalate():
    resolver = JevV2Resolver(new_policy=True, transport=_jev_uncertain)
    fallback = Fallback()
    for i, text in enumerate(("[文件] 关于终止汛期防汛值班工作的通知.pdf fileId: TESTFILE",
                              "[图片消息](mediaId=@TESTMEDIA)",
                              "请查阅，具体要求见R列并完成工作。")):
        result = _payload([_msg(f"blocked-{i}", text)], resolver, fallback)
        assert result["plans"][0]["decision"] == "REVIEW"
    assert fallback.calls == []


def test_attachment_only_semantic_uncertainty_does_not_escalate_fallback():
    resolver = JevV2Resolver(new_policy=True, transport=_jev_uncertain)
    fallback = Fallback()
    result = _payload([_msg("attachment", "[附件]")], resolver, fallback)
    assert result["plans"][0]["decision"] == "REVIEW"
    assert fallback.calls == []


def test_fallback_invalid_owner_or_fabricated_evidence_is_rejected_by_processor():
    for extra in ({"owner": "伪造责任人"},):
        # A fake returning outside the constrained resolver's contract must fail closed.
        resolver = JevV2Resolver(new_policy=True, transport=_jev_uncertain)
        fallback = Fallback(evidence="请各工班完成库存核对")
        fallback.resolve_context = lambda *a, **k: {**fallback.response, **extra}
        result = _payload([_msg("bad-owner", "请各工班完成库存核对工作。")], resolver, fallback)
        assert result["plans"][0]["decision"] == "REVIEW"
    resolver = JevV2Resolver(new_policy=True, transport=_jev_uncertain)
    fallback = Fallback(evidence="不在原文中的证据")
    # Use the real SemanticFallbackResolver validator around a deterministic fake agent.
    from llm_adapter import SemanticFallbackResolver
    class Agent:
        def run_conversation(self, user_message, **kwargs):
            data = json.loads(user_message)
            return {"completed": True, "final_response": json.dumps({**fallback.response,
                "evidence_spans": [fallback.response["evidence_spans"][0]]})}
    guarded = SemanticFallbackResolver(agent_factory=lambda **kw: Agent())
    result = _payload([_msg("bad-evidence", "请各工班完成库存核对工作。")], resolver, guarded)
    assert result["plans"][0]["decision"] == "REVIEW"
    assert result["summary"]["semantic_fallback_failed"] == 1


def test_fallback_cannot_select_effective_date_as_due_date():
    text = (
        "郑州市防汛抗旱指挥部办公室定于10月7日18时终止汛期防汛值班。"
        "请各工班按新安排完成值守衔接和人员确认。"
    )
    resolver = JevV2Resolver(new_policy=True, transport=_jev_uncertain)
    fallback = Fallback(due_key="date_0", evidence=text)
    result = _payload([_msg("effective-date", text)], resolver, fallback)
    plan = result["plans"][0]
    assert fallback.calls[0][2][0]["role"] == "effective"
    assert plan["decision"] == "CREATE"
    assert plan.get("due_date") is None
    assert plan.get("due_iso") is None
