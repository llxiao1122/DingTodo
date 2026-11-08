from __future__ import annotations

from threading import Barrier

from shadow_judgment import evaluate_limited_judgments_in_shadow


class _MainResolver:
    def __init__(self, barrier: Barrier, observed: list[dict]):
        self.barrier = barrier
        self.observed = observed

    def resolve(self, message, plan, *, date_candidates, business_candidates):
        self.barrier.wait(timeout=2)
        self.observed.append(("jev", message["text"], plan["decision"], date_candidates, business_candidates))
        message["text"] = "mutated by main"
        return {"prospective_action": 0.8, "needs_follow_up": 0.9}


class _LimitedResolver:
    def __init__(self, barrier: Barrier, observed: list[dict]):
        self.barrier = barrier
        self.observed = observed

    def resolve_context(self, message, plan, *, date_candidates, business_candidates):
        self.barrier.wait(timeout=2)
        self.observed.append(("gpt", message["text"], plan["decision"], date_candidates, business_candidates))
        return {
            "applicability": "applicable",
            "followup": "required",
            "title_span": "title_0",
            "due_key": "date_0",
            "same_work": {"business_0": "different"},
            "evidence_spans": ["请核对"],
        }


def test_shadow_evaluates_both_resolvers_concurrently_on_independent_same_inputs():
    barrier = Barrier(2)
    observed = []
    result = evaluate_limited_judgments_in_shadow(
        {"text": "合成群消息，请核对", "nested": {"keep": True}},
        {"decision": "REVIEW"},
        main_resolver=_MainResolver(barrier, observed),
        limited_resolver=_LimitedResolver(barrier, observed),
        date_candidates=[{"key": "date_0", "due_date": "2026-11-08"}],
        business_candidates=[{"key": "business_0", "text": "合成旧任务"}],
    )

    assert result["status"] == "complete"
    assert result["jev_main"] == {
        "status": "ok",
        "judgment": {"prospective_action": 0.8, "needs_follow_up": 0.9},
    }
    assert result["gpt_limited"]["status"] == "ok"
    assert len(observed) == 2
    assert all(row[1] == "合成群消息，请核对" for row in observed)
    assert all(row[2] == "REVIEW" for row in observed)
    assert all(row[3] == [{"key": "date_0", "due_date": "2026-11-08"}] for row in observed)
    assert all(row[4] == [{"key": "business_0", "text": "合成旧任务"}] for row in observed)


def test_shadow_rejects_action_authority_fields_and_keeps_other_judgment():
    class UnauthorizedMain:
        def resolve(self, *_args, **_kwargs):
            return {"prospective_action": 0.9, "decision": "CREATE"}

    class Limited:
        def resolve_context(self, *_args, **_kwargs):
            return {
                "applicability": "unknown",
                "followup": "unknown",
                "title_span": None,
                "due_key": "none",
                "same_work": {},
                "evidence_spans": ["合成"],
            }

    result = evaluate_limited_judgments_in_shadow(
        {"text": "合成"}, {"decision": "REVIEW"},
        main_resolver=UnauthorizedMain(), limited_resolver=Limited(),
    )

    assert result["status"] == "partial"
    assert result["jev_main"] == {"status": "invalid", "reason": "judgment_schema_invalid"}
    assert result["gpt_limited"]["status"] == "ok"


def test_shadow_rejects_action_decision_from_limited_resolver():
    class Main:
        def resolve(self, *_args, **_kwargs):
            return {"prospective_action": 0.2}

    class UnauthorizedLimited:
        def resolve_context(self, *_args, **_kwargs):
            return {
                "applicability": "applicable",
                "followup": "required",
                "title_span": None,
                "due_key": "none",
                "same_work": {},
                "evidence_spans": ["合成"],
                "decision": "CREATE",
            }

    result = evaluate_limited_judgments_in_shadow(
        {"text": "合成"}, {"decision": "REVIEW"},
        main_resolver=Main(), limited_resolver=UnauthorizedLimited(),
    )

    assert result["status"] == "partial"
    assert result["jev_main"]["status"] == "ok"
    assert result["gpt_limited"] == {"status": "invalid", "reason": "judgment_schema_invalid"}
