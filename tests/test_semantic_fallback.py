import json
import sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from llm_adapter import SemanticFallbackResolver

class Agent:
    def __init__(self, response): self.response=response; self.calls=[]
    def run_conversation(self, user_message, **kwargs):
        self.calls.append((user_message,kwargs))
        return {"completed":True,"final_response":json.dumps(self.response)}

def test_constrained_semantics_and_source_evidence_checked():
    text="请各工班明日前完成库存核对。"
    answer={"applicability":"applicable","followup":"required","title_span":None,"due_key":"unknown","same_work":{},"evidence_spans":["各工班"]}
    agent=Agent(answer); resolver=SemanticFallbackResolver(agent_factory=lambda **kw:agent)
    result=resolver.resolve_context({"text":text,"create_time":"2026-10-06"},{"decision":"REVIEW","reason":"jev_v2_action_uncertain"},date_candidates=[])
    assert result["applicability"]=="applicable"
    assert len(agent.calls)==1

def test_rejects_extra_owner_and_hallucinated_evidence():
    for extra in ({"owner":"bad"},{"evidence_spans":["not in source"]}):
        answer={"applicability":"unknown","followup":"unknown","title_span":None,"due_key":"unknown","same_work":{},"evidence_spans":[],**extra}
        resolver=SemanticFallbackResolver(agent_factory=lambda **kw:Agent(answer))
        with pytest.raises(ValueError): resolver.resolve_context({"text":"原文","create_time":"t"},{"decision":"REVIEW","reason":"jev_v2_action_uncertain"},date_candidates=[])

def test_contract_has_general_responsibility_policy_not_explicit_scope_gate():
    answer={"applicability":"unknown","followup":"unknown","title_span":None,"due_key":"unknown","same_work":{},"evidence_spans":["原文"]}
    agent=Agent(answer); resolver=SemanticFallbackResolver(agent_factory=lambda **kw:agent)
    resolver.resolve_context({"text":"原文","create_time":"t"},{"decision":"REVIEW","reason":"jev_v2_action_uncertain"})
    system=agent.calls[0][1]["system_message"]
    assert "落实、跟进或调整安排" in system
    assert "No explicit @mention" in system
    assert "program determines default owner" in system
    assert "explicit workgroup scope" not in system
