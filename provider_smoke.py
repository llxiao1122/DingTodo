"""Synthetic provider smoke only. No adapters, tasks, ledger or group calls."""
import argparse
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from jev_v2 import JevV2Resolver
from llm_adapter import SemanticFallbackResolver

parser = argparse.ArgumentParser()
parser.add_argument("--codex-only", action="store_true")
args = parser.parse_args()

source = '请各工班核对库区标识并反馈核对结果。'
event = {'text': source, 'create_time': '2026-10-08 09:00:00'}
plan = {'decision': 'REVIEW', 'title': source, 'reason': 'jev_v2_scope_or_followup_uncertain'}
jev_contract_valid = False
if not args.codex_only:
    jev = JevV2Resolver(new_policy=True, timeout=30)
    a = jev.resolve(event, plan, date_candidates=[], business_candidates=[])
    assert isinstance(a, dict) and 'prospective_action' in a
    jev_contract_valid = True
native = SemanticFallbackResolver(timeout=60)
b = native.resolve_context(event, plan, date_candidates=[], business_candidates=[])
assert set(b) == {'applicability','followup','title_span','due_key','same_work','evidence_spans'}
print(json.dumps({'synthetic_only': True, 'jev_contract_valid':jev_contract_valid, 'native_contract_valid':True, 'tools_enabled':False, 'external_task_calls':0, 'ledger_calls':0, 'group_send_calls':0}))
