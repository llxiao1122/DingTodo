import copy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from replay import replay_day, replay_event_worker_day


def test_event_replay_uses_real_resolver_and_causal_simulated_ledger():
    from jev_v2 import JevV2Resolver
    from event_worker import EventProcessor
    from test_jev_v2 import _runtime

    requests = []
    def transport(request, timeout):
        requests.append(request)
        questions = request['questions']
        answers = {'prospective_action': {'type': 'noul', 'noul': .98}}
        answers.update({name: {'type': 'noul', 'noul': .98} for name in questions if name.startswith('same_work_')})
        if 'source_clause' in questions:
            opts = questions['source_clause']['criteria']
            key = next((k for k, v in opts.items() if '库存核对工作' in v), 'none')
            answers['source_clause'] = {'type': 'choice', 'choice': key,
                                        'probabilities': {k: .98 if k == key else .01 for k in opts}, 'confidence': .98}
        if 'due_date' in questions:
            opts = questions['due_date']['criteria']
            key = next((k for k, v in opts.items() if '2026-09-30T18:00:00' in v), 'none')
            answers['due_date'] = {'type': 'choice', 'choice': key,
                                   'probabilities': {k: .98 if k == key else .01 for k in opts}, 'confidence': .98}
        return {'answers': answers}

    config, self_user = _runtime()
    resolver = JevV2Resolver(transport=transport)
    first = {'conversation_id':'c1','message_id':'first','group':'物资调配室','sender_id':'OTHER','sender':'王萌','create_time':'2026-09-30 10:00:00','text':'请于2026-09-30前完成库存核对工作。明天完成。'}
    baseline = EventProcessor(config, self_user, mode='shadow', llm_resolver=resolver,
                              existing_actions=[], business_candidate_query_complete=True)
    assert baseline.process_batch([first])[0]['decision'] == 'CREATE'

    future = {'conversation_id':'c1','message_id':'future','status':'SUCCESS','external_task_id':'ext-f',
              'business_key':'bk:future','route_module':'materials','route_owner':'苗笑天',
              'created_at':'2026-10-01','source_text':'未来初始任务','message_time':'2026-10-01 10:00:00'}
    payload = {'source_day':'2026-09-30','config':config,'self_user':self_user,
               'business_candidate_query_complete':True,'existing_actions':[future],
               'messages':[first,{**first,'message_id':'follow','create_time':'2026-09-30 10:01:00','text':'同一项库存核对工作也请于2026-09-30前完成，明天完成。'}]}
    out = replay_event_worker_day(payload, resolver=resolver)

    assert [p['decision'] for p in out['plans']] == ['CREATE','IGNORE']
    assert out['plans'][1]['reason'] == 'jev_v2_same_work_already_exists'
    assert len(requests) >= 3
    assert 'business_candidates' in requests[-1]['state']
    assert '请于2026-09-30前完成库存核对工作。明天完成。' in requests[-1]['state']
    assert '同一项库存核对工作也请于2026-09-30前完成，明天完成。' in requests[-1]['state']
    assert all('未来初始任务' not in str(request) for request in requests)
    assert any(a.get('message_id') == 'first' and a.get('simulated') for a in out['simulated_ledger'])
    assert all(a.get('message_id') != 'future' for a in out['simulated_ledger'])
    assert out['summary']['external_todos_created'] == 0


def test_event_replay_carries_only_arrived_verified_notice_context_and_preserves_page_ties():
    from jev_v2 import JevV2Resolver

    calls = []
    resolver = JevV2Resolver(
        new_policy=True,
        transport=lambda *args, **kwargs: (calls.append((args, kwargs)) or {}),
    )

    class CountingFallback:
        def __init__(self): self.calls = 0
        def resolve_context(self, *args, **kwargs): self.calls += 1; raise AssertionError("unexpected fallback call")
        def resolve(self, *args, **kwargs): self.calls += 1; raise AssertionError("unexpected title call")
        def resolve_roles(self, *args, **kwargs): self.calls += 1; raise AssertionError("unexpected date-role call")

    fallback = CountingFallback()
    title = CountingFallback()
    date_role = CountingFallback()
    common = {
        "conversation_id": "c",
        "create_time": "2026-10-07 09:02:18",
        "sender": "Self",
        "sender_id": "message-self",
    }
    payload = {
        "source_day": "2026-10-07",
        "source_order_provenance": "complete_dws_chronological_page",
        "config": {"config_group": [{"conversation_id": "c", "group_name": "g", "purpose": "todo_scan"}]},
        "self_user": {"name": "Self", "user_id": "contact-self", "message_sender_id": "message-self"},
        "business_candidate_query_complete": True,
        "existing_actions": [],
        "messages": [
            {**common, "message_id": "z-notice", "text": "@所有人 请大家针对设备点检方案提出意见。"},
            {**common, "message_id": "m-mentions", "text": "@甲 @乙"},
            {
                **common,
                "message_id": "a-file",
                "text": "[文件]设备点检方案.docx",
                "resource_refs": [{"name": "设备点检方案.docx", "resourceId": "local-resource"}],
            },
        ],
    }

    result = replay_event_worker_day(
        payload,
        resolver=resolver,
        semantic_fallback=fallback,
        title_resolver=title,
        date_role_fallback_resolver=date_role,
    )

    assert [plan["message_id"] for plan in result["plans"]] == ["z-notice", "m-mentions", "a-file"]
    assert [plan["decision"] for plan in result["plans"]] == ["IGNORE", "IGNORE", "IGNORE"]
    assert [plan["reason"] for plan in result["plans"]] == [
        "self_authored_notice", "self_authored_pure_person_mention", "self_notice_supporting_artifact"
    ]
    assert calls == []
    assert result["summary"]["llm_calls"] == 0
    assert result["summary"]["semantic_fallback_calls"] == 0
    assert result["summary"]["title_field_calls"] == 0
    assert result["summary"]["date_role_fallback_calls"] == 0
    assert fallback.calls == title.calls == date_role.calls == 0
    assert result["summary"]["arrived_context_messages"] == 3
    assert result["summary"]["arrival_order_provenance"] == "complete_dws_chronological_page"
    assert result["simulated_ledger"] == []
    assert result["summary"]["writes_performed"] == 0


def test_replay_does_not_use_a_same_time_future_notice_to_ignore_an_earlier_file():
    from jev_v2 import JevV2Resolver

    calls = []
    resolver = JevV2Resolver(
        new_policy=True,
        transport=lambda *args, **kwargs: (calls.append((args, kwargs)) or {}),
    )
    common = {
        "conversation_id": "c",
        "create_time": "2026-10-07 09:02:18",
        "sender": "Self",
        "sender_id": "message-self",
    }
    result = replay_event_worker_day({
        "source_day": "2026-10-07",
        "source_order_provenance": "complete_dws_chronological_page",
        "config": {"config_group": [{"conversation_id": "c", "group_name": "g", "purpose": "todo_scan"}]},
        "self_user": {"name": "Self", "user_id": "contact-self", "message_sender_id": "message-self"},
        "business_candidate_query_complete": True,
        "existing_actions": [],
        "messages": [
            {
                **common,
                "message_id": "z-file",
                "text": "[文件]设备点检方案.docx",
                "resource_refs": [{"name": "设备点检方案.docx", "resourceId": "local-resource"}],
            },
            {**common, "message_id": "a-notice", "text": "@所有人 请大家针对设备点检方案提出意见。"},
        ],
    }, resolver=resolver)

    assert [plan["message_id"] for plan in result["plans"]] == ["z-file", "a-notice"]
    assert result["plans"][0]["decision"] == "REVIEW"
    assert result["plans"][0]["reason"] != "self_notice_supporting_artifact"
    assert result["plans"][1]["reason"] == "self_authored_notice"
    assert calls


def test_replay_reports_unverified_tie_order_and_does_not_link_equal_time_artifact():
    from jev_v2 import JevV2Resolver

    resolver = JevV2Resolver(new_policy=True, transport=lambda *_: {})
    common = {
        "conversation_id": "c",
        "create_time": "2026-10-07 09:02:18",
        "sender": "Self",
        "sender_id": "message-self",
    }
    result = replay_event_worker_day({
        "source_day": "2026-10-07",
        "config": {"config_group": [{"conversation_id": "c", "group_name": "g", "purpose": "todo_scan"}]},
        "self_user": {"name": "Self", "user_id": "contact-self", "message_sender_id": "message-self"},
        "business_candidate_query_complete": True,
        "existing_actions": [],
        "messages": [
            {**common, "message_id": "notice", "text": "@所有人 请大家针对设备点检方案提出意见。"},
            {
                **common,
                "message_id": "file",
                "text": "[文件]设备点检方案.docx",
                "resource_refs": [{"name": "设备点检方案.docx", "resourceId": "local-resource"}],
            },
        ],
    }, resolver=resolver)

    assert result["plans"][1]["decision"] == "REVIEW"
    assert result["plans"][1]["reason"] != "self_notice_supporting_artifact"
    assert result["summary"]["arrival_order_provenance"] == "unverified_input_order"


def test_replay_event_worker_retry_requires_new_fingerprint_and_attempt_budget():
    from event_worker import EventProcessor
    from jev_v2 import JevV2Resolver
    processor = EventProcessor({}, {}, llm_resolver=JevV2Resolver(transport=lambda *_: {}), existing_actions=[
        {'conversation_id':'c','message_id':'m','status':'REVIEW','review_attempts':1,'review_evidence_fingerprint':'same'},
        {'conversation_id':'c','message_id':'blocked','status':'REVIEW','external_task_id':'task-1'},
    ])
    assert replay_event_worker_day({'source_day':'2026-09-30','messages':[]}, resolver=processor.llm_resolver)['summary']['messages'] == 0

def test_review_retries_only_on_new_related_evidence_fingerprint():
    from replay import review_retry_allowed
    state = {"status": "REVIEW", "review_evidence_fingerprint": "old", "review_attempts": 1}
    assert not review_retry_allowed(state, "old", max_attempts=3)
    assert review_retry_allowed(state, "new", max_attempts=3)
    assert not review_retry_allowed({**state, "status": "CLAIMED"}, "new", max_attempts=3)
    assert not review_retry_allowed({**state, "status": "REVIEW", "review_attempts": 3}, "new", max_attempts=3)


def test_replay_calls_retry_gate_and_records_bounded_related_evidence_state():
    from replay import replay_day
    review = {"conversation_id": "c", "message_id": "source", "status": "REVIEW", "created_at": "2026-09-29T12:00:00+08:00",
              "review_attempts": 0, "review_evidence_fingerprint": ""}
    messages = [
        {"conversation_id": "c", "message_id": "source", "create_time": "2026-09-30T09:00:00+08:00", "text": "source"},
        {"conversation_id": "c", "message_id": "reply", "reply_to_message_id": "source", "create_time": "2026-09-30T09:01:00+08:00", "text": "detail"},
        {"conversation_id": "c", "message_id": "reply2", "reply_to_message_id": "source", "create_time": "2026-09-30T09:02:00+08:00", "text": "more detail"},
    ]
    calls = []
    result = replay_day({"source_day": "2026-09-30", "messages": messages, "existing_actions": [review]},
                        resolver=lambda msg, actions: (calls.append((msg["message_id"], [a.get("message_id") for a in actions])), {"decision": "REVIEW"})[1])
    # Original source is unchanged evidence; each distinct explicitly linked reply gets one bounded retry.
    assert len(calls) == 3
    assert result["review_retry_state"][0]["review_attempts"] == 2
    assert result["review_retry_state"][0]["review_evidence_fingerprint"]
    assert result["summary"]["writes_performed"] == 0


def test_replay_retry_metadata_invalid_fails_closed_and_linked_action_never_recreates():
    from replay import replay_day
    actions = [
        {"conversation_id": "c", "message_id": "source", "status": "REVIEW", "review_attempts": "bad"},
        {"conversation_id": "c", "message_id": "linked", "status": "EXTERNAL_CREATED", "todo_id": None},
    ]
    calls = []
    result = replay_day({"source_day": "2026-09-30", "messages": [
        {"conversation_id": "c", "message_id": "e", "reply_to_message_id": "source", "create_time": "2026-09-30T10:00:00+08:00", "text": "evidence"}],
        "existing_actions": actions}, resolver=lambda msg, acts: (calls.append(msg["message_id"]), {"decision": "REVIEW"})[1])
    assert calls == ["e"]
    assert result["review_retry_state"] == []
    assert all(p["decision"] != "CREATE" for p in result["plans"])


def test_replay_is_causal_stable_and_does_not_mutate_ledger():
    ledger = [{"source_key": "prior", "status": "REVIEW"}]
    original = copy.deepcopy(ledger)
    messages = [
        {"message_id": "later", "conversation_id": "c", "create_time": "2026-09-30T11:00:00+08:00", "text": "later"},
        {"message_id": "first-b", "conversation_id": "c", "create_time": "2026-09-30T10:00:00+08:00", "text": "b"},
        {"message_id": "first-a", "conversation_id": "c", "create_time": "2026-09-30T10:00:00+08:00", "text": "a"},
        {"message_id": "wrong-day", "conversation_id": "c", "create_time": "2026-10-01T00:00:00+08:00", "text": "future"},
    ]
    seen = []
    def resolver(message, actions):
        seen.append((message["message_id"], [a.get("message_id") for a in actions]))
        return {"decision": "IGNORE", "reason": "synthetic"}
    result = replay_day({"source_day": "2026-09-30", "messages": messages, "existing_actions": ledger}, resolver=resolver)
    assert [x["message_id"] for x in result["plans"]] == ["first-b", "first-a", "later"]
    assert all("later" not in prior for _, prior in seen)
    assert ledger == original
    assert result["summary"]["writes_performed"] == 0
    assert result["summary"]["external_todos_created"] == 0


def test_initial_ledger_requires_known_timestamp_before_source_day_midnight():
    ledger = [
        {"message_id": "prior", "created_at": "2026-09-29T23:59:59+08:00"},
        {"message_id": "midnight", "created_at": "2026-09-30T00:00:00+08:00"},
        {"message_id": "afternoon", "created_at": "2026-09-30T15:00:00+08:00"},
        {"message_id": "unknown"},
        {"message_id": "invalid", "created_at": "not-a-time"},
    ]
    seen = []
    result = replay_day({"source_day": "2026-09-30", "existing_actions": ledger, "messages": [
        {"message_id": "morning", "conversation_id": "c", "create_time": "2026-09-30T09:00:00+08:00"}]},
        resolver=lambda _msg, acts: (seen.extend(a["message_id"] for a in acts), {"decision": "IGNORE"})[1])
    assert seen == ["prior"]
    assert result["summary"]["initial_state_incomplete"] is True


def test_simulated_create_enters_only_subsequent_replay_context():
    contexts = []
    def resolver(message, actions):
        contexts.append([a.get("message_id") for a in actions])
        return {"decision": "CREATE", "source_key": "s:" + message["message_id"]}
    result = replay_day({"source_day": "2026-09-30", "messages": [
        {"message_id": "one", "conversation_id": "c", "create_time": "2026-09-30T10:00:00+08:00"},
        {"message_id": "two", "conversation_id": "c", "create_time": "2026-09-30T10:01:00+08:00"}], "existing_actions": []}, resolver=resolver)
    assert contexts == [[], ["one"]]
    assert all("external_task_id" not in p for p in result["plans"])
    assert result["simulated_ledger"][0]["simulation_id"] == "simulated:one"
    assert all("external_task_id" not in a for a in result["simulated_ledger"])
    assert result["summary"]["external_todos_created"] == 0
