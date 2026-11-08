import json
import math
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from jev_v2 import JevV2Resolver
from event_worker import EventProcessor
from llm_adapter import LlmError


def runtime():
    return ({"config_group": [{"group_name": "物资调配室", "conversation_id": "c1", "purpose": "todo_scan"}],
             "config_staff": [[1, "李林骁", "SELF", "leader"], [2, "苗笑天", "MIAO", "executor"]],
             "active_routing_loaded": True, "responsibility_rules": {"materials": "苗笑天"}, "config_area_duty": [{"workgroup":"铁炉西工班","module":"materials","owner":"苗笑天","start_date":"2026-01-01","end_date":"2026-12-31"}]},
            {"name": "李林骁", "user_id": "SELF"})


def event(text="请完成库存核对工作。", **extra):
    return {"conversation_id": "c1", "message_id": "m1", "group": "物资调配室", "sender": "王萌",
            "sender_id": "OTHER", "create_time": "2026-09-21 10:00:00", "text": text, **extra}


def processor(answers, *, text=None, candidates=(), complete=True, sender="OTHER"):
    config, self_user = runtime()
    def transport(payload, timeout):
        response = dict(answers)
        questions = payload["questions"]
        if "source_clause" in questions and "source_clause" not in response:
            options = questions["source_clause"]["criteria"]
            selected = "none"
            response["source_clause"] = {"type":"choice", "choice":selected,
                "probabilities":{key:(.98 if key==selected else .005) for key in options}, "confidence":.98}
        if "due_date" in questions and "due_date" not in response:
            options = questions["due_date"]["criteria"]
            keys = [key for key in options if key.startswith("date_")]
            selected = keys[0] if keys else "none"
            response["due_date"] = {"type":"choice", "choice":selected,
                "probabilities":{key:(.97 if key==selected else (.01 if key.startswith("date_") else .005)) for key in options}, "confidence":.97}
        return {"answers": response}
    proc = EventProcessor(config, self_user, mode="test", llm_resolver=JevV2Resolver(
        transport=transport), existing_actions=list(candidates),
        business_candidate_query_complete=complete)
    result = proc.process_batch([event(text=text or "请完成库存核对工作。", sender_id=sender)])[0]
    return proc, result


class JevV2ContractTests(unittest.TestCase):
    def test_official_noul_contract_and_live_transport_path_shape(self):
        captured = {}
        resolver = JevV2Resolver(api_key="nonempty-local-test-key", transport=lambda p, t: (captured.update(payload=p) or {"answers": {
            "prospective_action": {"type": "noul", "noul": .91},
            "source_clause": {"type":"choice", "choice":"none", "probabilities":{k:(.98 if k=="none" else .005) for k in p["questions"]["source_clause"]["criteria"]}, "confidence":.98}}}))
        self.assertEqual(resolver.resolve(event(), {"decision": "REVIEW"})["prospective_action"], .91)
        q = captured["payload"]["questions"]["prospective_action"]
        self.assertEqual(q["criteria"].keys(), {"true", "false"})
        self.assertEqual(captured["payload"]["model"], "jev-1.14.0")

    def test_reject_probability_schema_extra_answer_and_bad_values(self):
        invalid = [
            {"type": "noul", "probability": .9},
            {"type": "noul", "noul": .9, "extra": 1},
            {"type": "noul", "noul": True},
            {"type": "noul", "noul": float("nan")},
            {"type": "noul", "noul": 1.1},
        ]
        for answer in invalid:
            with self.subTest(answer=answer), self.assertRaises(LlmError):
                JevV2Resolver(transport=lambda p, t: {"answers": {"prospective_action": answer}}).resolve(event(), {"decision": "REVIEW"})
        with self.assertRaises(LlmError):
            JevV2Resolver(transport=lambda p,t: {"answers": {"prospective_action":{"type":"noul","noul":.9},"extra":{"type":"noul","noul":.1}}}).resolve(event(), {"decision":"REVIEW"})

    def test_same_work_is_judged_and_not_created(self):
        candidate = {"business_key":"old", "external_task_id":"task-old", "route_module":"materials", "route_owner":"苗笑天", "status":"EXTERNAL_CREATED", "source_text":"请完成库存核对工作。", "message_time":"2026-09-20 09:00:00"}
        _, plan = processor({"prospective_action":{"type":"noul","noul":.97}, "same_work_business_0":{"type":"noul","noul":.94}}, candidates=[candidate])
        self.assertEqual(plan["decision"], "IGNORE")
        self.assertEqual(plan["reason"], "jev_v2_same_work_already_exists")

    def test_low_action_signal_is_ignore(self):
        _, plan = processor({"prospective_action":{"type":"noul","noul":.08}})
        self.assertEqual(plan["decision"], "IGNORE")

    def test_uncertain_candidate_or_incomplete_query_fails_closed(self):
        candidate={"business_key":"old", "external_task_id":"task-old", "route_module":"materials", "route_owner":"苗笑天", "status":"EXTERNAL_CREATED", "source_text":"核查库存", "message_time":"2026-09-20 09:00:00"}
        _, plan = processor({"prospective_action":{"type":"noul","noul":.97}, "same_work_business_0":{"type":"noul","noul":.51}}, candidates=[candidate])
        self.assertEqual(plan["decision"], "REVIEW")
        _, plan = processor({}, complete=False)
        self.assertEqual(plan["decision"], "REVIEW")

    def test_chinese_mentions_and_identifiers_redacted_in_serialized_body(self):
        captured={}
        def redaction_transport(payload, timeout):
            captured.update(payload=payload)
            opts = payload["questions"]["source_clause"]["criteria"]
            return {"answers":{"prospective_action":{"type":"noul","noul":.9},
                "source_clause":{"type":"choice","choice":"none","probabilities":{k:(.98 if k=="none" else .005) for k in opts},"confidence":.98}}}
        resolver=JevV2Resolver(transport=redaction_transport)
        msg=event("@苗笑天 请处理任务ID=task_123456", at_users=[{"name":"苗笑天","userId":"user_998877"}])
        resolver.resolve(msg,{"decision":"REVIEW"})
        body=json.dumps(captured["payload"],ensure_ascii=False)
        self.assertNotIn("苗笑天", body)
        self.assertNotIn("task_123456", body)
        self.assertNotIn("user_998877", body)
        self.assertGreater(resolver.last_redaction_counts["message"]["person_mentions"], 0)

    def test_missing_date_is_ok_but_missing_owner_or_scope_is_review(self):
        _, no_due = processor({"prospective_action":{"type":"noul","noul":.99}}, text="请尽快完成库存核对工作。")
        self.assertEqual(no_due["decision"], "REVIEW")
        _, explicit_other = processor({"prospective_action":{"type":"noul","noul":.99}},
                                      text="@李林骁 请于2026-09-30前完成库存核对工作。", sender="OTHER")
        self.assertEqual(explicit_other["decision"], "CREATE")
        _, self_mention = processor({"prospective_action":{"type":"noul","noul":.99}},
                                    text="@李林骁 请于2026-09-30前完成库存核对工作。", sender="SELF")
        self.assertEqual(self_mention["decision"], "REVIEW")
        config, self_user = runtime()
        config["active_routing_loaded"] = False
        proc = EventProcessor(config, self_user, mode="test", llm_resolver=JevV2Resolver(transport=lambda p,t:{"answers":{"prospective_action":{"type":"noul","noul":.99}}}), existing_actions=[], business_candidate_query_complete=True)
        result = proc.process_batch([event()])[0]
        self.assertNotEqual(result["decision"], "CREATE")

    def test_existing_missing_scope_and_date_errors_do_not_create(self):
        config, self_user = runtime()
        proc = EventProcessor(config, self_user, mode="test", llm_resolver=JevV2Resolver(
            transport=lambda p,t:{"answers":{"prospective_action":{"type":"noul","noul":.99}}}),
            existing_actions=[], business_candidate_query_complete=True)
        plan = proc.process_batch([event("请于2026-09-30及2026-10-01前完成库存核对")])[0]
        self.assertNotEqual(plan["decision"], "CREATE")

    def test_self_authored_or_attachment_only_never_creates(self):
        _, plan=processor({"prospective_action":{"type":"noul","noul":.99}}, sender="SELF")
        self.assertNotEqual(plan["decision"], "CREATE")
        _, plan=processor({"prospective_action":{"type":"noul","noul":.99}}, text="[附件：图片]")
        self.assertNotEqual(plan["decision"], "CREATE")

    def test_create_requires_high_signal_and_complete_routing_with_due_date(self):
        _, plan=processor({"prospective_action":{"type":"noul","noul":.99}}, text="请于2026-09-30前完成库存核对工作。")
        self.assertEqual(plan["decision"], "CREATE")
        self.assertEqual(plan["due_date"], "2026-09-30")

if __name__ == "__main__":
    unittest.main()
