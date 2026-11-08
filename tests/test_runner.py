import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import normalize_config, plan_batch  # noqa: E402


class RunnerTests(unittest.TestCase):
    def test_normalize_runtime_config_staff_rows(self):
        config = normalize_config(
            {
                "config_staff": [
                    [1, "李林骁", "STALE-ID", "leader"],
                    [2, "谭继衡", "TAN-ID", "executor"],
                ]
            }
        )
        self.assertEqual(config["staff"]["李林骁"]["role"], "leader")
        self.assertEqual(config["staff"]["谭继衡"]["user_id"], "TAN-ID")

    def test_test_batch_has_no_external_writes_and_self_only(self):
        payload = {
            "config": {
                "config_staff": [
                    [1, "李林骁", "STALE-ID", "leader"],
                    [2, "谭继衡", "TAN-ID", "executor"],
                ],
                "active_routing_loaded": True,
                "responsibility_rules": {"ledger": "谭继衡"},
                "handover_evidence": [{
                    "period": "2026-09", "area_name": "杂品库", "active_owners": ["苗笑天"],
                    "reviewed": True, "status": "accepted", "hygiene_review": "passed",
                    "source": "dws_readback", "evidence_id": "synthetic-runner-junk-handover",
                }],
            },
            "self_user": {"name": "李林骁", "user_id": "SELF-LIVE-ID"},
            "messages": [
                {
                    "conversation_id": "conv-1",
                    "message_id": "msg-1",
                    "group": "铁炉西工班群",
                    "sender": "张云云",
                    "create_time": "2026-09-21 09:00:00",
                    "text": "请于9月22日完成危废台账核对整改。",
                    "title": "完成危废台账核对整改",
                }
            ],
        }
        result = plan_batch(payload, mode="test", run_date=date(2026, 9, 21))
        self.assertEqual(result["summary"]["create"], 1)
        self.assertEqual(result["summary"]["writes_performed"], 0)
        plan = result["plans"][0]
        self.assertEqual(plan["executor_ids"], ["SELF-LIVE-ID"])
        self.assertEqual(plan["executor_names"], ["李林骁"])
        self.assertIn("来源群：铁炉西工班群", plan["comment"])
        self.assertFalse(plan["notify_group"])

    def test_legacy_production_writer_fails_closed_without_durable_ledger(self):
        from runner import _production_write
        payload = {
            "config": {"config_staff": [[1, "李林骁", "SELF", "leader"], [2, "谭继衡", "TAN", "executor"]],
                       "active_routing_loaded": True, "responsibility_rules": {"ledger": "谭继衡"},
                       "handover_evidence": [{
                           "period": "2026-09", "area_name": "杂品库", "active_owners": ["谭继衡"],
                           "reviewed": True, "status": "accepted", "hygiene_review": "passed",
                           "source": "dws_readback", "evidence_id": "synthetic-runner-ledger-handover",
                       }]},
            "self_user": {"name": "李林骁", "user_id": "SELF"},
            "messages": [{"conversation_id": "c", "message_id": "m", "group": "铁炉西工班群",
                          "sender": "张云云", "create_time": "2026-09-21 09:00:00",
                          "text": "请于9月22日完成危废台账核对整改。"}],
        }
        result = _production_write(payload, date(2026, 9, 21))
        self.assertEqual(result["plans"][0]["decision"], "REVIEW")
        self.assertEqual(result["summary"]["writes_performed"], 0)
        self.assertEqual(result["errors"][0]["error"], "production_write_blocked_no_precreate_ledger")
        self.assertEqual(result["summary"]["create"], 0)
        self.assertEqual(result["summary"]["review"], 1)

    def test_ambiguous_title_is_queued_for_llm_only(self):
        payload = {
            "config": {"config_staff": [[1, "李林骁", "STALE-ID", "leader"]]},
            "self_user": {"name": "李林骁", "user_id": "SELF-LIVE-ID"},
            "messages": [
                {
                    "conversation_id": "conv-1",
                    "message_id": "msg-2",
                    "group": "铁炉西工班群",
                    "sender": "李林骁",
                    "create_time": "2026-09-21 09:00:00",
                    "text": "通知：请各相关人员按照附件要求完成本次综合检查资料整理及后续闭环反馈，具体事项见群内上下文和附件。",
                }
            ],
        }
        result = plan_batch(payload, mode="test", run_date=date(2026, 9, 21))
        self.assertEqual(result["summary"]["review"], 1)
        self.assertEqual(len(result["llm_queue"]), 1)
        self.assertEqual(result["summary"]["writes_performed"], 0)


if __name__ == "__main__":
    unittest.main()
