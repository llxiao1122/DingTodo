import os
import unittest
from datetime import date
from unittest.mock import patch

from live_runner import build_planner_config
from event_worker import _possible_cross_group_repeat
from routing import plan_message
from runner import plan_batch


SELF = {"name": "李林骁", "user_id": "current-user-id"}
RUN_DATE = date(2026, 11, 3)


def sample_message(text):
    return {
        "conversation_id": "test-conversation",
        "message_id": "test-message",
        "group": "铁炉西工班群",
        "sender": "其他人",
        "create_time": "2026-11-03 14:00:00",
        "text": text,
        "title": text,
        "_jev_v2_applicable": True,
        "_jev_v2_title_verified": True,
    }


class DefaultSelfAssignmentTests(unittest.TestCase):
    def test_expired_area_assignments_do_not_block_supported_work(self):
        with patch.dict(os.environ, {"DINGTODO_ASSIGN_SELF_ONLY": "1"}):
            config = build_planner_config({"config_staff": [], "config_area_duty": []}, RUN_DATE)
        messages = [
            "请各工班盘点资产并反馈结果。@工班长",
            "请各工班检查危废存放并反馈。@工班长",
            "请各工班完成消防整改。@工班长",
            "会议通知：请李林骁参加安全例会。@李林骁",
        ]
        for text in messages:
            with self.subTest(text=text):
                result = plan_batch(
                    {"config": config, "self_user": SELF, "messages": [sample_message(text)]},
                    mode="production", run_date=RUN_DATE, live_ids={"李林骁": "current-user-id"},
                )["plans"][0]
                self.assertEqual(result["decision"], "CREATE", result)
                self.assertEqual(result["executor_names"], ["李林骁"])
                self.assertEqual(result["executor_ids"], ["current-user-id"])
                self.assertNotIn("active_routing_not_loaded", result["warnings"])

    def test_existing_source_is_not_created_twice(self):
        config = {"assign_to_self_only": True, "staff": {}}
        result = plan_message(
            sample_message("请各工班盘点资产并反馈结果。@工班长"), config, SELF,
            run_date=RUN_DATE, mode="production", live_ids={"李林骁": "current-user-id"},
            existing_actions=[{"conversation_id": "test-conversation", "message_id": "test-message", "action_key": "todo"}],
        )
        self.assertEqual((result["decision"], result["reason"]), ("IGNORE", "source_already_processed"))

    def test_unrelated_notices_with_same_compiled_title_have_distinct_business_keys(self):
        config = {"assign_to_self_only": True, "staff": {}}
        def plan(text):
            message = sample_message(text)
            message.update(title="处理工班物资工作", _jev_v2_title_verified=True)
            return plan_message(message, config, SELF, run_date=RUN_DATE,
                                mode="production", live_ids={"李林骁": "current-user-id"})

        first = plan("请涉及工班领取已完成的计量器具。")
        second = plan("请工班核对固定资产验收单制造商信息。")
        self.assertNotEqual(first["business_key"], second["business_key"])
        self.assertEqual(first["business_key"], plan("请涉及工班领取已完成的计量器具。 ")["business_key"])

    def test_missing_current_user_id_prevents_creation(self):
        result = plan_message(
            sample_message("请各工班盘点资产并反馈结果。@工班长"),
            {"assign_to_self_only": True, "staff": {}}, {"name": "李林骁", "user_id": ""},
            run_date=RUN_DATE, mode="production", live_ids={},
        )
        self.assertEqual(result["decision"], "REVIEW")
        self.assertIn("self_user_id", result["missing_fields"])

    def test_a_named_colleague_does_not_change_the_default_executor(self):
        config = {
            "assign_to_self_only": True,
            "staff": {"张志斌": {"name": "张志斌", "user_id": "other-id"}},
        }
        result = plan_message(
            sample_message("请各工班张志斌负责核对资产。@工班长"), config, SELF,
            run_date=RUN_DATE, mode="production", live_ids={"李林骁": "current-user-id"},
        )
        self.assertEqual(result["decision"], "CREATE", result)
        self.assertEqual(result["executor_names"], ["李林骁"])
        self.assertNotIn("owner_conflict", result["warnings"])

    def test_missing_legacy_assignment_contract_does_not_block_self_default(self):
        from assignment_policy import AssignmentContractError

        with patch("routing.classify_rule", side_effect=AssignmentContractError("unavailable")):
            result = plan_message(
                sample_message("请各工班盘点资产并反馈结果。@工班长"),
                {"assign_to_self_only": True, "staff": {}}, SELF,
                run_date=RUN_DATE, mode="production", live_ids={"李林骁": "current-user-id"},
            )
        self.assertEqual(result["decision"], "CREATE")
        self.assertEqual(result["executor_names"], ["李林骁"])

    def test_identical_cross_group_notice_from_same_sender_is_held_for_review(self):
        text = "会议通知：11月4日13:30在427会议室召开月度安全例会，请各位及时参加。"
        previous = {
            "conversation_id": "first-group", "sender_id": "sender-1", "planned_decision": "CREATE",
            "create_time": "2026-11-03 18:22:10", "text": text + "@李林骁(李林骁)",
        }
        current = {
            "conversation_id": "second-group", "sender_id": "sender-1",
            "create_time": "2026-11-03 18:22:57", "text": text + "@所有人",
        }
        self.assertTrue(_possible_cross_group_repeat(current, [previous]))
        self.assertFalse(_possible_cross_group_repeat({**current, "sender_id": "other"}, [previous]))
        self.assertFalse(_possible_cross_group_repeat({**current, "text": text.replace("13:30", "14:00")}, [previous]))
        self.assertFalse(_possible_cross_group_repeat({**current, "create_time": "2026-11-03 19:00:00"}, [previous]))
        self.assertFalse(_possible_cross_group_repeat(current, [{**previous, "planned_decision": "IGNORE"}]))


if __name__ == "__main__":
    unittest.main()
