import json
import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from routing import plan_message, parse_due_date, parse_due_datetime, build_source_comment, extract_due_candidates  # noqa: E402


SELF = {"name": "李林骁", "user_id": "SELF-LIVE-ID"}
STAFF = {
    "李林骁": {"name": "李林骁", "user_id": "STALE-SELF-ID", "role": "leader"},
    "陈红洁": {"name": "陈红洁", "user_id": "CHEN-ID", "role": "executor"},
    "张志斌": {"name": "张志斌", "user_id": "ZHANG-ID", "role": "executor"},
    "杨梦卓": {"name": "杨梦卓", "user_id": "YANG-ID", "role": "executor"},
    "苗笑天": {"name": "苗笑天", "user_id": "MIAO-ID", "role": "executor"},
    "谭继衡": {"name": "谭继衡", "user_id": "TAN-ID", "role": "executor"},
}
CONFIG = {
    "staff": STAFF,
    "active_routing_loaded": True,
    "responsibility_rules": {
        "attendance": "陈红洁",
        "training": "张志斌",
        "drill": "陈红洁",
        "asset": "杨梦卓",
        "materials": "苗笑天",
        "ledger": "谭继衡",
        "leader_inspection": "李林骁",
    },
    "handover_evidence": [
        {
            "period": "2026-09", "area_name": "杂品库", "active_owners": ["苗笑天"],
            "reviewed": True, "status": "accepted", "hygiene_review": "passed",
            "source": "dws_readback", "evidence_id": "synthetic-junk-handover-sep",
        },
        {
            "period": "2026-09", "area_name": "材料棚及办公用房", "active_owners": ["张志斌"],
            "reviewed": True, "status": "accepted", "hygiene_review": "passed",
            "source": "dws_readback", "evidence_id": "synthetic-shed-handover-sep",
        },
    ],
}


def message(text, **extra):
    base = {
        "conversation_id": "conv-1",
        "message_id": "msg-1",
        "group": "铁炉西工班群",
        "sender": "张云云",
        "create_time": "2026-09-21 09:00:00",
        "text": text,
    }
    base.update(extra)
    return base


class RoutingTests(unittest.TestCase):
    def test_exact_authored_holiday_broadcast_requires_review_not_self_todo(self):
        text = (
            "[图片消息](mediaId=a)[图片消息](mediaId=b)中秋节和国庆节值班要求有更新，"
            "请各工班长收到后回复确认，并传达到班组每个人。"
            "@翟笑然(翟笑然) @常俊(常俊) @李林骁(李林骁)"
        )
        result = plan_message(
            message(text, sender="李林骁", sender_id="SELF-LIVE-ID"),
            CONFIG, SELF, run_date=date(2026, 9, 24), mode="production",
            live_ids={"李林骁": "SELF-LIVE-ID"},
        )
        self.assertEqual(result["decision"], "REVIEW")
        self.assertIn("self_authored_requires_review", result["warnings"])

    def test_own_message_never_auto_creates_self_todo(self):
        for text in (
            "请各工班长收到后回复确认，并传达到班组每个人。@李林骁(李林骁)",
            "请各工班及科室人员开展签订工作。@所有人",
            "排查结果反馈群11点前@",
        ):
            with self.subTest(text=text):
                result = plan_message(
                    message(text, sender="李林骁", sender_id="SELF-LIVE-ID"),
                    CONFIG, SELF, run_date=date(2026, 9, 24), mode="production",
                    live_ids={"李林骁": "SELF-LIVE-ID"},
                )
                self.assertNotEqual(result["decision"], "CREATE")
                self.assertIn("self_authored_requires_review", result["warnings"])

    def test_verified_sender_namespace_confirms_self_authored_and_short_circuits_dispatch(self):
        result = plan_message(
            message(
                "《库区巡库指导手册》意见或建议，请今天12:00前反馈意见或建议。@所有人",
                sender="李林骁", sender_id="SENDER-SPACE-SELF",
            ),
            CONFIG,
            {**SELF, "message_sender_id": "SENDER-SPACE-SELF"},
            run_date=date(2026, 9, 30), mode="test",
        )
        self.assertEqual(result["decision"], "IGNORE")
        self.assertEqual(result["reason"], "self_authored_notice")

    def test_same_name_without_verified_sender_mapping_stays_review(self):
        result = plan_message(
            message("请今天12:00前反馈意见。@所有人", sender="李林骁", sender_id="UNMAPPED-SENDER"),
            CONFIG, SELF, run_date=date(2026, 9, 30), mode="test",
        )
        self.assertEqual(result["decision"], "REVIEW")
        self.assertIn("self_authorship_unverified", result["warnings"])

    def test_own_message_detected_by_sender_name_without_id(self):
        result = plan_message(
            message("请各工班明天完成盘点。@所有人", sender="李林骁"),
            CONFIG, SELF, run_date=date(2026, 9, 24), mode="test",
        )
        self.assertEqual(result["decision"], "REVIEW")

    def test_reply_reporting_no_problem_does_not_create_self_todo(self):
        result = plan_message(
            message("@李林骁(李林骁) 无问题", sender="杨梦卓", quoted_message={"text": "排查结果反馈群11点前@"}),
            CONFIG, SELF, run_date=date(2026, 9, 24), mode="production",
            live_ids={"李林骁": "SELF-LIVE-ID"},
        )
        self.assertEqual(result["decision"], "IGNORE")
        self.assertEqual(result["reason"], "completion_report")

    def test_broadcast_information_without_action_is_review_not_todo(self):
        result = plan_message(
            message("此文件中内容为9月底废矿物油处置清单。@所有人", sender="王超06", group="危废、废旧工作群"),
            CONFIG, SELF, run_date=date(2026, 9, 24), mode="production",
            live_ids={"李林骁": "SELF-LIVE-ID"},
        )
        self.assertEqual(result["decision"], "REVIEW")
        self.assertIn("explicit_action_missing", result["warnings"])

    def test_direct_at_current_user_resolves_unmatched_route_to_self(self):
        result = plan_message(
            message("请五点前反馈盘点表@李林骁(李林骁)"),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 22),
            mode="test",
        )
        self.assertEqual(result["decision"], "CREATE")
        self.assertEqual(result["route_owner"], "李林骁")
        self.assertEqual(result["route_module"], "direct_assignment")
        self.assertEqual(result["assignment_basis"], "direct_mention")
        self.assertEqual(result["executor_names"], ["李林骁"])
        self.assertEqual(result["due_iso"], "2026-09-22T17:00:00+08:00")

    def test_structured_mention_assigns_current_user_without_text_marker(self):
        result = plan_message(
            message(
                "请五点前反馈盘点表",
                at_users=[{"name": "李林骁", "userId": "SELF-LIVE-ID"}],
            ),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 22),
            mode="test",
        )
        self.assertEqual(result["decision"], "CREATE")
        self.assertEqual(result["assignment_basis"], "structured_mention")
        self.assertEqual(result["route_owner"], "李林骁")

    def test_direct_at_current_user_without_action_signal_needs_review(self):
        result = plan_message(
            message(
                "通知：前期统计的回购物资仍存放在集中存储货位。但需求部门可以办理领用手续。@李林骁(李林骁)",
                group="台账更新群",
            ),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 22),
            mode="test",
        )
        self.assertEqual(result["decision"], "REVIEW")
        self.assertEqual(result["assignment_basis"], "direct_mention")
        self.assertIn("explicit_action_missing", result["warnings"])
        self.assertIsNone(result["due_date"])
        self.assertEqual(result["executor_names"], [])

    def test_direct_at_work_reminder_with_image_placeholders_is_created(self):
        result = plan_message(
            message(
                "[图片消息](mediaId=x)工作提醒：4个前期工作，各工班注意开展时间，不要忘记了。@李林骁(李林骁)",
                group="台账更新群",
            ),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 22),
            mode="test",
        )
        self.assertEqual(result["decision"], "CREATE")
        self.assertEqual(result["title"], "工作提醒：4个前期工作，各工班注意开展时间，不要忘记了")
        self.assertIsNone(result["due_date"])

    def test_local_all_notice_without_action_needs_review(self):
        result = plan_message(
            message(
                "此文件中内容为9月底废矿物油（含废油桶）废空容器处置清单。@所有人",
                group="危废、废旧工作群",
            ),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 22),
            mode="test",
        )
        self.assertEqual(result["decision"], "REVIEW")
        self.assertEqual(result["assignment_basis"], "workgroup_all_mention")
        self.assertIn("explicit_action_missing", result["warnings"])
        self.assertIsNone(result["due_date"])
        self.assertEqual(result["executor_names"], [])

    def test_all_workgroups_opinion_request_creates_when_current_workgroup_is_covered(self):
        result = plan_message(
            message(
                "关于铁炉西工班安全奖惩细则的意见征询，请各工班于9月29日前反馈。@所有人",
                group="物资管理部学习交流群",
            ),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 22),
            mode="test",
        )
        self.assertEqual(result["decision"], "CREATE")
        self.assertEqual(result["assignment_basis"], "workgroup_all_mention")
        self.assertEqual(result["route_owner"], "李林骁")
        self.assertEqual(result["due_date"], "2026-09-29")
        self.assertEqual(result["executor_names"], ["李林骁"])

    def test_plain_all_mention_stays_ignored_without_substantive_object(self):
        result = plan_message(
            message("@所有人", group="铁炉西工班群"),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 22),
            mode="test",
        )
        self.assertEqual(result["decision"], "IGNORE")
        self.assertEqual(result["reason"], "no_action_signal")

    def test_direct_at_test_message_remains_ignored(self):
        result = plan_message(
            message("信息测试@李林骁(李林骁)"),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 22),
            mode="test",
        )
        self.assertEqual(result["decision"], "IGNORE")
        self.assertEqual(result["reason"], "test_message")

    def test_direct_at_completion_report_remains_ignored(self):
        result = plan_message(
            message("已完成盘点表，@李林骁(李林骁)"),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 22),
            mode="test",
        )
        self.assertEqual(result["decision"], "IGNORE")
        self.assertEqual(result["reason"], "completion_report")

    def test_name_and_workgroup_reference_assigns_current_user(self):
        result = plan_message(
            message("李林骁负责跟进铁炉西工班这项工作"),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 22),
            mode="test",
        )
        self.assertEqual(result["decision"], "CREATE")
        self.assertEqual(result["assignment_basis"], "self_and_workgroup_reference")
        self.assertEqual(result["route_owner"], "李林骁")

    def test_all_general_library_team_leaders_alias_maps_to_self(self):
        result = plan_message(
            message("请各总库工班长于明天上午10点完成盘点表反馈。"),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 22),
            mode="test",
        )
        self.assertEqual(result["decision"], "CREATE")
        self.assertEqual(result["route_owner"], "李林骁")
        self.assertEqual(result["assignment_basis"], "role_alias")
        self.assertEqual(result["due_iso"], "2026-09-23T10:00:00+08:00")

    def test_direct_at_current_user_overrides_department_scope_gate(self):
        result = plan_message(
            message(
                "请物资管理中心于今天17:00前反馈盘点表@李林骁(李林骁)",
                group="物资管理部学习交流群",
            ),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 22),
            mode="test",
        )
        self.assertEqual(result["decision"], "CREATE")
        self.assertEqual(result["workgroup_handling"], "DIRECT_SELF")
        self.assertEqual(result["route_owner"], "李林骁")

    def test_test_mode_assigns_only_self_but_preserves_matrix_owner(self):
        result = plan_message(
            message("请于9月22日完成危废台账核对整改。", title="完成危废台账核对整改"),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 21),
            mode="test",
        )
        self.assertEqual(result["decision"], "CREATE")
        self.assertEqual(result["route_owner"], "苗笑天")
        self.assertEqual(result["executor_ids"], ["SELF-LIVE-ID"])
        self.assertEqual(result["executor_names"], ["李林骁"])
        self.assertFalse(result["notify_group"])

    def test_scrap_route_uses_injected_material_shed_handover_evidence(self):
        result = plan_message(
            message("请于9月22日完成废旧物资接收台账核对。", title="完成废旧物资接收台账核对"),
            {**CONFIG, "responsibility_rules": {**CONFIG["responsibility_rules"], "scrap": "谭继衡"}},
            SELF,
            run_date=date(2026, 9, 21),
            mode="test",
        )
        self.assertEqual(result["decision"], "CREATE")
        self.assertEqual(result["route_owner"], "张志斌")
        self.assertEqual(result["matrix_route_owner"], "谭继衡")

    def test_production_always_includes_self_and_route_owner(self):
        result = plan_message(
            message("请于9月22日完成库存物资来源核对。", title="完成库存物资来源核对"),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 21),
            mode="production",
            live_ids={"苗笑天": "LIVE-MIAO", "李林骁": "SELF-LIVE-ID"},
        )
        self.assertEqual(result["decision"], "CREATE")
        self.assertEqual(result["route_owner"], "苗笑天")
        self.assertEqual(result["executor_ids"], ["LIVE-MIAO", "SELF-LIVE-ID"])
        self.assertEqual(result["executor_names"], ["苗笑天", "李林骁"])
        self.assertFalse(result["notify_group"])

    def test_training_ledger_uses_training_route_not_generic_ledger(self):
        result = plan_message(
            message("请于9月24日前完成继续教育报名名单报送。", title="完成继续教育报名名单报送"),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 21),
            mode="test",
        )
        self.assertEqual(result["decision"], "CREATE")
        self.assertEqual(result["route_owner"], "张志斌")

    def test_no_action_message_requests_semantic_follow_up_resolution(self):
        result = plan_message(
            message("回购物资仍存放在集中存储货位，需求部门可在9月23日前办理领用手续。"),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 22),
            mode="test",
        )
        self.assertEqual(result["decision"], "REVIEW")
        self.assertEqual(result.get("reason"), "semantic_resolution_required")
        self.assertIn("semantic_intent_needs_llm", result["warnings"])

    def test_semantic_follow_up_hint_promotes_only_an_otherwise_actionable_local_task(self):
        result = plan_message(
            message(
                "回购物资仍存放在集中存储货位，需求部门可在9月23日前办理领用手续。",
                _llm_semantic_intent="follow_up",
            ),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 22),
            mode="test",
        )
        self.assertEqual(result["decision"], "CREATE")
        self.assertEqual(result["semantic_intent"], "follow_up")
        self.assertEqual(result["route_owner"], "苗笑天")
        self.assertEqual(result["due_date"], "2026-09-23")

    def test_semantic_follow_up_hint_cannot_bypass_department_scope_or_missing_due(self):
        department = plan_message(
            message(
                "各科室可在9月23日前办理领用手续。",
                group="物资管理部学习交流群",
                _llm_semantic_intent="follow_up",
            ),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 22),
            mode="test",
        )
        self.assertEqual(department["decision"], "IGNORE")
        self.assertEqual(department["reason"], "department_scope_non_local")

        missing_due = plan_message(
            message("回购物资仍存放在集中存储货位，需求部门可办理领用手续。", _llm_semantic_intent="follow_up"),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 22),
            mode="test",
        )
        self.assertEqual(missing_due["decision"], "REVIEW")
        self.assertIn("due_date", missing_due["missing_fields"])

    def test_missing_due_date_goes_to_review(self):
        result = plan_message(
            message("请核对危废台账。", title="核对危废台账"),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 21),
            mode="test",
        )
        self.assertEqual(result["decision"], "REVIEW")
        self.assertIn("due_date", result["missing_fields"])
        self.assertEqual(result["executor_ids"], [])

    def test_explicit_conflict_goes_to_review(self):
        result = plan_message(
            message("请张志斌于9月22日完成危废台账核对。", title="完成危废台账核对"),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 21),
            mode="test",
        )
        self.assertEqual(result["decision"], "REVIEW")
        self.assertIn("owner_conflict", result["warnings"])

    def test_source_comment_keeps_group_and_original_notice(self):
        comment = build_source_comment(message("请于9月22日完成盘点。"))
        self.assertIn("来源群：铁炉西工班群", comment)
        self.assertIn("原文：\n请于9月22日完成盘点。", comment)
        self.assertIn("messageId：msg-1", comment)

    def test_source_fields_are_required(self):
        result = plan_message(
            message("请于9月22日完成盘点。", group=""),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 21),
            mode="test",
        )
        self.assertEqual(result["decision"], "REVIEW")
        self.assertIn("source_group", result["missing_fields"])

    def test_source_idempotency_suppresses_existing_action(self):
        result = plan_message(
            message("请于9月22日完成盘点。", title="完成盘点"),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 21),
            mode="test",
            existing_actions=[{"source_key": "conv-1:msg-1:todo", "external_task_id": "123456"}],
        )
        self.assertEqual(result["decision"], "IGNORE")
        self.assertEqual(result["reason"], "source_already_processed")

    def test_source_idempotency_matches_persisted_action_row(self):
        result = plan_message(
            message("请于9月22日完成盘点。", title="完成盘点"),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 21),
            mode="test",
            existing_actions=[
                {
                    "conversation_id": "conv-1",
                    "message_id": "msg-1",
                    "action_key": "todo",
                    "status": "SUCCESS",
                }
            ],
        )
        self.assertEqual(result["decision"], "IGNORE")
        self.assertEqual(result["reason"], "source_already_processed")

    def test_missing_active_routing_config_blocks_creation(self):
        result = plan_message(
            message("请于9月22日完成危废台账核对整改。", title="完成危废台账核对整改"),
            {"staff": STAFF},
            SELF,
            run_date=date(2026, 9, 21),
            mode="test",
        )
        self.assertEqual(result["decision"], "REVIEW")
        self.assertIn("active_routing_not_loaded", result["warnings"])

    def test_department_only_scope_is_not_local_create(self):
        result = plan_message(
            message("请各科室完成继续教育报名名单报送。", title="完成继续教育报名名单报送", group="物资管理部学习交流群"),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 21),
            mode="test",
        )
        self.assertEqual(result["decision"], "IGNORE")
        self.assertEqual(result["reason"], "department_scope_non_local")

    def test_department_opinion_feedback_is_not_routed_to_materials_owner(self):
        result = plan_message(
            message(
                "现已完成《物资管理中心安全奖惩管理细则》修订，现进行意见征询，如有意见请于9月29日前反馈意见。@所有人",
                group="台账更新群",
            ),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 22),
            mode="test",
        )
        self.assertEqual(result["decision"], "IGNORE")
        self.assertEqual(result["reason"], "department_scope_non_local")
        self.assertIsNone(result["route_owner"])
        self.assertEqual(result["semantic_context"]["target_scope"], "department_only")

    def test_department_scope_overrides_local_source_group(self):
        result = plan_message(
            message(
                "现已完成《物资管理中心安全奖惩管理细则》修订，现进行意见征询，如有意见请于9月29日前反馈意见。",
                group="物资调配室",
            ),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 22),
            mode="test",
        )
        self.assertEqual(result["decision"], "IGNORE")
        self.assertEqual(result["reason"], "department_scope_non_local")
        self.assertIsNone(result["route_owner"])

    def test_external_group_message_without_workgroup_target_stays_review(self):
        result = plan_message(
            message(
                "通知：今天上午，消防救援大队对郑东车辆段进行消防安全检查，请现场配合。",
                group="物资管理部安全管理群",
            ),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 22),
            mode="test",
        )
        self.assertEqual(result["decision"], "REVIEW")
        self.assertIn("workgroup_scope_unconfirmed", result["warnings"])
        self.assertIsNone(result["route_owner"])

    def test_workgroup_source_is_eligible_for_responsibility_routing(self):
        result = plan_message(
            message(
                "请于9月24日前完成物资盘点并反馈结果。",
                group="铁炉西工班群",
            ),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 22),
            mode="test",
        )
        self.assertEqual(result["decision"], "CREATE")
        self.assertEqual(result["route_owner"], "苗笑天")

    def test_colloquial_clock_deadline_is_parsed_with_exact_time(self):
        run_date = date(2026, 9, 22)
        self.assertEqual(parse_due_date("请五点前反馈盘点表", run_date), "2026-09-22")
        self.assertEqual(
            parse_due_datetime("请五点前反馈盘点表", run_date),
            ("2026-09-22", "2026-09-22T17:00:00+08:00"),
        )
        self.assertEqual(
            parse_due_datetime("请明天下午5点半前反馈", run_date),
            ("2026-09-23", "2026-09-23T17:30:00+08:00"),
        )

    def test_plan_preserves_colloquial_clock_deadline(self):
        result = plan_message(
            message("请五点前反馈盘点表@李林骁(李林骁)"),
            CONFIG,
            SELF,
            run_date=date(2026, 9, 22),
            mode="test",
        )
        self.assertEqual(result["due_date"], "2026-09-22")
        self.assertEqual(result["due_iso"], "2026-09-22T17:00:00+08:00")
        self.assertEqual(result["decision"], "CREATE")
        self.assertEqual(result["route_owner"], "李林骁")
        self.assertEqual(result["assignment_basis"], "direct_mention")

    def test_program_extracts_only_valid_source_date_candidates_in_china_time(self):
        candidates = extract_due_candidates(
            "请于2026年9月29日17:00前完成资料核对；另在9月30日前反馈。",
            date(2026, 9, 27),
        )
        self.assertEqual(
            candidates,
            [
                {
                    "key": "date_0",
                    "due_date": "2026-09-29",
                    "due_iso": "2026-09-29T17:00:00+08:00",
                },
                {
                    "key": "date_1",
                    "due_date": "2026-09-30",
                    "due_iso": "2026-09-30T18:00:00+08:00",
                },
            ],
        )

    def test_program_rejects_invalid_calendar_date_and_reports_missing_candidate(self):
        with self.assertRaisesRegex(ValueError, "invalid_due_date_candidate"):
            extract_due_candidates("请于2026年2月30日前完成资料核对。", date(2026, 2, 1))
        self.assertEqual(extract_due_candidates("请尽快完成资料核对。", date(2026, 2, 1)), [])

    def test_due_date_parser_uses_explicit_year_and_china_workflow(self):
        self.assertEqual(parse_due_date("请于9月24日下班前报送", date(2026, 9, 21)), "2026-09-24")
        self.assertEqual(parse_due_date("明天完成", date(2026, 9, 21)), "2026-09-22")

    def test_numeric_version_like_token_is_not_a_deadline(self):
        self.assertIsNone(parse_due_date("前期网盘4.8文件，有需要请自取", date(2026, 9, 21)))


if __name__ == "__main__":
    unittest.main()

