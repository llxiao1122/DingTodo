import json
from datetime import date
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from jev_v2 import JevV2Resolver
from event_worker import EventProcessor
from routing import extract_due_candidates


def _runtime():
    return ({"config_group": [{"group_name": "物资调配室", "conversation_id": "c1", "purpose": "todo_scan"}],
             "config_staff": [[1, "李林骁", "SELF", "leader"], [2, "苗笑天", "MIAO", "executor"]],
             "active_routing_loaded": True, "responsibility_rules": {"materials": "苗笑天"},
             "config_area_duty": [{"workgroup": "铁炉西工班", "module": "materials", "owner": "苗笑天", "start_date": "2026-01-01", "end_date": "2026-12-31"}]},
            {"name": "李林骁", "user_id": "SELF"})


def _event(text):
    return {"conversation_id":"c1", "message_id":"m1", "group":"物资调配室", "sender":"王萌", "sender_id":"OTHER",
            "create_time":"2026-09-21 10:00:00", "text":text}


def _synthetic_junk_handover():
    return [{
        "period": "2026-09", "area_name": "杂品库", "active_owners": ["苗笑天"],
        "reviewed": True, "status": "accepted", "hygiene_review": "passed",
        "source": "dws_readback", "evidence_id": "synthetic-jev-junk-handover",
    }]


def _policy_answer(request, scope=0.99, follow=0.99):
    answers={"applies_to_workgroup":{"type":"noul","noul":scope},
             "needs_follow_up":{"type":"noul","noul":follow},
             "prospective_action":{"type":"noul","noul":0.99}}
    for key,question in request["questions"].items():
        if question["type"] == "choice":
            options=question["criteria"]
            if key.startswith("date_role_"):
                state=json.loads(request["state"])
                candidate_key=key.removeprefix("date_role_")
                next(row for row in state["date_role_candidates"] if row["key"] == candidate_key)
                choice="unknown"
            else:
                choice="none" if "none" in options else next(iter(options))
            answers[key]={"type":"choice","choice":choice,
                          "probabilities":{k:(0.99 if k==choice else 0.01/(len(options)-1)) for k in options},"confidence":0.99}
    return {"answers":answers}


class JevV2EventProcessorTests(unittest.TestCase):
    def test_direct_mentioned_workgroup_adjustment_is_not_rejected_by_legacy_replan_action_guard(self):
        cfg,self_user=_runtime()
        proc=EventProcessor(cfg,self_user,mode="shadow",llm_resolver=JevV2Resolver(new_policy=True,transport=lambda req,to:_policy_answer(req,scope=.8,follow=.88)),existing_actions=[],business_candidate_query_complete=True)
        msg=_event("@李林骁(李林骁) 每个工作日下午巡库时间能否调整至16：00开始。")
        msg["message_id"]="direct-rota-adjustment"
        plan=proc.process_batch([msg])[0]
        self.assertEqual(plan["decision"],"CREATE")
        self.assertEqual(plan["route_owner"],"李林骁")
        self.assertNotIn("explicit_action_missing",plan.get("warnings",[]))

    def test_due_candidates_keep_deadline_usage_separate_from_history(self):
        candidates=extract_due_candidates("今天上午11点前完成反馈。历史联单时间：2026年08月14日；另有处置日期2026年09月24日。",date(2026,9,30),include_context=True)
        usable=[x for x in candidates if x["deadline_context"]]
        self.assertEqual(len(usable),1)
        self.assertEqual(usable[0]["due_iso"],"2026-09-30T11:00:00+08:00")
        self.assertEqual(len([x for x in candidates if not x["deadline_context"]]),2)

    def test_effective_termination_time_is_not_a_completion_deadline(self):
        candidates=extract_due_candidates("防汛值守于10月7日18时终止，请知悉。",date(2026,10,7),include_context=True)
        assert candidates
        assert candidates[0]["role"] == "effective"

    def test_new_policy_generic_drill_scope_uses_default_owner_not_legacy_matrix_owner(self):
        cfg,self_user=_runtime()
        cfg["responsibility_rules"]["drill"]="陈红洁"
        proc=EventProcessor(cfg,self_user,mode="shadow",llm_resolver=JevV2Resolver(new_policy=True,transport=lambda req,to:_policy_answer(req)),existing_actions=[],business_candidate_query_complete=True)
        msg=_event("请各工班跟进安全演练频次调整并完成值班安排确认。")
        msg["message_id"]="generic-domain-default-owner"
        result=proc.process_batch([msg])[0]
        self.assertEqual(result["decision"],"CREATE")
        self.assertEqual(result["route_owner"],"李林骁")
        self.assertEqual(result["matrix_route_owner"],"陈红洁")

    def test_actual_all_employee_notice_with_workgroup_target_overrides_model_scope_uncertainty(self):
        cfg,self_user=_runtime()
        proc=EventProcessor(cfg,self_user,mode="shadow",llm_resolver=JevV2Resolver(new_policy=True,transport=lambda req,to:_policy_answer(req,scope=0.62,follow=0.78)),existing_actions=[],business_candidate_query_complete=True)
        msg=_event("通知：网络安全承诺书科室及工班人员是否已签订完毕。@所有人 今天上午11点前完成反馈。")
        msg["message_id"]="broad-allstaff-notice"
        plan=proc.process_batch([msg])[0]
        self.assertEqual(plan["decision"],"CREATE")
        self.assertEqual(plan["semantic_evidence"]["scope_basis"],"explicit_workgroup_or_self_target")
        self.assertEqual(plan["route_owner"],"李林骁")

    def test_authoritative_hazard_owner_missing_is_specific_review_not_self_fallback(self):
        cfg,self_user=_runtime()
        proc=EventProcessor(cfg,self_user,mode="shadow",llm_resolver=JevV2Resolver(new_policy=True,transport=lambda req,to:_policy_answer(req,scope=.64,follow=.93)),existing_actions=[],business_candidate_query_complete=True)
        msg=_event("请各工班核对危险废物清单并更新联单信息。")
        msg["message_id"]="hazard-owner-unavailable"
        plan=proc.process_batch([msg])[0]
        self.assertEqual(plan["decision"],"REVIEW")
        self.assertEqual(plan["reason"],"confirmed_area_handover_unverified")
        self.assertIn("current_area_owner",plan["missing_fields"])

    def test_verified_hazard_handover_overrides_legacy_owner_and_needs_no_legacy_config(self):
        cfg,self_user=_runtime()
        cfg["responsibility_rules"].pop("hazardous_waste", None)
        cfg["config_area_duty"] = [
            {"area_name":"杂品库","staff_name":"杨梦卓","cycle_start":"2026-01-01","cycle_end":"2026-12-31","reviewed":1},
            {"area_name":"大件库区","staff_name":"谭继衡","cycle_start":"2026-01-01","cycle_end":"2026-12-31","reviewed":1},
            {"area_name":"材料棚及办公用房","staff_name":"张志斌","cycle_start":"2026-01-01","cycle_end":"2026-12-31","reviewed":1},
            {"area_name":"中小件区","staff_name":"杨梦卓","cycle_start":"2026-01-01","cycle_end":"2026-12-31","reviewed":1},
        ]
        cfg["handover_evidence"] = _synthetic_junk_handover()
        proc=EventProcessor(cfg,self_user,mode="shadow",llm_resolver=JevV2Resolver(new_policy=True,transport=lambda req,to:_policy_answer(req,scope=.64,follow=.93)),existing_actions=[],business_candidate_query_complete=True)
        msg=_event("请各工班核对危险废物清单并更新联单信息。")
        msg["message_id"]="verified-hazard-handover"
        plan=proc.process_batch([msg])[0]
        self.assertEqual(plan["decision"],"CREATE")
        self.assertEqual(plan["route_owner"],"苗笑天")
        self.assertEqual(plan["assignment_audit"]["conflict_resolution"],"verified_handover_evidence_has_priority_over_legacy_area_config")

    def test_external_reference_without_source_content_stays_review_with_specific_gap(self):
        cfg,self_user=_runtime()
        proc=EventProcessor(cfg,self_user,mode="shadow",llm_resolver=JevV2Resolver(new_policy=True,transport=lambda req,to:_policy_answer(req,scope=.5,follow=.84)),existing_actions=[],business_candidate_query_complete=True)
        msg=_event("请@李林骁(李林骁)查阅，并在10月13日前完成，具体要求见R列。")
        msg["message_id"]="external-content-reference"
        plan=proc.process_batch([msg])[0]
        self.assertEqual(plan["decision"],"REVIEW")
        self.assertEqual(plan["reason"],"jev_v2_referenced_context_missing")
        self.assertIn("referenced_content",plan["missing_fields"])

    def test_new_policy_redacts_at_prefixed_media_identifier_before_call(self):
        seen=[]
        resolver=JevV2Resolver(new_policy=True,transport=lambda req,to: seen.append(req) or _policy_answer(req))
        message=_event("[图片消息](mediaId=@lQLPKdIJAP-B6WnNAubNAmCwmJI_6PotljgKjawYEkt7AA)危险废物清单已更新。")
        result=resolver.resolve(message,{"decision":"REVIEW"})
        self.assertEqual(result["applies_to_workgroup"],0.99)
        self.assertEqual(len(seen),1)
        self.assertNotIn("lQLPKdIJAP-B6WnNAubNAmCwmJI_6PotljgKjawYEkt7AA",json.dumps(seen[0],ensure_ascii=False))

    def test_hazardous_followup_with_competing_assessment_cutoff_stays_review(self):
        cfg, self_user = _runtime()
        cfg["responsibility_rules"]["hazardous_waste"]="杨梦卓"
        cfg["config_area_duty"] = [
            {"area_name":"杂品库","staff_name":"杨梦卓","cycle_start":"2026-09-01","cycle_end":"2026-10-31","reviewed":1},
            {"area_name":"大件库区","staff_name":"谭继衡","cycle_start":"2026-09-01","cycle_end":"2026-10-31","reviewed":1},
            {"area_name":"材料棚及办公用房","staff_name":"张志斌","cycle_start":"2026-09-01","cycle_end":"2026-10-31","reviewed":1},
            {"area_name":"中小件区","staff_name":"杨梦卓","cycle_start":"2026-09-01","cycle_end":"2026-10-31","reviewed":1},
        ]
        cfg["handover_evidence"] = _synthetic_junk_handover()
        def transport(request, timeout):
            return _policy_answer(request, scope=0.64, follow=0.93)
        resolver=JevV2Resolver(new_policy=True,transport=transport)
        proc=EventProcessor(cfg,self_user,mode="shadow",llm_resolver=resolver,existing_actions=[],business_candidate_query_complete=True)
        source=_event("请各工班今日下班前对危险废物嘉祥处置联单信息进行重新录入更新。4点30分仍未更新的工班，将纳入本月考核项。；空容器、滤芯第5次处置联单编号已列明。")
        source["message_id"]="hazardous-compound"
        plan=proc.process_batch([source])[0]
        self.assertEqual(plan["decision"],"REVIEW")
        self.assertEqual(plan["reason"],"jev_v2_competing_deadline_context")
        self.assertEqual(plan["route_owner"],"苗笑天")
        self.assertEqual(plan.get("due_date"), None)
        self.assertEqual(plan.get("due_iso"), None)
        self.assertEqual(plan["semantic_evidence"]["needs_follow_up"], 0.93)
        self.assertIn("嘉祥处置联单",plan["detail"])

    def test_long_title_compiler_injected_real_processor_positive(self):
        cfg, self_user = _runtime()
        cfg["responsibility_rules"]["hazardous_waste"] = "杨梦卓"
        cfg["handover_evidence"] = _synthetic_junk_handover()
        source = _event("请各工班及时在系统中录入危险废物处置联单信息并完成复核，逐项确认本年度所有历史处置记录及系统台账内容准确完整，确保现场涉及的空容器、废弃滤芯和相关包装物信息均按要求登记并同步反馈，避免遗漏任何需要持续跟进的事项。")
        semantic = JevV2Resolver(new_policy=True, transport=lambda req, timeout: _policy_answer(req, scope=.64, follow=.93))
        calls = []
        compiler = type("Compiler", (), {"resolve": staticmethod(lambda event, plan: calls.append((event, plan)) or {"title": "录入危废处置联单信息", "evidence_spans": ["请各工班及时在系统中录入危险废物处置联单信息并完成复核"]})})()
        plain = EventProcessor(cfg, self_user, mode="shadow", llm_resolver=semantic, existing_actions=[], business_candidate_query_complete=True).process_batch([source])[0]
        self.assertEqual(plain["decision"], "REVIEW")
        proc = EventProcessor(cfg, self_user, mode="shadow", llm_resolver=semantic, title_resolver=compiler, existing_actions=[], business_candidate_query_complete=True)
        result = proc.process_batch([source])[0]
        self.assertEqual(len(calls), 1)
        self.assertEqual(result["decision"], "CREATE")
        self.assertEqual(result["title"], "录入危废处置联单信息")
        self.assertEqual(result["route_owner"], "苗笑天")
        self.assertIn("semantic_evidence", result)

    def test_long_title_compiler_rejects_extra_fields_invalid_citation_and_exception(self):
        cfg, self_user = _runtime()
        cfg["responsibility_rules"]["hazardous_waste"] = "杨梦卓"
        cfg["handover_evidence"] = _synthetic_junk_handover()
        source = _event("请各工班及时在系统中录入危险废物处置联单信息并完成复核，逐项确认本年度所有历史处置记录及系统台账内容准确完整，确保现场涉及的空容器、废弃滤芯和相关包装物信息均按要求登记并同步反馈，避免遗漏任何需要持续跟进的事项。")
        source["message_id"] = "long-title-compile-reject"
        for answer in ({"title": "录入危废处置联单信息", "evidence_spans": ["请各工班及时在系统中录入危险废物处置联单信息"], "owner": "杨梦卓"}, {"title": "录入危废处置联单信息", "evidence_spans": ["not source text"]}, None):
            calls = []
            def compile_title(event, plan, answer=answer):
                calls.append(1)
                if answer is None:
                    raise RuntimeError("compiler unavailable")
                return answer
            compiler = type("Compiler", (), {"resolve": staticmethod(compile_title)})()
            proc = EventProcessor(cfg, self_user, mode="shadow", llm_resolver=JevV2Resolver(new_policy=True, transport=lambda req, timeout: _policy_answer(req, scope=.64, follow=.93)), title_resolver=compiler, existing_actions=[], business_candidate_query_complete=True)
            result = proc.process_batch([source])[0]
            self.assertEqual(result["decision"], "REVIEW")
            self.assertEqual(len(calls), 1)

    def test_new_policy_retains_safe_compound_semantics_instead_of_blanket_review(self):
        cfg,self_user=_runtime()
        cfg["responsibility_rules"]["hazardous_waste"]="杨梦卓"
        cfg["handover_evidence"]=_synthetic_junk_handover()
        proc=EventProcessor(cfg,self_user,mode="shadow",llm_resolver=JevV2Resolver(new_policy=True,transport=lambda req,to:_policy_answer(req)),existing_actions=[],business_candidate_query_complete=True)
        source=_event("请各工班完成危险废物清单核对；依据清单更新系统记录。")
        source["message_id"]="compound-policy"
        plan=proc.process_batch([source])[0]
        self.assertNotEqual(plan.get("reason"),"compound_requires_split")
        self.assertEqual(plan["decision"],"CREATE")

    def test_new_policy_resolves_unscoped_notice_and_no_deadline_without_model_owner(self):
        seen = {}
        def transport(request, timeout):
            seen.update(request=request)
            questions = request["questions"]
            return _policy_answer(request)
        cfg, self_user = _runtime()
        cfg["config_area_duty"] = [{"area_name":"杂品库", "staff_name":"杨梦卓", "cycle_start":"2026-09-01", "cycle_end":"2026-10-31", "reviewed":1}]
        proc = EventProcessor(cfg, self_user, mode="shadow", llm_resolver=JevV2Resolver(transport=transport, new_policy=True),
                              existing_actions=[], business_candidate_query_complete=True)
        plan = proc.process_batch([_event("通知：终止汛期防汛值班安排，请知悉。")])[0]
        self.assertEqual(proc.stats["llm_calls"], 1)
        self.assertEqual(plan["decision"], "CREATE")
        self.assertEqual(plan["route_owner"], "李林骁")
        self.assertNotIn("owner", seen["request"]["questions"])

    def test_new_policy_negative_scope_is_ignore_and_hazardous_owner_is_configured(self):
        calls=[]
        def transport(request, timeout):
            calls.append(request)
            return _policy_answer(request,scope=0.01)
        cfg,self_user=_runtime()
        cfg["responsibility_rules"]["hazardous_waste"]="杨梦卓"
        cfg["config_area_duty"]=[{"area_name":"杂品库","staff_name":"杨梦卓","cycle_start":"2026-09-01","cycle_end":"2026-10-31","reviewed":1}]
        cfg["handover_evidence"]=_synthetic_junk_handover()
        proc=EventProcessor(cfg,self_user,mode="shadow",llm_resolver=JevV2Resolver(transport=transport,new_policy=True),existing_actions=[],business_candidate_query_complete=True)
        neg=proc.process_batch([_event("科室内勤请整理科室本周值班表。")])[0]
        self.assertEqual(neg["decision"],"IGNORE")
        hazardous = _event("请更新危险废物联单系统记录。")
        hazardous["message_id"] = "hazardous"
        pos=proc.process_batch([hazardous])[0]
        self.assertEqual(pos["route_owner"],"苗笑天")
        self.assertEqual(len(calls),2)

    def test_clause_choice_maps_to_full_local_original_and_compound_requires_review(self):
        source = "请持续推进工作，核对库存并完成消防检查。"
        captured = {}
        def transport(request, timeout):
            captured["request"] = request
            opts = request["questions"]["source_clause"]["criteria"]
            key = next(k for k, v in opts.items() if "核对库存" in v)
            return {"answers": {"prospective_action": {"type":"noul", "noul":0.98},
                "source_clause": {"type":"choice", "choice":key,
                    "probabilities":{k:(0.98 if k == key else 0.01) for k in opts}, "confidence":0.98}}}
        resolver = JevV2Resolver(transport=transport)
        result = resolver.resolve(_event(source), {"decision":"CREATE"})
        self.assertEqual(result["source_clause"], "请持续推进工作，核对库存并完成消防检查。")
        config, self_user = _runtime()
        processor = EventProcessor(config, self_user, mode="test", llm_resolver=resolver,
                                   existing_actions=[], business_candidate_query_complete=True)
        plans = processor.process_batch([_event("核对库存；组织消防检查。")])
        self.assertEqual(plans[0]["decision"], "REVIEW")
        self.assertEqual(plans[0]["reason"], "compound_requires_split")

    def test_multiple_dates_use_choice_and_replan_selected_candidate(self):
        def transport(request, timeout):
            questions = request["questions"]
            opts = questions["source_clause"]["criteria"]
            clause = next(k for k, v in opts.items() if "库存核对工作" in v)
            answers = {"prospective_action": {"type":"noul", "noul":0.98},
                "source_clause": {"type":"choice", "choice":clause,
                    "probabilities":{key:(0.98 if key==clause else 0.005) for key in opts}, "confidence":0.98}}
            if "due_date" in questions:
                opts = questions["due_date"]["criteria"]
                selected = "date_0"
                answers["due_date"] = {"type":"choice", "choice":selected,
                    "probabilities":{key:(0.97 if key == selected else (0.01 if key.startswith("date_") else 0.005)) for key in opts}, "confidence":0.97}
            return {"answers": answers}
        resolver = JevV2Resolver(transport=transport)
        config, self_user = _runtime()
        processor = EventProcessor(config, self_user, mode="test", llm_resolver=resolver,
                                   existing_actions=[], business_candidate_query_complete=True)
        plans=processor.process_batch([_event("请于2026-09-25前完成库存核对工作，并于2026-09-30反馈。")])
        self.assertEqual(plans[0]["decision"], "CREATE")
        self.assertEqual(plans[0]["due_date"], "2026-09-25")

    def test_unresolved_semantics_are_resolved_then_replanned(self):
        calls = []
        def resolve(event, plan, **kwargs):
            calls.append((event, plan, kwargs))
            return {"prospective_action": 0.98}
        resolver = type("Resolver", (), {"resolve": staticmethod(resolve)})()
        def transport(request, timeout):
            questions = request["questions"]
            opts = questions["source_clause"]["criteria"]
            selected_clause = next((k for k, v in opts.items() if "库存核对方案" in v), "none")
            answers = {"prospective_action": {"type": "noul", "noul": 0.98},
                "source_clause": {"type":"choice", "choice":selected_clause,
                    "probabilities":{k:(0.98 if k == selected_clause else 0.005) for k in opts}, "confidence":0.98}}
            if "due_date" in questions:
                options = questions["due_date"]["criteria"]
                answers["due_date"] = {"type": "choice", "choice": "date_0",
                    "probabilities": {key: (0.97 if key == "date_0" else (0.01 if key.startswith("date_") else 0.005)) for key in options}, "confidence": 0.97}
            return {"answers": answers}
        resolver = JevV2Resolver(transport=transport)
        processor = EventProcessor(
            {"config_group": [{"group_name":"物资调配室","conversation_id":"c1","purpose":"todo_scan"}],
             "config_staff": [[1,"李林骁","SELF","leader"],[2,"苗笑天","MIAO","executor"]],
             "active_routing_loaded":True,"responsibility_rules":{"materials":"苗笑天"},"config_area_duty":[{"workgroup":"铁炉西工班","module":"materials","owner":"苗笑天","start_date":"2026-01-01","end_date":"2026-12-31"}]},
            {"name":"李林骁","user_id":"SELF"}, mode="test", llm_resolver=resolver,
            existing_actions=[], business_candidate_query_complete=True)
        plans = processor.process_batch([{"conversation_id":"c1","message_id":"m1","group":"物资调配室",
            "sender":"王萌","sender_id":"OTHER","create_time":"2026-09-21 10:00:00",
            "text":"请于2026-09-30前完成库存核对方案整理。"}])
        self.assertEqual(processor.stats["llm_calls"], 1)
        self.assertEqual(plans[0]["decision"], "CREATE")

    def test_explicit_self_assignment_is_not_blocked_by_assignment_target(self):
        resolver = JevV2Resolver(transport=lambda req, timeout: {"answers": {"prospective_action": {"type": "noul", "noul": 0.98}}})
        processor = EventProcessor(
            {"config_group": [{"group_name":"物资调配室","conversation_id":"c1","purpose":"todo_scan"}],
             "config_staff": [[1,"李林骁","SELF","leader"],[2,"苗笑天","MIAO","executor"]],
             "active_routing_loaded":True,"responsibility_rules":{"materials":"苗笑天"},"config_area_duty":[{"workgroup":"铁炉西工班","module":"materials","owner":"苗笑天","start_date":"2026-01-01","end_date":"2026-12-31"}]},
            {"name":"李林骁","user_id":"SELF"}, mode="test", llm_resolver=resolver,
            existing_actions=[], business_candidate_query_complete=True)
        plans = processor.process_batch([{"conversation_id":"c1","message_id":"m2","group":"物资调配室",
            "sender":"李林骁","sender_id":"SELF","create_time":"2026-09-21 10:00:00",
            "text":"@李林骁 请于2026-09-30前核对库存。"}])
        self.assertEqual(plans[0]["decision"], "REVIEW")

    def test_complete_duplicate_query_with_no_candidates_can_create(self):
        def transport(request, timeout):
            questions = request["questions"]
            opts = questions["source_clause"]["criteria"]
            selected_clause = next((k for k, v in opts.items() if "库存核对方案" in v), "none")
            answers = {"prospective_action": {"type": "noul", "noul": 0.98},
                "source_clause": {"type":"choice", "choice":selected_clause,
                    "probabilities":{k:(0.98 if k == selected_clause else 0.005) for k in opts}, "confidence":0.98}}
            if "due_date" in questions:
                options = questions["due_date"]["criteria"]
                answers["due_date"] = {"type": "choice", "choice": "date_0",
                    "probabilities": {key: (0.97 if key == "date_0" else (0.01 if key.startswith("date_") else 0.005)) for key in options}, "confidence": 0.97}
            return {"answers": answers}
        config, self_user = _runtime()
        processor = EventProcessor(config, self_user, mode="test", llm_resolver=JevV2Resolver(transport=transport), existing_actions=[], business_candidate_query_complete=True)
        plans = processor.process_batch([_event("请于2026-09-30前完成库存核对工作。")])
        self.assertEqual(plans[0]["decision"], "CREATE")
        self.assertEqual(plans[0]["route_owner"], "苗笑天")
        self.assertFalse(processor.allow_external_writes)

    def test_incomplete_duplicate_query_reviews(self):
        resolver = JevV2Resolver(transport=lambda *_: self.fail("should not call model"))
        processor = EventProcessor(
            {"config_group": [{"group_name":"物资调配室", "conversation_id":"c1", "purpose":"todo_scan"}],
             "config_staff": [[1,"李林骁","SELF","leader"],[2,"苗笑天","MIAO","executor"]],
             "active_routing_loaded":True,"responsibility_rules":{"materials":"苗笑天"},"config_area_duty":[{"workgroup":"铁炉西工班","module":"materials","owner":"苗笑天","start_date":"2026-01-01","end_date":"2026-12-31"}]},
            {"name":"李林骁","user_id":"SELF"}, mode="test", llm_resolver=resolver,
            existing_actions=[], business_candidate_query_complete=False)
        plans = processor.process_batch([{"conversation_id":"c1","message_id":"m1","group":"物资调配室",
            "sender":"王萌","sender_id":"OTHER","create_time":"2026-09-21 10:00:00","text":"请完成库存核对工作。"}])
        self.assertEqual(plans[0]["decision"], "REVIEW")
