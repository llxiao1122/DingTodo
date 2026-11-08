import json
import subprocess
import sys
import unittest
from datetime import datetime
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dws_adapter import (  # noqa: E402
    DwsClient,
    attachment_is_present,
    build_add_attachment_args,
    build_download_resource_args,
)
from dws_parity import DwsParityLedger  # noqa: E402
import event_worker  # noqa: E402
from event_worker import (  # noqa: E402
    EventProcessor,
    configure_writer_environment,
    within_execution_window,
    dedupe_events,
    load_existing_actions,
    load_business_candidate_context,
    main,
    normalize_event,
    should_process_group,
)
import llm_adapter  # noqa: E402
from llm_adapter import CommandLlmResolver, LlmError, validate_llm_result  # noqa: E402


class EventWorkerTests(unittest.TestCase):
    def test_official_comment_is_linked_before_write_and_keeps_exact_source_readback(self):
        content = "来源群：铁炉西工班群\n原文：请于10月10日前完成盘点。"
        task_detail = {
            "taskId": "remote-1",
            "subject": "完成盘点",
            "isDone": False,
            "executorInfos": [{"name": "谭继衡", "userId": "owner"}],
            "priority": 20,
        }
        sequence = []

        def fake_run(command, **kwargs):
            sequence.append(("command", command))
            if command[1:4] == ["todo", "task", "create"]:
                output = {"data": {"taskId": "remote-1"}}
            elif command[1:3] == ["todo", "+comment"]:
                output = {"ok": True, "outcome": "success", "data": {
                    "taskId": "remote-1", "commentId": "comment-1",
                    "content": content, "verified": True,
                }}
            elif command[1:4] == ["todo", "comment", "list"]:
                output = {"data": {"comments": [{"content": content}]}}
            elif command[1:4] == ["todo", "task", "get"]:
                output = {"data": {"todoDetailModel": task_detail}}
            else:
                raise AssertionError(f"unexpected DWS command: {command[1:]}")
            return subprocess.CompletedProcess(command, 0, json.dumps(output, ensure_ascii=False), "")

        processor = EventProcessor(
            {"config_group": [], "config_staff": [], "active_routing_loaded": True},
            {"name": "李林骁", "user_id": "SELF"},
            mode="production",
            dws=DwsClient(executable="dws-test", confirmed_official_writes=True),
        )
        plan = {
            "decision": "CREATE", "title": "完成盘点", "executor_ids": ["owner"],
            "executor_names": ["谭继衡"], "priority": 20, "comment": content,
        }
        event = {"conversation_id": "conv-1", "message_id": "msg-1"}

        with patch("dws_adapter.subprocess.run", side_effect=fake_run), patch.object(processor, "_persist_action_status"):
            task_id = processor._write_external(
                event, plan,
                action_id=42, claim_acquired=True, test_only=False,
                on_created=lambda value: sequence.append(("link", value)),
            )

        command_rows = [row[1] for row in sequence if row[0] == "command"]
        comment_position = next(i for i, row in enumerate(sequence) if row[0] == "command" and row[1][2] == "+comment")
        link_position = next(i for i, row in enumerate(sequence) if row[0] == "link")
        self.assertEqual(task_id, "remote-1")
        self.assertLess(link_position, comment_position)
        self.assertEqual(
            command_rows[1],
            ["dws-test", "todo", "+comment", "--task-id", "remote-1", "--content", content, "--yes", "--format", "json"],
        )
        self.assertEqual(command_rows[2][1:4], ["todo", "comment", "list"])
        self.assertEqual(command_rows[-1][1:4], ["todo", "task", "get"])

    def test_comment_timeout_after_link_never_recreates_task_on_restart(self):
        class FakeWriter:
            def __init__(self):
                self.claimed = False
                self.requests = []

            def execute(self, request):
                self.requests.append(dict(request))
                if request["operation"] == "claim_action":
                    if self.claimed:
                        return {"created": False, "action_id": 9, "reason": "source_exists"}
                    self.claimed = True
                    return {"created": True, "action_id": 9}
                return {"ok": True, "committed": True, "action_id": request.get("action_id"), "row": {"status": request.get("status"), "external_task_id": request.get("external_task_id")}}

        task_create_calls = 0
        comment_calls = 0

        def fake_run(command, **kwargs):
            nonlocal task_create_calls, comment_calls
            if command[1:4] == ["contact", "user", "search"]:
                output = {"result": {"data": [{"name": "谭继衡", "userId": "LIVE-TAN"}]}}
                return subprocess.CompletedProcess(command, 0, json.dumps(output, ensure_ascii=False), "")
            if command[1:3] == ["todo", "+search"]:
                output = {
                    "ok": True,
                    "outcome": "success",
                    "data": {"complete": True, "count": 0, "todos": []},
                }
                return subprocess.CompletedProcess(command, 0, json.dumps(output), "")
            if command[1:4] == ["todo", "task", "create"]:
                task_create_calls += 1
                output = {"result": {"data": {"taskId": "remote-1"}}}
                return subprocess.CompletedProcess(command, 0, json.dumps(output), "")
            if command[1:3] == ["todo", "+comment"]:
                comment_calls += 1
                raise subprocess.TimeoutExpired(command, timeout=60)
            raise AssertionError(f"unexpected DWS command: {command[1:]}")

        runtime = {
            "config_group": [{"group_name": "铁炉西工班群", "conversation_id": "conv-1", "purpose": "todo_scan"}],
            "config_staff": [[1, "李林骁", "SELF", "leader"], [2, "谭继衡", "TAN", "executor"]],
            "active_routing_loaded": True,
            "responsibility_rules": {"hazardous_waste": "谭继衡"},
            "handover_evidence": [{
                "period": "2026-09", "area_name": "杂品库", "active_owners": ["谭继衡"],
                "reviewed": True, "status": "accepted", "hygiene_review": "passed",
                "source": "dws_readback", "evidence_id": "synthetic-timeout-junk-handover",
            }],
        }
        writer = FakeWriter()
        dws = DwsClient(executable="dws-test", confirmed_official_writes=True)
        event = {
            "conversationId": "conv-1", "messageId": "msg-comment-timeout",
            "createTime": "2026-09-21 10:00:00", "text": "请于9月22日完成危废台账核对整改。",
        }
        with patch("dws_adapter.subprocess.run", side_effect=fake_run):
            first = EventProcessor(
                runtime, {"name": "李林骁", "user_id": "SELF"}, mode="production",
                write_ledger=True, allow_external_writes=True, dws=dws, writer=writer,
            ).process_batch([event])
            second = EventProcessor(
                runtime, {"name": "李林骁", "user_id": "SELF"}, mode="production",
                write_ledger=True, allow_external_writes=True, dws=dws, writer=writer,
            ).process_batch([event])

        operations = [row["operation"] for row in writer.requests]
        self.assertEqual(first[0]["decision"], "REVIEW")
        self.assertEqual(second[0]["decision"], "CREATE")
        self.assertEqual(task_create_calls, 1)
        self.assertEqual(comment_calls, 1)
        self.assertIn("link_external_task", operations)
        self.assertEqual(
            [row["status"] for row in writer.requests if row["operation"] == "link_external_task"],
            ["EXTERNAL_CREATED"],
        )

    def test_comment_readback_failure_after_link_never_recreates_task_on_restart(self):
        class FakeWriter:
            def __init__(self):
                self.claimed = False
                self.requests = []

            def execute(self, request):
                self.requests.append(dict(request))
                if request["operation"] == "claim_action":
                    if self.claimed:
                        return {"created": False, "action_id": 10, "reason": "source_exists"}
                    self.claimed = True
                    return {"created": True, "action_id": 10}
                return {"ok": True, "committed": True, "action_id": request.get("action_id"), "row": {"status": request.get("status"), "external_task_id": request.get("external_task_id")}}

        task_create_calls = 0
        comment_calls = 0
        comment_readback_calls = 0

        def fake_run(command, **kwargs):
            nonlocal task_create_calls, comment_calls, comment_readback_calls
            if command[1:4] == ["contact", "user", "search"]:
                output = {"result": {"data": [{"name": "谭继衡", "userId": "LIVE-TAN"}]}}
            elif command[1:3] == ["todo", "+search"]:
                output = {
                    "ok": True,
                    "outcome": "success",
                    "data": {"complete": True, "count": 0, "todos": []},
                }
            elif command[1:4] == ["todo", "task", "create"]:
                task_create_calls += 1
                output = {"result": {"data": {"taskId": "remote-2"}}}
            elif command[1:3] == ["todo", "+comment"]:
                comment_calls += 1
                content = command[command.index("--content") + 1]
                output = {"ok": True, "outcome": "success", "data": {
                    "taskId": "remote-2", "commentId": "comment-2",
                    "content": content,
                    "verified": True,
                }}
            elif command[1:4] == ["todo", "comment", "list"]:
                comment_readback_calls += 1
                output = {"data": {"comments": []}}
            else:
                raise AssertionError(f"unexpected DWS command: {command[1:]}")
            return subprocess.CompletedProcess(command, 0, json.dumps(output, ensure_ascii=False), "")

        runtime = {
            "config_group": [{"group_name": "铁炉西工班群", "conversation_id": "conv-1", "purpose": "todo_scan"}],
            "config_staff": [[1, "李林骁", "SELF", "leader"], [2, "谭继衡", "TAN", "executor"]],
            "active_routing_loaded": True,
            "responsibility_rules": {"hazardous_waste": "谭继衡"},
            "handover_evidence": [{
                "period": "2026-09", "area_name": "杂品库", "active_owners": ["谭继衡"],
                "reviewed": True, "status": "accepted", "hygiene_review": "passed",
                "source": "dws_readback", "evidence_id": "synthetic-readback-failure-handover",
            }],
        }
        writer = FakeWriter()
        dws = DwsClient(executable="dws-test", confirmed_official_writes=True)
        event = {
            "conversationId": "conv-1", "messageId": "msg-comment-readback-failure",
            "createTime": "2026-09-21 10:00:00", "text": "请于9月22日完成危废台账核对整改。",
        }
        with patch("dws_adapter.subprocess.run", side_effect=fake_run):
            first = EventProcessor(
                runtime, {"name": "李林骁", "user_id": "SELF"}, mode="production",
                write_ledger=True, allow_external_writes=True, dws=dws, writer=writer,
            ).process_batch([event])
            second = EventProcessor(
                runtime, {"name": "李林骁", "user_id": "SELF"}, mode="production",
                write_ledger=True, allow_external_writes=True, dws=dws, writer=writer,
            ).process_batch([event])

        self.assertEqual(first[0]["decision"], "REVIEW")
        self.assertEqual(second[0]["decision"], "CREATE")
        self.assertEqual(task_create_calls, 1)
        self.assertEqual(comment_calls, 1)
        self.assertEqual(comment_readback_calls, 1)

    def test_date_roles_fail_closed_for_mixed_effective_and_deadline_dates(self):
        from datetime import date
        from routing import extract_due_candidates
        candidates = extract_due_candidates(
            "自10月1日8时起暂停某项值守，请9月30日17时前反馈调整安排",
            date(2026, 10, 7), include_context=True,
        )
        self.assertEqual([c["role"] for c in candidates], ["unknown", "unknown"])
        self.assertEqual([c["deadline_context"] for c in candidates], [False, False])

    def test_completed_historical_date_is_not_a_deadline_but_today_deadline_still_is(self):
        from datetime import date
        from routing import extract_due_candidates
        history = extract_due_candidates("2026年10月6日已完成处置", date(2026, 10, 7), include_context=True)
        today = extract_due_candidates("今天11点前完成检查", date(2026, 10, 7), include_context=True)
        self.assertEqual(history[0]["role"], "unknown")
        self.assertFalse(history[0]["deadline_context"])
        self.assertEqual((today[0]["role"], today[0]["due_date"]), ("deadline", "2026-10-07"))

    def test_processor_jev_v2_unknown_historical_date_review_not_create(self):
        from jev_v2 import JevV2Resolver
        def transport(payload, timeout):
            state = json.loads(payload["state"])
            self.assertEqual(state["date_candidates"][0]["due_date"], "2026-10-06")
            answers = {"prospective_action": {"type":"noul","noul":0.9},
                "applies_to_workgroup": {"type":"noul","noul":0.9},
                "needs_follow_up": {"type":"noul","noul":0.9}}
            for key, question in payload["questions"].items():
                if question["type"] == "choice":
                    options = question["criteria"]
                    if key.startswith("date_role_"):
                        candidate_key = key.removeprefix("date_role_")
                        next(row for row in state["date_role_candidates"] if row["key"] == candidate_key)
                        choice = "unknown"
                    else:
                        choice = "date_0" if "date_0" in options else "none"
                    answers[key] = {"type":"choice", "choice":choice,
                        "probabilities":{option:(0.9 if option == choice else 0.1/(len(options)-1)) for option in options},
                        "confidence":0.9}
            return {"answers": answers}
        config={"config_group":[{"group_name":"铁炉西工班群","conversation_id":"c","purpose":"todo_scan"}],"config_staff":[[1,"李林骁","SELF","leader"],[2,"苗笑天","MIAO","executor"]],"active_routing_loaded":True,"responsibility_rules":{"materials":"苗笑天"}}
        resolver=JevV2Resolver(api_key="fake", transport=transport, new_policy=True)
        processor=EventProcessor(config,{"name":"李林骁","user_id":"SELF"},llm_resolver=resolver,business_candidate_query_complete=True,semantic_fallback=None)
        result=processor.process_batch([{"conversation_id":"c","message_id":"mixed","create_time":"2026-10-07 09:00:00","text":"2026年10月6日已完成物资处置。"}])[0]
        self.assertNotEqual(result["decision"],"CREATE")

    def test_new_policy_title_uses_later_explicit_request_and_preserves_full_detail(self):
        text = "2024年3月1日已完成危废转运。请各工班及时在系统中录入联单信息。"
        from routing import plan_message
        plan = plan_message({"_jev_v2_applicable": True, "text": text, "group": "铁炉西工班", "message_id": "m", "conversation_id": "c"}, {}, {"name":"李林骁"}, run_date=__import__('datetime').date(2026, 10, 7))
        self.assertEqual(plan["title"], "请各工班及时在系统中录入联单信息")
        self.assertIn(text, plan["detail"])
        self.assertNotIn("title_needs_llm", plan["warnings"])

    def test_verified_self_notice_is_ignored_before_llm_or_fallback(self):
        from jev_v2 import JevV2Resolver
        resolver_calls = []
        class CountingFallback:
            def __init__(self): self.calls = []
            def resolve_context(self, *args, **kwargs): self.calls.append((args, kwargs))
        fallback = CountingFallback()
        resolver = JevV2Resolver(new_policy=True, transport=lambda *a, **k: (resolver_calls.append((a, k)) or {}))
        processor = EventProcessor(
            {"config_group": [{"conversation_id": "c", "group_name": "g", "purpose": "todo_scan"}]},
            {"name": "李林骁", "user_id": "contact-self", "message_sender_id": "sender-verified"},
            llm_resolver=resolver,
            semantic_fallback=fallback,
        )
        result = processor.process_batch([{
            "conversation_id": "c", "message_id": "m", "create_time": "2026-10-07 09:02:00",
            "sender": "李林骁", "sender_id": "sender-verified",
            "text": "@所有人 请各工班今天完成设备巡检并反馈。",
        }])
        self.assertEqual(result[0]["decision"], "IGNORE")
        self.assertEqual(result[0]["reason"], "self_authored_notice")
        self.assertEqual(resolver_calls, [])
        self.assertEqual(fallback.calls, [])

    def test_arrived_verified_self_notice_chain_ignores_mentions_and_matching_file_without_models(self):
        from jev_v2 import JevV2Resolver

        model_calls = []
        resolver = JevV2Resolver(
            new_policy=True,
            transport=lambda *args, **kwargs: (model_calls.append((args, kwargs)) or {}),
        )

        class CountingFallback:
            def __init__(self):
                self.calls = 0

            def resolve_context(self, *args, **kwargs):
                self.calls += 1
                raise AssertionError("hard-ignored source reached semantic fallback")

        class CountingTitle:
            def __init__(self):
                self.calls = 0

            def resolve(self, *args, **kwargs):
                self.calls += 1
                raise AssertionError("hard-ignored source reached title resolver")

        class CountingDateRole:
            def __init__(self):
                self.calls = 0

            def resolve_roles(self, *args, **kwargs):
                self.calls += 1
                raise AssertionError("hard-ignored source reached date-role fallback")

        fallback = CountingFallback()
        title = CountingTitle()
        date_role = CountingDateRole()
        processor = EventProcessor(
            {"config_group": [{"conversation_id": "c", "group_name": "g", "purpose": "todo_scan"}]},
            {"name": "Self", "user_id": "contact-self", "message_sender_id": "message-self"},
            llm_resolver=resolver,
            semantic_fallback=fallback,
            title_resolver=title,
            date_role_fallback_resolver=date_role,
            business_candidate_query_complete=True,
            source_order_provenance="complete_dws_chronological_page",
        )
        common = {
            "conversation_id": "c",
            "create_time": "2026-10-07 09:02:18",
            "sender": "Self",
            "sender_id": "message-self",
        }
        plans = processor.process_batch([
            {
                **common,
                "message_id": "z-notice",
                "text": "@所有人 请大家针对设备点检方案提出意见。",
            },
            {**common, "message_id": "m-mentions", "text": "@甲 @乙"},
            {
                **common,
                "message_id": "a-file",
                "text": "[文件]设备点检方案.docx",
                "resource_refs": [{"name": "设备点检方案.docx", "resourceId": "local-resource"}],
            },
        ])

        self.assertEqual([plan["decision"] for plan in plans], ["IGNORE", "IGNORE", "IGNORE"])
        self.assertEqual([plan["reason"] for plan in plans], [
            "self_authored_notice",
            "self_authored_pure_person_mention",
            "self_notice_supporting_artifact",
        ])
        self.assertEqual(model_calls, [])
        self.assertEqual(fallback.calls, 0)
        self.assertEqual(title.calls, 0)
        self.assertEqual(date_role.calls, 0)
        self.assertEqual(processor.stats["llm_calls"], 0)
        self.assertEqual(processor.stats["semantic_fallback_calls"], 0)
        self.assertEqual(processor.stats["title_field_calls"], 0)
        self.assertEqual(processor.stats["date_role_fallback_calls"], 0)

    def test_self_notice_artifact_requires_verified_same_author_group_and_title_relation(self):
        from jev_v2 import JevV2Resolver

        calls = []
        resolver = JevV2Resolver(
            new_policy=True,
            transport=lambda payload, *args, **kwargs: (calls.append(json.dumps(payload, ensure_ascii=False)) or {}),
        )
        processor = EventProcessor(
            {"config_group": [
                {"conversation_id": "c", "group_name": "g", "purpose": "todo_scan"},
                {"conversation_id": "other", "group_name": "g", "purpose": "todo_scan"},
            ]},
            {"name": "Self", "user_id": "contact-self", "message_sender_id": "message-self"},
            llm_resolver=resolver,
            business_candidate_query_complete=True,
            source_order_provenance="complete_dws_chronological_page",
        )

        def notice(message_id="notice", conversation_id="c"):
            return {
                "conversation_id": conversation_id,
                "message_id": message_id,
                "create_time": "2026-10-07 09:00:00",
                "sender_id": "message-self",
                "sender": "Self",
                "text": "@所有人 请大家针对设备点检方案提出意见。 ARRIVED_NOTICE_LOCAL_SENTINEL",
            }

        def attachment(message_id, *, conversation_id="c", sender_id="message-self", filename="设备点检方案.docx", sender="Self"):
            return {
                "conversation_id": conversation_id,
                "message_id": message_id,
                "create_time": "2026-10-07 09:01:00",
                "sender_id": sender_id,
                "sender": sender,
                "text": f"[文件]{filename}",
                "resource_refs": [{"name": filename, "resourceId": "local-resource"}],
            }

        plans = processor.process_batch([
            notice(),
            attachment("other-author", sender_id="someone-else"),
            attachment("other-group", conversation_id="other"),
            attachment("unrelated-title", filename="值班名单.docx"),
            attachment("unverified-name", sender_id=None),
        ])

        self.assertEqual(plans[0]["reason"], "self_authored_notice")
        self.assertTrue(all(plan["reason"] != "self_notice_supporting_artifact" for plan in plans[1:]))
        self.assertTrue(all(plan["decision"] != "IGNORE" for plan in plans[1:]))
        self.assertGreater(len(calls), 0)
        self.assertTrue(all("message-self" not in payload for payload in calls))
        self.assertTrue(all("ARRIVED_NOTICE_LOCAL_SENTINEL" not in payload for payload in calls))

    def test_explicit_reply_to_prior_self_notice_can_support_an_attachment_without_title_match(self):
        from jev_v2 import JevV2Resolver

        calls = []
        processor = EventProcessor(
            {"config_group": [{"conversation_id": "c", "group_name": "g", "purpose": "todo_scan"}]},
            {"name": "Self", "user_id": "contact-self", "message_sender_id": "message-self"},
            llm_resolver=JevV2Resolver(new_policy=True, transport=lambda *args, **kwargs: (calls.append(1) or {})),
            business_candidate_query_complete=True,
            source_order_provenance="complete_dws_chronological_page",
        )
        plans = processor.process_batch([
            {
                "conversation_id": "c", "message_id": "notice", "create_time": "2026-10-07 09:00:00",
                "sender_id": "message-self", "text": "@所有人 请大家提出意见。",
            },
            {
                "conversation_id": "c", "message_id": "file", "create_time": "2026-10-07 09:01:00",
                "sender_id": "message-self", "reply_to_message_id": "notice",
                "text": "[文件]不相关的文档.docx",
                "resource_refs": [{"name": "不相关的文档.docx", "resourceId": "local-resource"}],
            },
        ])

        self.assertEqual(plans[1]["decision"], "IGNORE")
        self.assertEqual(plans[1]["reason"], "self_notice_supporting_artifact")
        self.assertEqual(calls, [])

    def test_normalize_flattened_group_message_preserves_image_refs(self):
        event = normalize_event(
            {
                "eventType": "user_im_message_receive_group_all",
                "conversationId": "conv-1",
                "messageId": "msg-1",
                "createTime": "2026-09-21 10:00:00",
                "sender": "李林骁",
                "senderId": "user-1",
                "text": "请核对库存",
                "atUsers": [{"name": "李林骁", "userId": "self-id"}],
                "resourceRefs": [{"resourceId": "media-1", "resourceIdType": "mediaId"}],
            }
        )
        self.assertEqual(event["conversation_id"], "conv-1")
        self.assertEqual(event["message_id"], "msg-1")
        self.assertEqual(event["at_users"][0]["name"], "李林骁")
        self.assertEqual(event["resource_refs"][0]["resourceId"], "media-1")

    def test_normalize_transport_envelope_unwraps_message(self):
        event = normalize_event(
            {
                "type": "event",
                "event_type": "user_im_message_receive_group_all",
                "data": {
                    "conversationId": "conv-2",
                    "messageId": "msg-2",
                    "createTime": "2026-09-21T10:01:00+08:00",
                    "sender": {"name": "王萌"},
                    "text": "通知：请反馈",
                },
            }
        )
        self.assertEqual(event["conversation_id"], "conv-2")
        self.assertEqual(event["sender"], "王萌")

    def test_normalize_dws_flattened_schema_uses_content_and_snake_case_ids(self):
        event = normalize_event(
            {
                "type": "user_im_message_receive_group_all",
                "conversation_id": "conv-schema",
                "message_id": "msg-schema",
                "create_time": "2026-09-22 09:00:00",
                "sender": "王萌",
                "sender_open_dingtalk_id": "sender-open-id",
                "content": "请于9月24日完成资料反馈。",
            }
        )
        self.assertEqual(event["conversation_id"], "conv-schema")
        self.assertEqual(event["message_id"], "msg-schema")
        self.assertEqual(event["text"], "请于9月24日完成资料反馈。")
        self.assertEqual(event["sender_id"], "sender-open-id")

    def test_external_todo_writes_require_production_mode_even_with_explicit_flags(self):
        with self.assertRaisesRegex(ValueError, "external_writes_require_production_mode"):
            EventProcessor(
                {},
                {},
                mode="test",
                write_ledger=True,
                allow_external_writes=True,
            )

    def test_event_processor_includes_configured_iron_west_workgroup_in_stream_scope(self):
        runtime = {
            "config_group": [
                {
                    "group_name": "铁炉西工班群",
                    "conversation_id": "cid8QKDfYpg+85AWaskH8XHjQ==",
                    "purpose": "todo_scan",
                },
                {
                    "group_name": "台账更新群",
                    "conversation_id": "allowed-conversation",
                    "purpose": "todo_scan",
                },
            ]
        }
        processor = EventProcessor(runtime, {"name": "李林骁", "user_id": "self"})
        self.assertEqual(processor.group_map["cid8QKDfYpg+85AWaskH8XHjQ=="], "铁炉西工班群")
        self.assertTrue(
            should_process_group(
                "cid8QKDfYpg+85AWaskH8XHjQ==",
                processor.group_map,
            )
        )

    def test_stream_and_parallel_recovery_entrypoints_are_removed(self):
        for name in ("EventSource", "HistoryRecovery", "run_event_loop", "build_listener_command"):
            with self.subTest(name=name):
                self.assertFalse(hasattr(event_worker, name))

    def test_dedupe_events_keeps_first_payload_for_source_key(self):
        events = [
            {"conversation_id": "c", "message_id": "m", "text": "first"},
            {"conversation_id": "c", "message_id": "m", "text": "duplicate"},
            {"conversation_id": "c", "message_id": "m2", "text": "second"},
        ]
        result = dedupe_events(events)
        self.assertEqual([item["text"] for item in result], ["first", "second"])

    def test_should_process_group_requires_configured_conversation(self):
        self.assertTrue(should_process_group("conv-1", {"conv-1": "物资调配室"}))
        self.assertFalse(should_process_group("conv-other", {"conv-1": "物资调配室"}))

    def test_main_runs_exactly_one_shadow_history_poll_by_default(self):
        runtime = {"config_group": []}

        with (
            patch("event_worker.WorkQueryAdapter") as adapter,
            patch("event_worker.DingTalkReader") as reader,
            patch("event_worker.DwsClient") as dws,
            patch("event_worker.OneShotHistoryPoller") as poller,
            patch("builtins.print"),
        ):
            poller.return_value.run_once.return_value = {"status": "COMPLETED"}
            status = main(["--once"])

        self.assertEqual(status, 0)
        poller.assert_called_once()
        kwargs = poller.call_args.kwargs
        self.assertEqual(kwargs["mode"], "shadow")
        self.assertFalse(kwargs["write_ledger"])
        self.assertFalse(kwargs["allow_external_writes"])
        self.assertIsNone(kwargs["resolver"])

    def test_main_selects_jev_resolver_only_when_explicit(self):
        fake_resolver = object()
        with (
            patch("event_worker.WorkQueryAdapter"),
            patch("event_worker.DingTalkReader"),
            patch("event_worker.DwsClient"),
            patch("event_worker.OneShotHistoryPoller") as poller,
            patch("event_worker.JevResolver", return_value=fake_resolver) as resolver,
            patch("builtins.print"),
        ):
            poller.return_value.run_once.return_value = {"status": "COMPLETED"}
            status = main(["--once", "--llm-jev"])

        self.assertEqual(status, 0)
        resolver.assert_called_once_with()
        self.assertIs(poller.call_args.kwargs["resolver"], fake_resolver)

    def test_main_rejects_removed_codex_flag_before_external_construction(self):
        with (
            patch("event_worker.WorkQueryAdapter") as adapter,
            patch("event_worker.DingTalkReader") as reader,
            patch("event_worker.DwsClient") as dws,
            patch("builtins.print"),
        ):
            with self.assertRaises(SystemExit):
                main(["--once", "--llm-codex"])

        adapter.assert_not_called()
        reader.assert_not_called()
        dws.assert_not_called()

    def test_load_existing_actions_scopes_metadata_to_enabled_groups(self):
        class FakeAdapter:
            def __init__(self):
                self.requests = []

            def query(self, request):
                self.requests.append(dict(request))
                return {
                    "rows": [{
                        "conversation_id": request["conversation_id"],
                        "message_id": "prior-message",
                        "action_key": "todo",
                    }],
                    "has_more": False,
                }

        adapter = FakeAdapter()
        runtime = {
            "config_group": [
                {"group_name": "铁炉西工班群", "conversation_id": "cid8QKDfYpg+85AWaskH8XHjQ==", "purpose": "todo_scan"},
                {"group_name": "物资调配室", "conversation_id": "active-conversation", "purpose": "todo_scan"},
                {"group_name": "不扫描", "conversation_id": "non-scan-conversation", "purpose": "other"},
            ]
        }
        actions = load_existing_actions(adapter, runtime)
        self.assertEqual(
            [row["conversation_id"] for row in actions],
            ["cid8QKDfYpg+85AWaskH8XHjQ==", "active-conversation"],
        )
        self.assertEqual(
            [request["conversation_id"] for request in adapter.requests],
            ["cid8QKDfYpg+85AWaskH8XHjQ==", "active-conversation"],
        )
        self.assertTrue(all(request["view"] == "actions" for request in adapter.requests))

    def test_business_candidate_hydration_uses_exact_source_identity(self):
        class FakeAdapter:
            def __init__(self):
                self.requests = []

            def query(self, request):
                self.requests.append(dict(request))
                self.asserted = request
                if request["view"] != "messages":
                    raise AssertionError(request)
                if request.get("conversation_id") != "conv-old" or request.get("message_id") != "msg-old":
                    raise AssertionError("candidate hydration must use exact source keys")
                return {
                    "rows": [{
                        "conversation_id": "conv-old",
                        "message_id": "msg-old",
                        "group_name": "铁炉西工班群",
                        "sender_id": "old-sender-id",
                        "sender_name": "王萌",
                        "message_time": "2026-09-20 09:00:00",
                        "text": "请于9月24日前完成回购物资核验工作。",
                    }],
                    "has_more": False,
                }

        config = {
            "config_group": [{"group_name": "铁炉西工班群", "conversation_id": "conv-old", "purpose": "todo_scan"}],
            "config_staff": [[1, "李林骁", "SELF", "leader"], [2, "苗笑天", "MIAO", "executor"]],
            "active_routing_loaded": True,
            "responsibility_rules": {"materials": "苗笑天"},
        }
        actions = [{
            "conversation_id": "conv-old",
            "message_id": "msg-old",
            "business_key": "old-business-key",
            "status": "EXTERNAL_CREATED",
            "external_task_id": "remote-task-id",
            "created_at": "2026-09-20 09:01:00",
        }]
        adapter = FakeAdapter()
        candidates, issue = load_business_candidate_context(
            adapter, config, {"name": "李林骁", "user_id": "SELF"}, actions
        )

        self.assertIsNone(issue)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["route_module"], "materials")
        self.assertEqual(candidates[0]["route_owner"], "苗笑天")
        self.assertEqual(candidates[0]["source_text"], "请于9月24日前完成回购物资核验工作。")
        self.assertEqual(candidates[0]["message_time"], "2026-09-20 09:00:00")
        self.assertEqual(adapter.requests, [{
            "view": "messages", "limit": 100,
            "conversation_id": "conv-old", "message_id": "msg-old",
        }])

    def test_contextual_retrieval_candidate_uses_only_verified_linked_todo_readback(self):
        source_text = "铁炉西工班的卡尺已送检，尚未取回；未取回的下次开会时去取。"
        title = "下次开会时取回铁炉西送检卡尺"
        config = {
            "config_group": [{"group_name": "铁炉西工班群", "conversation_id": "conv-retrieval", "purpose": "todo_scan"}],
            "config_staff": [[1, "李林骁", "different-contact-namespace", "leader"]],
            "active_routing_loaded": True,
        }
        action = {
            "conversation_id": "conv-retrieval",
            "message_id": "retrieval-source",
            "business_key": "linked-business-key",
            "status": "EXTERNAL_CREATED",
            "external_task_id": "native-task",
            "created_at": "2026-10-08 09:01:00",
        }
        self_user = {"name": "李林骁", "user_id": "different-contact-namespace"}

        class FakeAdapter:
            def __init__(self):
                self.requests = []

            def query(self, request):
                self.requests.append(dict(request))
                self.assert_exact_source = request
                if request != {
                    "view": "messages", "limit": 100,
                    "conversation_id": "conv-retrieval", "message_id": "retrieval-source",
                }:
                    raise AssertionError(request)
                return {"rows": [{
                    "conversation_id": "conv-retrieval",
                    "message_id": "retrieval-source",
                    "group_name": "铁炉西工班群",
                    "sender_id": "message-sender-opaque",
                    "sender_name": "报告人",
                    "message_time": "2026-10-08 09:00:00",
                    "text": source_text,
                }], "has_more": False}

        class FakeDws:
            def __init__(self, detail):
                self.detail = detail
                self.calls = []

            def get_todo(self, task_id):
                self.calls.append(("get_todo", task_id))
                return self.detail

            def __getattr__(self, name):
                if name in {"list_todos", "search_todos", "create_todo", "update_todo"}:
                    raise AssertionError(f"forbidden DWS call: {name}")
                raise AttributeError(name)

        adapter = FakeAdapter()
        dws = FakeDws({
            "taskId": "native-task",
            "subject": title,
            "isDone": False,
            "executorInfos": [{"name": "李林骁", "userId": 333}],
            "executorIds": ["333"],
            "dueTime": 0,
        })
        candidates, issue = load_business_candidate_context(
            adapter, config, self_user, [action], dws=dws
        )

        self.assertIsNone(issue)
        self.assertEqual(dws.calls, [("get_todo", "native-task")])
        self.assertEqual(adapter.requests, [{
            "view": "messages", "limit": 100,
            "conversation_id": "conv-retrieval", "message_id": "retrieval-source",
        }])
        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate["business_key"], "linked-business-key")
        self.assertEqual(candidate["external_task_id"], "native-task")
        self.assertEqual(candidate["conversation_id"], "conv-retrieval")
        self.assertEqual(candidate["message_id"], "retrieval-source")
        self.assertEqual(candidate["route_module"], "workgroup_follow_up")
        self.assertEqual(candidate["route_owner"], "李林骁")
        self.assertEqual(candidate["title"], title)
        self.assertIn(source_text, candidate["source_text"])
        self.assertIn(title, candidate["source_text"])

    def test_contextual_retrieval_candidate_requires_complete_unambiguous_readback(self):
        source_text = "铁炉西工班的卡尺已送检，尚未取回；未取回的下次开会时去取。"
        title = "下次开会时取回铁炉西送检卡尺"
        config = {
            "config_group": [{"group_name": "铁炉西工班群", "conversation_id": "conv-retrieval", "purpose": "todo_scan"}],
            "config_staff": [[1, "李林骁", "different-contact-namespace", "leader"]],
            "active_routing_loaded": True,
        }
        action = {
            "conversation_id": "conv-retrieval", "message_id": "retrieval-source",
            "business_key": "linked-business-key", "status": "EXTERNAL_CREATED",
            "external_task_id": "native-task", "created_at": "2026-10-08 09:01:00",
        }

        class FakeAdapter:
            def query(self, request):
                return {"rows": [{
                    "conversation_id": "conv-retrieval", "message_id": "retrieval-source",
                    "group_name": "铁炉西工班群", "sender_id": "message-sender-opaque",
                    "sender_name": "报告人", "message_time": "2026-10-08 09:00:00",
                    "text": source_text,
                }], "has_more": False}

        complete = {
            "taskId": "native-task", "subject": title, "isDone": False,
            "executorInfos": [{"name": "李林骁", "userId": 333}], "executorIds": ["333"],
        }

        class FakeDws:
            def __init__(self, detail=None, error=None):
                self.detail = detail
                self.error = error
                self.calls = []

            def get_todo(self, task_id):
                self.calls.append(task_id)
                if self.error:
                    raise self.error
                return self.detail

        bad_details = {
            "absent": None,
            "mismatched_pointer": {**complete, "taskId": "other-native-task"},
            "mismatched_subject": {**complete, "subject": "下次开会时取回其他器具"},
            "ambiguous_executor": {**complete, "executorInfos": [{"name": "李林骁"}, {"name": "其他人"}]},
            "unknown_done": {**complete, "isDone": 0},
            "missing_done": {key: value for key, value in complete.items() if key != "isDone"},
            "wrong_executor": {**complete, "executorInfos": [{"name": "其他人"}]},
        }
        for case, detail in bad_details.items():
            with self.subTest(case=case):
                dws = FakeDws(detail=detail)
                candidates, issue = load_business_candidate_context(
                    FakeAdapter(), config, {"name": "李林骁", "user_id": "different-contact-namespace"},
                    [action], dws=dws,
                )
                self.assertEqual(candidates, [])
                self.assertEqual(issue, "business_candidate_context_unavailable")
                self.assertEqual(dws.calls, ["native-task"])

        failed_dws = FakeDws(error=RuntimeError("read failed"))
        candidates, issue = load_business_candidate_context(
            FakeAdapter(), config, {"name": "李林骁", "user_id": "different-contact-namespace"},
            [action], dws=failed_dws,
        )
        self.assertEqual(candidates, [])
        self.assertEqual(issue, "business_candidate_context_unavailable")

        candidates, issue = load_business_candidate_context(
            FakeAdapter(), config, {"name": "李林骁", "user_id": "different-contact-namespace"},
            [action], dws=None,
        )
        self.assertEqual(candidates, [])
        self.assertEqual(issue, "business_candidate_context_unavailable")

    def test_confirmed_done_contextual_retrieval_is_omitted_safely(self):
        source_text = "铁炉西工班的卡尺已送检，尚未取回；未取回的下次开会时去取。"
        config = {
            "config_group": [{"group_name": "铁炉西工班群", "conversation_id": "conv-retrieval", "purpose": "todo_scan"}],
            "config_staff": [[1, "李林骁", "different-contact-namespace", "leader"]],
            "active_routing_loaded": True,
        }
        action = {
            "conversation_id": "conv-retrieval", "message_id": "retrieval-source",
            "business_key": "linked-business-key", "status": "EXTERNAL_CREATED",
            "external_task_id": "native-task", "created_at": "2026-10-08 09:01:00",
        }

        class FakeAdapter:
            def query(self, request):
                return {"rows": [{
                    "conversation_id": "conv-retrieval", "message_id": "retrieval-source",
                    "group_name": "铁炉西工班群", "sender_id": "sender", "sender_name": "报告人",
                    "message_time": "2026-10-08 09:00:00", "text": source_text,
                }], "has_more": False}

        class FakeDws:
            def get_todo(self, task_id):
                self.task_id = task_id
                return {
                    "taskId": "native-task", "subject": "下次开会时取回铁炉西送检卡尺",
                    "isDone": True, "executorInfos": [{"name": "李林骁", "userId": 333}],
                }

        candidates, issue = load_business_candidate_context(
            FakeAdapter(), config, {"name": "李林骁", "user_id": "different-contact-namespace"},
            [action], dws=FakeDws(),
        )

        self.assertEqual(candidates, [])
        self.assertIsNone(issue)

    def test_business_candidate_hydration_caps_candidates_and_uses_latest_five(self):
        class FakeAdapter:
            def __init__(self):
                self.message_ids = []

            def query(self, request):
                message_id = request.get("message_id")
                self.message_ids.append(message_id)
                return {"rows": [{
                    "conversation_id": "conv-old",
                    "message_id": message_id,
                    "group_name": "铁炉西工班群",
                    "sender_id": "sender-id",
                    "sender_name": "王萌",
                    "message_time": "2026-09-20 09:00:00",
                    "text": "请于9月24日前完成回购物资核验工作。",
                }], "has_more": False}

        config = {
            "config_group": [{"group_name": "铁炉西工班群", "conversation_id": "conv-old", "purpose": "todo_scan"}],
            "config_staff": [[1, "李林骁", "SELF", "leader"], [2, "苗笑天", "MIAO", "executor"]],
            "active_routing_loaded": True,
            "responsibility_rules": {"materials": "苗笑天"},
        }
        actions = [{
            "conversation_id": "conv-old", "message_id": f"msg-{index}",
            "business_key": f"key-{index}", "status": "EXTERNAL_CREATED",
            "external_task_id": f"task-{index}", "created_at": f"2026-09-{20 + index:02d} 09:00:00",
        } for index in range(1, 7)]
        adapter = FakeAdapter()
        candidates, issue = load_business_candidate_context(
            adapter, config, {"name": "李林骁", "user_id": "SELF"}, actions
        )

        self.assertIsNone(issue)
        self.assertEqual(len(candidates), 5)
        self.assertEqual([row["message_id"] for row in candidates], [f"msg-{index}" for index in range(6, 1, -1)])
        self.assertEqual(adapter.message_ids, [f"msg-{index}" for index in range(6, 1, -1)])

    def test_writer_environment_defaults_to_restricted_local_file(self):
        with patch.dict(os.environ, {}, clear=True):
            configure_writer_environment()
            self.assertEqual(os.environ["HERMES_WORK_WRITER_ENV"], "/etc/cipher-work.env")

    def test_execution_window_is_09_to_1730_beijing_time(self):
        self.assertTrue(within_execution_window(datetime.fromisoformat("2026-09-22T09:00:00+08:00")))
        self.assertTrue(within_execution_window(datetime.fromisoformat("2026-09-22T17:29:59+08:00")))
        self.assertFalse(within_execution_window(datetime.fromisoformat("2026-09-22T08:59:59+08:00")))
        self.assertFalse(within_execution_window(datetime.fromisoformat("2026-09-22T17:30:00+08:00")))

    def test_event_processor_does_not_call_llm_before_department_scope_rejection(self):
        class FakeResolver:
            calls = 0
            def resolve(self, event, plan):
                self.calls += 1
                return {"semantic_intent": "follow_up"}

        runtime = {
            "config_group": [{"group_name": "铁炉西工班群", "conversation_id": "conv-1", "purpose": "todo_scan"}],
            "config_staff": [[1, "李林骁", "SELF", "leader"]],
            "active_routing_loaded": True,
        }
        resolver = FakeResolver()
        processor = EventProcessor(runtime, {"name": "李林骁", "user_id": "SELF"}, mode="shadow", llm_resolver=resolver)
        plans = processor.process_batch([{
            "conversationId": "conv-1", "messageId": "department-only",
            "createTime": "2026-09-24 10:00:00",
            "text": "物资管理中心可办理领用手续。",
        }])
        self.assertEqual(plans[0]["decision"], "IGNORE")
        self.assertEqual(resolver.calls, 0)

    def test_jev_serializes_redacted_message_and_business_candidate_text(self):
        request_bodies = []
        response_body = {"answers": {
            "action_signal": {"type": "noul", "noul": 0.90},
            "item_status": {
                "type": "choice", "choice": "new",
                "probabilities": {"new": 0.90, "change": 0.02, "complete": 0.02, "notice": 0.02, "uncertain": 0.04},
                "confidence": 0.90,
            },
            "due_date": {
                "type": "choice", "choice": "none",
                "probabilities": {"none": 0.90, "uncertain": 0.10},
                "confidence": 0.90,
            },
            "business_match_0": {
                "type": "score", "score": 0.4, "confidence": 0.90,
                "probabilities": {"0": 0.70, "1": 0.20, "2": 0.10},
            },
        }}

        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self, _limit):
                return json.dumps(response_body).encode("utf-8")

        def fake_urlopen(request, timeout):
            self.assertGreater(timeout, 0)
            request_bodies.append(request.data.decode("utf-8"))
            return FakeResponse()

        resolver = llm_adapter.JevResolver(api_key="synthetic-local-only")
        with patch("llm_adapter.urllib.request.urlopen", side_effect=fake_urlopen):
            resolver.resolve(
                {
                    "text": "请核对附件 fileId=FILE-XY99 并联系13800138000，随后@李林骁确认。",
                    "create_time": "2026-09-22 10:00:00",
                    "at_users": [{"name": "李林骁", "userId": "synthetic-user-1"}],
                },
                business_candidates=[{
                    "key": "business_0",
                    "text": "核查 mediaId:MEDIA-QR88 并联系+86 139-0011-2233，@王萌",
                    "create_time": "2026-09-20 09:00:00",
                }],
            )

        self.assertEqual(len(request_bodies), 1)
        serialized_body = request_bodies[0]
        request_payload = json.loads(serialized_body)
        state = json.loads(request_payload["state"])
        for secret in ("FILE-XY99", "13800138000", "李林骁", "MEDIA-QR88", "139-0011-2233", "王萌"):
            self.assertNotIn(secret, serialized_body)
        self.assertIn("fileId=[RESOURCE_ID]", state["text"])
        self.assertIn("[PHONE]", state["text"])
        self.assertIn("[PERSON_MENTION]", state["text"])
        candidate_text = state["business_candidates"][0]["text"]
        self.assertIn("mediaId:[RESOURCE_ID]", candidate_text)
        self.assertIn("[PHONE]", candidate_text)
        self.assertIn("[PERSON_MENTION]", candidate_text)
        self.assertEqual(resolver.last_redaction_counts, {
            "message": {"resource_ids": 1, "phone_numbers": 1, "person_mentions": 1, "embedded_ids": 0},
            "business_candidates": {"resource_ids": 1, "phone_numbers": 1, "person_mentions": 1, "embedded_ids": 0},
            "total": {"resource_ids": 2, "phone_numbers": 2, "person_mentions": 2, "embedded_ids": 0},
        })

    def test_jev_redacts_embedded_identifier_labels_from_final_serialized_body(self):
        request_bodies = []
        response_body = {"answers": {
            "action_signal": {"type": "noul", "noul": 0.90},
            "item_status": {
                "type": "choice", "choice": "new",
                "probabilities": {"new": 0.90, "change": 0.02, "complete": 0.02, "notice": 0.02, "uncertain": 0.04},
                "confidence": 0.90,
            },
            "due_date": {
                "type": "choice", "choice": "none",
                "probabilities": {"none": 0.90, "uncertain": 0.10},
                "confidence": 0.90,
            },
            "business_match_0": {
                "type": "score", "score": 0.0, "confidence": 0.90,
                "probabilities": {"0": 0.90, "1": 0.05, "2": 0.05},
            },
        }}

        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self, _limit):
                return json.dumps(response_body).encode("utf-8")

        def fake_urlopen(request, timeout):
            request_bodies.append(request.data)
            return FakeResponse()

        resolver = llm_adapter.JevResolver(api_key="synthetic-local-only")
        with patch("llm_adapter.urllib.request.urlopen", side_effect=fake_urlopen):
            resolver.resolve(
                {
                    "text": (
                        "请核对回购物资 taskId=TASK-MSG-101; messageId:MESSAGE-MSG-202; "
                        "conversation_id=CONV-MSG-303; sessionId%3DSESSION-MSG-404。"
                    ),
                    "create_time": "2026-09-22 10:00:00",
                },
                business_candidates=[{
                    "key": "business_0",
                    "text": (
                        "复核既有关联 externalTaskId:CAND-TASK-501; "
                        "message_id%3AMSG-CAND-602; conversationId=CONV-CAND-703。"
                    ),
                    "create_time": "2026-09-20 09:00:00",
                }],
            )

        self.assertEqual(len(request_bodies), 1)
        serialized_body = request_bodies[0].decode("utf-8")
        state = json.loads(json.loads(serialized_body)["state"])
        for secret in (
            "TASK-MSG-101", "MESSAGE-MSG-202", "CONV-MSG-303", "SESSION-MSG-404",
            "CAND-TASK-501", "MSG-CAND-602", "CONV-CAND-703",
        ):
            self.assertNotIn(secret, serialized_body)
        self.assertIn("taskId=[TASK_ID]", state["text"])
        self.assertIn("messageId:[TASK_ID]", state["text"])
        self.assertIn("conversation_id=[TASK_ID]", state["text"])
        self.assertIn("sessionId%3D[TASK_ID]", state["text"])
        self.assertEqual(resolver.last_redaction_counts, {
            "message": {"resource_ids": 0, "phone_numbers": 0, "person_mentions": 0, "embedded_ids": 4},
            "business_candidates": {"resource_ids": 0, "phone_numbers": 0, "person_mentions": 0, "embedded_ids": 3},
            "total": {"resource_ids": 0, "phone_numbers": 0, "person_mentions": 0, "embedded_ids": 7},
        })

    def test_jev_double_encoded_identifier_labels_fail_closed_for_message_and_candidate(self):
        transport_calls = []
        resolver = llm_adapter.JevResolver(
            api_key="synthetic-local-only",
            transport=lambda payload, timeout: transport_calls.append(payload) or {"answers": {}},
        )
        cases = (
            ({"text": "请核对 taskId%253DTASK-DOUBLE-801", "create_time": "2026-09-22 10:00:00"}, []),
            (
                {"text": "请核对回购物资", "create_time": "2026-09-22 10:00:00"},
                [{"key": "business_0", "text": "messageId%253AMSG-DOUBLE-902", "create_time": "2026-09-20 09:00:00"}],
            ),
        )
        for message, business_candidates in cases:
            with self.subTest(message=message, candidates=business_candidates):
                with self.assertRaisesRegex(LlmError, "jev_redaction_incomplete"):
                    resolver.resolve(message, business_candidates=business_candidates)

        self.assertEqual(transport_calls, [])

    def test_jev_identifier_marker_with_unredacted_suffix_fails_before_transport(self):
        transport_calls = []
        resolver = llm_adapter.JevResolver(
            api_key="synthetic-local-only",
            transport=lambda payload, timeout: transport_calls.append(payload) or {"answers": {}},
        )
        with self.assertRaisesRegex(LlmError, "jev_redaction_incomplete"):
            resolver.resolve({
                "text": "请核对 taskId=[TASK_ID]UNREDACTED-TAIL",
                "create_time": "2026-09-22 10:00:00",
            })
        self.assertEqual(transport_calls, [])

    def test_event_processor_includes_jev_redaction_counts_on_success_and_failure(self):
        runtime = {
            "config_group": [{"group_name": "铁炉西工班群", "conversation_id": "conv-redaction-audit", "purpose": "todo_scan"}],
            "config_staff": [[1, "李林骁", "SELF", "leader"], [2, "苗笑天", "MIAO", "executor"]],
            "active_routing_loaded": True,
            "responsibility_rules": {"materials": "苗笑天"},
        }
        answers = {
            "action_signal": {"type": "noul", "noul": 0.90},
            "item_status": {
                "type": "choice", "choice": "new",
                "probabilities": {"new": 0.90, "change": 0.02, "complete": 0.02, "notice": 0.02, "uncertain": 0.04},
                "confidence": 0.90,
            },
            "due_date": {
                "type": "choice", "choice": "none",
                "probabilities": {"date_0": 0.05, "none": 0.90, "uncertain": 0.05},
                "confidence": 0.90,
            },
            "business_match_0": {
                "type": "score", "score": 0.0, "confidence": 0.90,
                "probabilities": {"0": 0.90, "1": 0.05, "2": 0.05},
            },
        }
        existing = [{
            "business_key": "prior-redaction-audit",
            "external_task_id": "linked-task-id-not-for-jev",
            "route_module": "materials",
            "route_owner": "苗笑天",
            "status": "EXTERNAL_CREATED",
            "source_text": "复核既有关联 taskId=CAND-TASK-10; messageId:CAND-MSG-20; conversation_id=CAND-CONV-30。",
            "message_time": "2026-09-20 09:00:00",
        }]
        resolver = llm_adapter.JevResolver(
            api_key="synthetic-local-only",
            transport=lambda payload, timeout: {"answers": answers},
        )
        processor = EventProcessor(
            runtime,
            {"name": "李林骁", "user_id": "SELF"},
            mode="shadow",
            llm_resolver=resolver,
            existing_actions=existing,
        )
        good_event = {
            "conversation_id": "conv-redaction-audit",
            "message_id": "audit-message-1",
            "group": "铁炉西工班群",
            "create_time": "2026-09-22 10:00:00",
            "text": (
                "回购物资仍存放在集中存储货位，需求部门可在9月23日前办理领用手续。 "
                "taskId=MSG-TASK-10; messageId:MSG-MSG-20; conversation_id=MSG-CONV-30。"
            ),
        }

        success_plan = processor.process_batch([good_event])[0]
        expected_success = {
            "message": {"resource_ids": 0, "phone_numbers": 0, "person_mentions": 0, "embedded_ids": 3},
            "business_candidates": {"resource_ids": 0, "phone_numbers": 0, "person_mentions": 0, "embedded_ids": 3},
            "total": {"resource_ids": 0, "phone_numbers": 0, "person_mentions": 0, "embedded_ids": 6},
        }
        self.assertEqual(success_plan["redaction_counts"], expected_success)
        self.assertEqual(success_plan["semantic_evidence"]["redaction_counts"], expected_success)

        bad_event = dict(good_event)
        bad_event.update(
            message_id="audit-message-2",
            text=good_event["text"] + " taskId%253DUNREDACTABLE-TASK-99",
        )
        failure_plan = processor.process_batch([bad_event])[0]
        self.assertEqual(failure_plan["decision"], "REVIEW")
        self.assertEqual(failure_plan["reason"], "llm_resolution_failed")
        expected_failure = {
            "message": {"resource_ids": 0, "phone_numbers": 0, "person_mentions": 0, "embedded_ids": 3},
            "business_candidates": {"resource_ids": 0, "phone_numbers": 0, "person_mentions": 0, "embedded_ids": 3},
            "total": {"resource_ids": 0, "phone_numbers": 0, "person_mentions": 0, "embedded_ids": 6},
        }
        self.assertEqual(failure_plan["redaction_counts"], expected_failure)
        for value in ("TASK-10", "MSG-20", "CONV-30", "UNREDACTABLE-TASK-99", "CAND-TASK-10", "CAND-MSG-20", "CAND-CONV-30"):
            self.assertNotIn(value, json.dumps(failure_plan["redaction_counts"], ensure_ascii=False))

    def test_jev_fails_closed_before_transport_for_unrecognized_encoded_resource_id(self):
        transport_calls = []
        resolver = llm_adapter.JevResolver(
            api_key="synthetic-local-only",
            transport=lambda payload, timeout: transport_calls.append(payload) or {"answers": {}},
        )

        with self.assertRaisesRegex(LlmError, "jev_redaction_incomplete"):
            resolver.resolve({
                "text": "请打开链接 fileId%253DOPAQUE-RESOURCE-9",
                "create_time": "2026-09-22 10:00:00",
            })

        self.assertEqual(transport_calls, [])

    def test_jev_none_due_date_selection_suppresses_program_fallback_parse(self):
        def transport(payload, timeout):
            self.assertEqual(set(json.loads(payload["state"])["date_candidates"][0]), {"key", "due_date", "due_iso"})
            return {"answers": {
                "action_signal": {"type": "noul", "noul": 0.90},
                "item_status": {
                    "type": "choice", "choice": "new",
                    "probabilities": {"new": 0.90, "change": 0.02, "complete": 0.02, "notice": 0.02, "uncertain": 0.04},
                    "confidence": 0.90,
                },
                "due_date": {
                    "type": "choice", "choice": "none",
                    "probabilities": {"date_0": 0.05, "none": 0.90, "uncertain": 0.05},
                    "confidence": 0.90,
                },
            }}

        runtime = {
            "config_group": [{"group_name": "铁炉西工班群", "conversation_id": "conv-none-date", "purpose": "todo_scan"}],
            "config_staff": [[1, "李林骁", "SELF", "leader"], [2, "苗笑天", "MIAO", "executor"]],
            "active_routing_loaded": True,
            "responsibility_rules": {"materials": "苗笑天"},
        }
        processor = EventProcessor(
            runtime,
            {"name": "李林骁", "user_id": "SELF"},
            mode="shadow",
            llm_resolver=llm_adapter.JevResolver(api_key="fake-key", transport=transport),
        )
        plans = processor.process_batch([{
            "conversation_id": "conv-none-date", "message_id": "msg-none-date",
            "group": "铁炉西工班群", "create_time": "2026-09-22 10:00:00",
            "text": "请于9月23日前完成回购物资核验。",
        }])

        self.assertEqual(plans[0]["decision"], "REVIEW")
        self.assertIsNone(plans[0]["due_date"])
        self.assertIsNone(plans[0]["due_iso"])

    def test_event_processor_passes_program_date_candidates_to_jev_and_uses_only_selected_candidate(self):
        seen = {}
        candidate = {
            "key": "date_0",
            "due_date": "2026-09-23",
            "due_iso": "2026-09-23T18:00:00+08:00",
        }
        existing_business = {
            "business_key": "prior-business-key",
            "external_task_id": "remote-task-1",
            "route_module": "materials",
            "route_owner": "苗笑天",
            "status": "EXTERNAL_CREATED",
            "text": "请于9月23日前完成库房资料核对。",
            "message_time": "2026-09-20 09:00:00",
        }

        def transport(payload, timeout):
            seen["payload"] = payload
            state = json.loads(payload["state"])
            self.assertEqual(state["date_candidates"], [candidate])
            return {
                "answers": {
                    "route_owner": "恶意指定责任人",
                    "route_module": "malicious_module",
                    "decision": "CREATE",
                    "write": True,
                    "action_signal": {"type": "noul", "noul": 0.88},
                    "item_status": {
                        "type": "choice", "choice": "new",
                        "probabilities": {"new": 0.90, "change": 0.02, "complete": 0.02, "notice": 0.02, "uncertain": 0.04},
                        "confidence": 0.90,
                    },
                    "due_date": {
                        "type": "choice", "choice": "date_0",
                        "probabilities": {"date_0": 0.90, "none": 0.05, "uncertain": 0.05},
                        "confidence": 0.90,
                    },
                    "business_match_0": {
                        "type": "score", "score": 0.0, "confidence": 0.95,
                        "legend": {"0": "different", "1": "related", "2": "same business task"},
                        "probabilities": {"0": 0.95, "1": 0.03, "2": 0.02},
                    },
                },
            }

        runtime = {
            "config_group": [{"group_name": "铁炉西工班群", "conversation_id": "conv-jev", "purpose": "todo_scan"}],
            "config_staff": [[1, "李林骁", "SELF", "leader"], [2, "苗笑天", "MIAO", "executor"]],
            "active_routing_loaded": True,
            "responsibility_rules": {"materials": "苗笑天"},
        }
        resolver = llm_adapter.JevResolver(api_key="fake-key", transport=transport)
        processor = EventProcessor(
            runtime,
            {"name": "李林骁", "user_id": "SELF"},
            mode="shadow",
            llm_resolver=resolver,
            existing_actions=[existing_business],
        )
        plans = processor.process_batch([{
            "conversationId": "conv-jev",
            "messageId": "msg-jev-date",
            "createTime": "2026-09-22 10:00:00",
            "text": "回购物资仍存放在集中存储货位，需求部门可在9月23日前办理领用手续。",
        }])

        self.assertEqual(plans[0]["decision"], "REVIEW")
        self.assertEqual(plans[0]["reason"], "business_candidate_no_match_requires_review")
        self.assertEqual(plans[0]["route_owner"], "苗笑天")
        self.assertEqual(plans[0]["due_date"], candidate["due_date"])
        self.assertEqual(plans[0]["due_iso"], candidate["due_iso"])
        self.assertEqual(plans[0]["semantic_evidence"]["action_probability"], 0.88)
        self.assertFalse(plans[0]["semantic_evidence"]["decision_boundary"]["calibrated"])
        self.assertNotIn("action_probability", plans[0].get("llm_fields", []))
        self.assertEqual(processor.stats["create"], 0)
        self.assertEqual(processor.stats["external_todos_created"], 0)
        request_text = json.dumps(seen["payload"], ensure_ascii=False)
        for forbidden in ("message_id", "conversation_id", "group_name", "sender", "route_owner", "decision"):
            self.assertNotIn(forbidden, request_text)

    def test_event_processor_reviews_jev_create_candidate_when_no_business_match_pool_exists(self):
        def transport(payload, timeout):
            return {
                "answers": {
                    "action_signal": {"type": "noul", "noul": 0.90},
                    "item_status": {
                        "type": "choice", "choice": "new",
                        "probabilities": {"new": 0.90, "change": 0.02, "complete": 0.02, "notice": 0.02, "uncertain": 0.04},
                        "confidence": 0.90,
                    },
                    "due_date": {
                        "type": "choice", "choice": "date_0",
                        "probabilities": {"date_0": 0.90, "none": 0.05, "uncertain": 0.05},
                        "confidence": 0.90,
                    },
                },
            }

        runtime = {
            "config_group": [{"group_name": "铁炉西工班群", "conversation_id": "conv-no-business", "purpose": "todo_scan"}],
            "config_staff": [[1, "李林骁", "SELF", "leader"], [2, "苗笑天", "MIAO", "executor"]],
            "active_routing_loaded": True,
            "responsibility_rules": {"materials": "苗笑天"},
        }
        processor = EventProcessor(
            runtime,
            {"name": "李林骁", "user_id": "SELF"},
            mode="shadow",
            llm_resolver=llm_adapter.JevResolver(api_key="fake-key", transport=transport),
        )
        plans = processor.process_batch([{
            "conversation_id": "conv-no-business",
            "message_id": "new-source",
            "group": "铁炉西工班群",
            "create_time": "2026-09-22 10:00:00",
            "text": "回购物资在9月23日前由需求部门办理领用手续。",
        }])

        self.assertEqual(plans[0]["decision"], "REVIEW")
        self.assertEqual(plans[0]["reason"], "business_candidate_pool_missing")
        self.assertEqual(processor.stats["external_todos_created"], 0)

    def test_event_processor_sends_bounded_recalled_business_candidate_to_jev_score(self):
        seen = {}
        candidate = {
            "business_key": "prior-business-key",
            "external_task_id": "existing-task-1",
            "conversation_id": "private-conversation",
            "message_id": "private-message",
            "route_module": "materials",
            "route_owner": "苗笑天",
            "due_date": "2026-09-23",
            "text": "请于9月23日前完成回购物资核验工作。",
            "message_time": "2026-09-20 09:00:00",
            "status": "EXTERNAL_CREATED",
        }

        def transport(payload, timeout):
            seen["payload"] = payload
            return {
                "answers": {
                    "action_signal": {"type": "noul", "noul": 0.90},
                    "item_status": {
                        "type": "choice", "choice": "new",
                        "probabilities": {"new": 0.90, "change": 0.02, "complete": 0.02, "notice": 0.02, "uncertain": 0.04},
                        "confidence": 0.90,
                    },
                    "due_date": {
                        "type": "choice", "choice": "date_0",
                        "probabilities": {"date_0": 0.90, "none": 0.05, "uncertain": 0.05},
                        "confidence": 0.90,
                    },
                    "business_match_0": {
                        "type": "score", "score": 2.0, "confidence": 0.95,
                        "legend": {"0": "different", "1": "related", "2": "same business task"},
                        "probabilities": {"0": 0.02, "1": 0.03, "2": 0.95},
                    },
                },
            }

        runtime = {
            "config_group": [{"group_name": "铁炉西工班群", "conversation_id": "conv-business", "purpose": "todo_scan"}],
            "config_staff": [[1, "李林骁", "SELF", "leader"], [2, "苗笑天", "MIAO", "executor"]],
            "active_routing_loaded": True,
            "responsibility_rules": {"materials": "苗笑天"},
        }
        resolver = llm_adapter.JevResolver(api_key="fake-key", transport=transport)
        processor = EventProcessor(
            runtime,
            {"name": "李林骁", "user_id": "SELF"},
            mode="shadow",
            llm_resolver=resolver,
            existing_actions=[candidate],
        )
        plans = processor.process_batch([{
            "conversation_id": "conv-business",
            "message_id": "new-message",
            "group": "铁炉西工班群",
            "create_time": "2026-09-22 10:00:00",
            "text": "请于9月23日前完成回购物资复核工作。",
        }])

        self.assertEqual(plans[0]["decision"], "REVIEW")
        self.assertEqual(plans[0]["reason"], "business_duplicate_requires_review")
        state = json.loads(seen["payload"]["state"])
        self.assertEqual(state["business_candidates"], [{
            "key": "business_0",
            "text": candidate["text"],
            "create_time": candidate["message_time"],
        }])
        for forbidden in ("conversation_id", "message_id", "route_owner", "task_id", "苗笑天"):
            self.assertNotIn(forbidden, json.dumps(seen["payload"], ensure_ascii=False))

    def test_jev_business_score_instructions_identify_each_candidate_field(self):
        seen = {}
        candidates = [
            {
                "key": "business_0",
                "text": "请核验回购物资入库记录。",
                "create_time": "2026-09-20 09:00:00",
            },
            {
                "key": "business_1",
                "text": "请复核仓库货位标识。",
                "create_time": "2026-09-21 09:00:00",
            },
        ]
        answers = {
            "action_signal": {"type": "noul", "noul": 0.90},
            "item_status": {
                "type": "choice", "choice": "new",
                "probabilities": {"new": 0.90, "change": 0.02, "complete": 0.02, "notice": 0.02, "uncertain": 0.04},
                "confidence": 0.90,
            },
            "due_date": {
                "type": "choice", "choice": "none",
                "probabilities": {"none": 0.90, "uncertain": 0.10},
                "confidence": 0.90,
            },
            "business_match_0": {
                "type": "score", "score": 0.4, "confidence": 0.90,
                "probabilities": {"0": 0.70, "1": 0.20, "2": 0.10},
            },
            "business_match_1": {
                "type": "score", "score": 1.6, "confidence": 0.90,
                "probabilities": {"0": 0.10, "1": 0.20, "2": 0.70},
            },
        }

        def transport(payload, timeout):
            seen["payload"] = payload
            return {"answers": answers}

        resolver = llm_adapter.JevResolver(api_key="fake-key", transport=transport)
        resolver.resolve(
            {"text": "请完成回购物资核验。", "create_time": "2026-09-22 10:00:00"},
            business_candidates=candidates,
        )

        payload = seen["payload"]
        state = json.loads(payload["state"])
        self.assertEqual(state["business_candidates"], candidates)
        questions = payload["questions"]
        score_questions = [questions[f"business_match_{index}"] for index in range(2)]
        instructions = [question["instructions"] for question in score_questions]
        self.assertEqual(len(set(instructions)), 2)
        for index, (candidate, question, instruction) in enumerate(zip(candidates, score_questions, instructions)):
            with self.subTest(candidate_index=index):
                self.assertEqual(question["type"], "score")
                self.assertEqual(
                    question["criteria"],
                    ["明显不同的业务事项", "主题相关但不是同一项待办", "同一业务事项或同一待办的重复/跟进表述"],
                )
                self.assertIn(f"state.business_candidates[{index}].text", instruction)
                self.assertIn(candidate["key"], instruction)
                self.assertNotIn(candidate["text"], instruction)

    def test_event_processor_reviews_close_cross_message_candidate_scores(self):
        answers = {
            "action_signal": {"type": "noul", "noul": 0.90},
            "item_status": {
                "type": "choice", "choice": "new",
                "probabilities": {"new": 0.90, "change": 0.02, "complete": 0.02, "notice": 0.02, "uncertain": 0.04},
                "confidence": 0.90,
            },
            "due_date": {
                "type": "choice", "choice": "none",
                "probabilities": {"none": 0.90, "uncertain": 0.10},
                "confidence": 0.90,
            },
            "business_match_0": {
                "type": "score", "score": 1.85, "confidence": 0.95,
                "legend": {"0": "different", "1": "related", "2": "same business task"},
                "probabilities": {"0": 0.02, "1": 0.03, "2": 0.95},
            },
            "business_match_1": {
                "type": "score", "score": 1.70, "confidence": 0.93,
                "legend": {"0": "different", "1": "related", "2": "same business task"},
                "probabilities": {"0": 0.04, "1": 0.22, "2": 0.74},
            },
        }
        runtime = {
            "config_group": [{"group_name": "铁炉西工班群", "conversation_id": "conv-near-score", "purpose": "todo_scan"}],
            "config_staff": [[1, "李林骁", "SELF", "leader"], [2, "苗笑天", "MIAO", "executor"]],
            "active_routing_loaded": True,
            "responsibility_rules": {"materials": "苗笑天"},
        }
        existing = [
            {
                "business_key": "prior-1", "external_task_id": "task-1", "route_module": "materials",
                "route_owner": "苗笑天", "status": "EXTERNAL_CREATED",
                "source_text": "完成回购物资入库核验。", "message_time": "2026-09-20 09:00:00",
            },
            {
                "business_key": "prior-2", "external_task_id": "task-2", "route_module": "materials",
                "route_owner": "苗笑天", "status": "EXTERNAL_CREATED",
                "source_text": "复核回购物资存储货位。", "message_time": "2026-09-21 09:00:00",
            },
        ]
        processor = EventProcessor(
            runtime,
            {"name": "李林骁", "user_id": "SELF"},
            mode="shadow",
            llm_resolver=llm_adapter.JevResolver(
                api_key="fake-key", transport=lambda payload, timeout: {"answers": answers}
            ),
            existing_actions=existing,
        )
        plans = processor.process_batch([{
            "conversation_id": "conv-near-score", "message_id": "new-near-score",
            "group": "铁炉西工班群", "create_time": "2026-09-22 10:00:00",
            "text": "请完成回购物资使用情况核验。",
        }])

        self.assertEqual(plans[0]["decision"], "REVIEW")
        self.assertEqual(plans[0]["reason"], "business_candidate_scores_close")
        self.assertEqual(len(plans[0]["semantic_evidence"]["business_matches"]), 2)
        self.assertEqual(processor.stats["external_todos_created"], 0)

    def test_event_processor_hard_dedupes_same_source_id_before_jev(self):
        calls = []
        runtime = {
            "config_group": [{"group_name": "铁炉西工班群", "conversation_id": "conv-idempotent", "purpose": "todo_scan"}],
            "config_staff": [[1, "李林骁", "SELF", "leader"], [2, "苗笑天", "MIAO", "executor"]],
            "active_routing_loaded": True,
            "responsibility_rules": {"materials": "苗笑天"},
        }

        def transport(payload, timeout):
            calls.append(payload)
            return {"answers": {
                "action_signal": {"type": "noul", "noul": 0.90},
                "item_status": {
                    "type": "choice", "choice": "new",
                    "probabilities": {"new": 0.90, "change": 0.02, "complete": 0.02, "notice": 0.02, "uncertain": 0.04},
                    "confidence": 0.90,
                },
                "due_date": {
                    "type": "choice", "choice": "none",
                    "probabilities": {"none": 0.90, "uncertain": 0.10},
                    "confidence": 0.90,
                },
            }}

        processor = EventProcessor(
            runtime,
            {"name": "李林骁", "user_id": "SELF"},
            mode="shadow",
            llm_resolver=llm_adapter.JevResolver(api_key="fake-key", transport=transport),
        )
        source = {
            "conversation_id": "conv-idempotent", "message_id": "same-message-id",
            "group": "铁炉西工班群", "create_time": "2026-09-22 10:00:00",
            "text": "请完成回购物资核验。",
        }
        plans = processor.process_batch([source, dict(source)])

        self.assertEqual(len(plans), 1)
        self.assertEqual(len(calls), 1)
        self.assertEqual(processor.stats["target_messages"], 1)
        self.assertEqual(plans[0]["decision"], "REVIEW")
        self.assertEqual(plans[0]["reason"], "business_candidate_pool_missing")

    def test_event_processor_uses_llm_semantics_before_final_program_gate(self):
        class FakeResolver:
            def __init__(self):
                self.calls = 0

            def resolve(self, event, plan):
                self.calls += 1
                self.event = event
                self.plan = plan
                return {"semantic_intent": "follow_up", "title": "回购物资领用安排跟进"}

        runtime = {
            "config_group": [
                {"group_name": "铁炉西工班群", "conversation_id": "conv-1", "purpose": "todo_scan"}
            ],
            "config_staff": [[1, "李林骁", "STALE", "leader"], [2, "苗笑天", "MIAO", "executor"]],
            "active_routing_loaded": True,
            "responsibility_rules": {"materials": "苗笑天"},
        }
        resolver = FakeResolver()
        processor = EventProcessor(
            runtime,
            {"name": "李林骁", "user_id": "SELF"},
            mode="shadow",
            llm_resolver=resolver,
        )
        plans = processor.process_batch(
            [{
                "conversationId": "conv-1",
                "messageId": "msg-semantic-1",
                "createTime": "2026-09-22 10:00:00",
                "text": "回购物资仍存放在集中存储货位，需求部门可在9月23日前办理领用手续。",
            }]
        )
        self.assertEqual(resolver.calls, 1)
        self.assertEqual(processor.stats["llm_calls"], 1)
        self.assertEqual(plans[0]["decision"], "CREATE")
        self.assertEqual(plans[0]["semantic_intent"], "follow_up")
        self.assertEqual(plans[0]["route_owner"], "苗笑天")
        self.assertEqual(plans[0]["executor_names"], ["李林骁"])

    def test_unresolved_semantics_stays_review_without_resolver(self):
        runtime = {
            "config_group": [
                {"group_name": "铁炉西工班群", "conversation_id": "conv-1", "purpose": "todo_scan"}
            ],
            "config_staff": [[1, "李林骁", "SELF", "leader"]],
            "active_routing_loaded": True,
        }
        processor = EventProcessor(runtime, {"name": "李林骁", "user_id": "SELF"}, mode="shadow")
        plans = processor.process_batch(
            [{
                "conversationId": "conv-1",
                "messageId": "msg-no-resolver",
                "createTime": "2026-09-22 10:00:00",
                "text": "回购物资仍存放在集中存储货位，需求部门可在9月23日前办理领用手续。",
            }]
        )
        self.assertEqual(plans[0]["decision"], "REVIEW")
        self.assertEqual(plans[0]["reason"], "llm_resolution_required")
        self.assertIn("llm_resolver_unavailable", plans[0]["warnings"])
        self.assertEqual(processor.stats["llm_calls"], 0)

    def test_llm_failure_keeps_unresolved_semantics_in_review(self):
        class FailingResolver:
            def resolve(self, event, plan):
                raise ValueError("test failure")

        runtime = {
            "config_group": [
                {"group_name": "铁炉西工班群", "conversation_id": "conv-1", "purpose": "todo_scan"}
            ],
            "config_staff": [[1, "李林骁", "SELF", "leader"]],
            "active_routing_loaded": True,
        }
        processor = EventProcessor(
            runtime,
            {"name": "李林骁", "user_id": "SELF"},
            mode="shadow",
            llm_resolver=FailingResolver(),
        )
        plans = processor.process_batch(
            [{
                "conversationId": "conv-1",
                "messageId": "msg-resolver-failure",
                "createTime": "2026-09-22 10:00:00",
                "text": "回购物资仍存放在集中存储货位，需求部门可在9月23日前办理领用手续。",
            }]
        )
        self.assertEqual(plans[0]["decision"], "REVIEW")
        self.assertEqual(plans[0]["reason"], "llm_resolution_failed")
        self.assertIn("llm_resolution_failed", plans[0]["warnings"])
        self.assertEqual(
            plans[0]["llm_resolver_error"],
            {"code": "resolver_error", "stage": "validation"},
        )
        self.assertNotIn("test failure", json.dumps(plans[0]["llm_resolver_error"]))
        self.assertEqual(processor.stats["llm_calls"], 1)

    def test_empty_llm_result_keeps_semantic_candidate_in_review(self):
        class EmptyResolver:
            def resolve(self, event, plan):
                return {}

        runtime = {
            "config_group": [
                {"group_name": "铁炉西工班群", "conversation_id": "conv-1", "purpose": "todo_scan"}
            ],
            "config_staff": [[1, "李林骁", "SELF", "leader"]],
            "active_routing_loaded": True,
        }
        processor = EventProcessor(
            runtime,
            {"name": "李林骁", "user_id": "SELF"},
            mode="shadow",
            llm_resolver=EmptyResolver(),
        )
        plans = processor.process_batch(
            [{
                "conversationId": "conv-1",
                "messageId": "msg-empty-resolver",
                "createTime": "2026-09-22 10:00:00",
                "text": "回购物资仍存放在集中存储货位，需求部门可在9月23日前办理领用手续。",
            }]
        )
        self.assertEqual(plans[0]["decision"], "REVIEW")
        self.assertEqual(plans[0]["reason"], "llm_resolution_incomplete")
        self.assertEqual(processor.stats["llm_calls"], 1)

    def test_event_processor_skips_llm_for_existing_source_action(self):
        class FakeResolver:
            def __init__(self):
                self.calls = 0

            def resolve(self, event, plan):
                self.calls += 1
                return {"semantic_intent": "follow_up", "title": "重复事项"}

        runtime = {
            "config_group": [
                {"group_name": "铁炉西工班群", "conversation_id": "conv-1", "purpose": "todo_scan"}
            ],
            "config_staff": [[1, "李林骁", "SELF", "leader"], [2, "苗笑天", "MIAO", "executor"]],
            "active_routing_loaded": True,
            "responsibility_rules": {"materials": "苗笑天"},
        }
        resolver = FakeResolver()
        processor = EventProcessor(
            runtime,
            {"name": "李林骁", "user_id": "SELF"},
            mode="shadow",
            llm_resolver=resolver,
            existing_actions=[
                {"conversation_id": "conv-1", "message_id": "msg-existing", "action_key": "todo"}
            ],
        )
        plans = processor.process_batch(
            [{
                "conversationId": "conv-1",
                "messageId": "msg-existing",
                "createTime": "2026-09-22 10:00:00",
                "text": "回购物资仍存放在集中存储货位，需求部门可办理领用手续。",
            }]
        )
        self.assertEqual(plans[0]["decision"], "IGNORE")
        self.assertEqual(plans[0]["reason"], "source_already_processed")
        self.assertEqual(resolver.calls, 0)
        self.assertEqual(processor.stats["llm_calls"], 0)

    def test_recovered_source_in_later_batch_reuses_in_process_idempotency(self):
        class FakeResolver:
            def __init__(self):
                self.calls = 0

            def resolve(self, event, plan):
                self.calls += 1
                return {"semantic_intent": "follow_up", "title": "回购物资领用安排跟进"}

        runtime = {
            "config_group": [
                {"group_name": "物资调配室", "conversation_id": "conv-repeat", "purpose": "todo_scan"}
            ],
            "config_staff": [[1, "李林骁", "SELF", "leader"], [2, "苗笑天", "MIAO", "executor"]],
            "active_routing_loaded": True,
            "responsibility_rules": {"materials": "苗笑天"},
        }
        resolver = FakeResolver()
        processor = EventProcessor(
            runtime,
            {"name": "李林骁", "user_id": "SELF"},
            mode="shadow",
            llm_resolver=resolver,
        )
        payload = {
            "conversationId": "conv-repeat",
            "messageId": "msg-repeat",
            "createTime": "2026-09-22 10:00:00",
            "text": "回购物资仍存放在集中存储货位，需求部门可办理领用手续。",
        }
        first = processor.process_batch([payload])
        replay = processor.process_batch([payload])
        self.assertEqual(len(first), 1)
        self.assertEqual(replay[0]["decision"], "IGNORE")
        self.assertEqual(replay[0]["reason"], "source_already_processed")
        self.assertEqual(resolver.calls, 1)
        self.assertEqual(processor.stats["llm_calls"], 1)

    def test_production_reuses_llm_enriched_event_for_final_id_planning(self):
        class FakeResolver:
            def resolve(self, event, plan):
                return {"semantic_intent": "follow_up", "title": "回购物资领用安排跟进"}

        class FakeDws:
            def resolve_exact(self, names):
                self.names = list(names)
                return {"苗笑天": "LIVE-MIAO"}

        runtime = {
            "config_group": [
                {"group_name": "铁炉西工班群", "conversation_id": "conv-1", "purpose": "todo_scan"}
            ],
            "config_staff": [[1, "李林骁", "STALE", "leader"], [2, "苗笑天", "MIAO", "executor"]],
            "active_routing_loaded": True,
            "responsibility_rules": {"materials": "苗笑天"},
        }
        dws = FakeDws()
        processor = EventProcessor(
            runtime,
            {"name": "李林骁", "user_id": "SELF"},
            mode="production",
            dws=dws,
            llm_resolver=FakeResolver(),
        )
        plans = processor.process_batch(
            [{
                "conversationId": "conv-1",
                "messageId": "msg-semantic-3",
                "createTime": "2026-09-22 10:00:00",
                "text": "回购物资仍存放在集中存储货位，需求部门可在9月23日前办理领用手续。",
            }]
        )
        self.assertEqual(plans[0]["decision"], "CREATE")
        self.assertEqual(plans[0]["executor_ids"], ["LIVE-MIAO", "SELF"])
        self.assertEqual(plans[0]["llm_used"], True)
        self.assertEqual(plans[0]["llm_fields"], ["semantic_intent", "title"])
        self.assertEqual(dws.names, ["苗笑天"])
        self.assertEqual(processor.stats["external_todos_created"], 0)

    def test_event_processor_rejects_llm_invented_due_date(self):
        class FakeResolver:
            def resolve(self, event, plan):
                return {"semantic_intent": "follow_up", "due_date": "2099-01-01"}

        runtime = {
            "config_group": [
                {"group_name": "铁炉西工班群", "conversation_id": "conv-1", "purpose": "todo_scan"}
            ],
            "config_staff": [[1, "李林骁", "STALE", "leader"], [2, "苗笑天", "MIAO", "executor"]],
            "active_routing_loaded": True,
            "responsibility_rules": {"materials": "苗笑天"},
        }
        processor = EventProcessor(
            runtime,
            {"name": "李林骁", "user_id": "SELF"},
            mode="shadow",
            llm_resolver=FakeResolver(),
        )
        plans = processor.process_batch(
            [{
                "conversationId": "conv-1",
                "messageId": "msg-semantic-2",
                "createTime": "2026-09-22 10:00:00",
                "text": "回购物资仍存放在集中存储货位，需求部门可办理领用手续。",
            }]
        )
        self.assertEqual(plans[0]["decision"], "REVIEW")
        self.assertIn("due_date", plans[0]["missing_fields"])
        self.assertIn("llm_due_date_not_explicit_in_source", plans[0]["warnings"])

    def test_event_processor_uses_existing_planner_and_shadow_has_no_writes(self):
        runtime = {
            "config_group": [
                {"group_name": "铁炉西工班群", "conversation_id": "conv-1", "purpose": "todo_scan"}
            ],
            "config_staff": [[1, "李林骁", "STALE", "leader"], [2, "谭继衡", "TAN", "executor"]],
            "active_routing_loaded": True,
            "responsibility_rules": {"ledger": "谭继衡"},
            "handover_evidence": [{
                "period": "2026-09", "area_name": "杂品库", "active_owners": ["谭继衡"],
                "reviewed": True, "status": "accepted", "hygiene_review": "passed",
                "source": "dws_readback", "evidence_id": "synthetic-shadow-handover",
            }],
        }
        processor = EventProcessor(runtime, {"name": "李林骁", "user_id": "SELF"}, mode="shadow")
        plans = processor.process_batch(
            [
                {
                    "eventType": "user_im_message_receive_group_all",
                    "conversationId": "conv-1",
                    "messageId": "msg-1",
                    "createTime": "2026-09-21 10:00:00",
                    "sender": "张云云",
                    "text": "请于9月22日完成危废台账核对整改。",
                }
            ]
        )
        self.assertEqual(plans[0]["decision"], "CREATE")
        self.assertEqual(plans[0]["executor_names"], ["李林骁"])
        self.assertEqual(processor.stats["external_todos_created"], 0)
        self.assertEqual(processor.stats["ledger_writes"], 0)

    def test_event_processor_rejects_authored_forward_and_ack_reply(self):
        runtime = {
            "config_group": [{"group_name": "铁炉西工班群", "conversation_id": "conv-1", "purpose": "todo_scan"}],
            "config_staff": [[1, "李林骁", "SELF", "leader"]],
            "active_routing_loaded": True,
            "responsibility_rules": {"leader_inspection": "李林骁"},
        }
        processor = EventProcessor(runtime, {"name": "李林骁", "user_id": "SELF"}, mode="shadow")
        plans = processor.process_batch([
            {"conversationId": "conv-1", "messageId": "msg-own", "createTime": "2026-09-24 15:19:07",
             "sender": "李林骁", "senderId": "SELF", "text": "请各工班长收到后回复确认，并传达到班组每个人。@李林骁(李林骁)"},
            {"conversationId": "conv-1", "messageId": "msg-reply", "createTime": "2026-09-24 10:25:54",
             "sender": "杨梦卓", "text": "@李林骁(李林骁) 无问题"},
        ])
        self.assertEqual([plan["decision"] for plan in plans], ["REVIEW", "IGNORE"])
        self.assertEqual(processor.stats["external_todos_created"], 0)

    def test_event_processor_accepts_dws_schema_content_for_target_group(self):
        runtime = {
            "config_group": [
                {"group_name": "铁炉西工班群", "conversation_id": "conv-schema", "purpose": "todo_scan"}
            ],
            "config_staff": [[1, "李林骁", "STALE", "leader"], [2, "杨梦卓", "YANG", "executor"]],
            "active_routing_loaded": True,
            "responsibility_rules": {"hazardous_waste": "杨梦卓"},
            "handover_evidence": [{
                "period": "2026-09", "area_name": "杂品库", "active_owners": ["杨梦卓"],
                "reviewed": True, "status": "accepted", "hygiene_review": "passed",
                "source": "dws_readback", "evidence_id": "synthetic-prod-junk-handover",
            }],
        }
        processor = EventProcessor(runtime, {"name": "李林骁", "user_id": "SELF"}, mode="shadow")
        plans = processor.process_batch(
            [{
                "type": "user_im_message_receive_group_all",
                "conversation_id": "conv-schema",
                "message_id": "msg-schema",
                "create_time": "2026-09-21 10:00:00",
                "content": "请于9月22日完成危废台账核对整改。",
            }]
        )
        self.assertEqual(plans[0]["decision"], "CREATE")
        self.assertEqual(plans[0]["route_owner"], "杨梦卓")

    def test_production_resolves_live_owner_id_before_planning_create(self):
        class FakeDws:
            def resolve_exact(self, names):
                self.names = list(names)
                return {"杨梦卓": "LIVE-YANG"}

        runtime = {
            "config_group": [
                {"group_name": "铁炉西工班群", "conversation_id": "conv-1", "purpose": "todo_scan"}
            ],
            "config_staff": [[1, "李林骁", "STALE", "leader"], [2, "杨梦卓", "YANG", "executor"], [3, "谭继衡", "TAN", "executor"]],
            "active_routing_loaded": True,
            "responsibility_rules": {"ledger": "谭继衡"},
            "handover_evidence": [{
                "period": "2026-09", "area_name": "杂品库", "active_owners": ["杨梦卓"],
                "reviewed": True, "status": "accepted", "hygiene_review": "passed",
                "source": "dws_readback", "evidence_id": "synthetic-production-owner-handover",
            }],
        }
        dws = FakeDws()
        processor = EventProcessor(runtime, {"name": "李林骁", "user_id": "SELF"}, mode="production", dws=dws)
        plans = processor.process_batch(
            [{
                "conversationId": "conv-1",
                "messageId": "msg-prod-1",
                "createTime": "2026-09-21 10:00:00",
                "text": "请于9月22日完成危废台账核对整改。",
            }]
        )
        self.assertEqual(plans[0]["decision"], "CREATE")
        self.assertEqual(plans[0]["executor_ids"], ["LIVE-YANG", "SELF"])
        self.assertEqual(dws.names, ["杨梦卓"])

    def test_event_processor_drops_non_target_group_before_planning(self):
        runtime = {
            "config_group": [
                {"group_name": "物资调配室", "conversation_id": "target", "purpose": "todo_scan"}
            ],
            "config_staff": [[1, "李林骁", "STALE", "leader"]],
            "active_routing_loaded": True,
        }
        processor = EventProcessor(runtime, {"name": "李林骁", "user_id": "SELF"}, mode="shadow")
        plans = processor.process_batch(
            [{"conversationId": "other", "messageId": "m", "text": "请完成任务", "createTime": "2026-09-21 10:00:00"}]
        )
        self.assertEqual(plans, [])
        self.assertEqual(processor.stats["target_messages"], 0)
    def test_external_create_timeout_is_review_and_never_retried(self):
        class FakeWriter:
            def __init__(self):
                self.requests = []
            def execute(self, request):
                self.requests.append(request)
                if request["operation"] == "claim_action":
                    return {"created": True, "action_id": 9}
                return {"ok": True, "committed": True, "action_id": request.get("action_id"), "row": {"status": request.get("status"), "external_task_id": request.get("external_task_id")}}

        class FakeDws:
            def __init__(self): self.create_calls = 0
            def resolve_exact(self, names): return {name: "LIVE-" + name for name in names}
            def search_todos(self, query): return []
            def create_todo(self, plan, priority=20):
                self.create_calls += 1
                raise TimeoutError("simulated timeout")

        runtime = {
            "config_group": [{"group_name": "铁炉西工班群", "conversation_id": "conv-1", "purpose": "todo_scan"}],
            "config_staff": [[1, "李林骁", "STALE", "leader"], [2, "谭继衡", "TAN", "executor"]],
            "active_routing_loaded": True, "responsibility_rules": {"ledger": "谭继衡"},
            "handover_evidence": [{
                "period": "2026-09", "area_name": "杂品库", "active_owners": ["谭继衡"],
                "reviewed": True, "status": "accepted", "hygiene_review": "passed",
                "source": "dws_readback", "evidence_id": "synthetic-create-timeout-handover",
            }],
        }
        writer, dws = FakeWriter(), FakeDws()
        processor = EventProcessor(runtime, {"name": "李林骁", "user_id": "SELF"}, mode="production",
            write_ledger=True, allow_external_writes=True, dws=dws, writer=writer)
        event = {"conversationId": "conv-1", "messageId": "safe-src-1", "createTime": "2026-09-21 10:00:00",
            "text": "请于9月22日完成危废台账核对整改。"}
        first = processor.process_batch([event])
        second = processor.process_batch([event])
        self.assertEqual(first[0]["decision"], "REVIEW")
        self.assertEqual(second[0]["decision"], "IGNORE")
        self.assertEqual(dws.create_calls, 1)
        statuses = [r.get("status") for r in writer.requests if r["operation"] == "save_action_result"]
        self.assertEqual(statuses, ["CLAIMED", "CLAIMED"])

    def test_event_processor_does_not_recreate_existing_claim(self):
        class FakeWriter:
            def execute(self, request):
                if request["operation"] == "claim_action":
                    return {"created": False, "action_id": 9, "reason": "source_exists"}
                return {"created": False, "changed": False}

        class FakeDws:
            def __init__(self):
                self.create_calls = 0

            def resolve_exact(self, names):
                return {name: "LIVE-" + name for name in names}

            def create_todo(self, plan, priority=20):
                self.create_calls += 1
                return {"task_id": "unexpected"}

        runtime = {
            "config_group": [
                {"group_name": "铁炉西工班群", "conversation_id": "conv-1", "purpose": "todo_scan"}
            ],
            "config_staff": [[1, "李林骁", "STALE", "leader"], [2, "谭继衡", "TAN", "executor"]],
            "active_routing_loaded": True,
            "responsibility_rules": {"ledger": "谭继衡"},
        }
        dws = FakeDws()
        processor = EventProcessor(
            runtime,
            {"name": "李林骁", "user_id": "SELF"},
            mode="production",
            write_ledger=True,
            allow_external_writes=True,
            dws=dws,
            writer=FakeWriter(),
        )
        processor.process_batch(
            [{
                "conversationId": "conv-1",
                "messageId": "msg-1",
                "createTime": "2026-09-21 10:00:00",
                "text": "请于9月22日完成危废台账核对整改。",
            }]
        )
        self.assertEqual(dws.create_calls, 0)

    def test_created_claim_without_action_id_fails_closed(self):
        class FakeWriter:
            def execute(self, request):
                if request["operation"] == "claim_action":
                    return {"created": True}
                return {"ok": True, "committed": True, "action_id": request.get("action_id"), "row": {"status": request.get("status"), "external_task_id": request.get("external_task_id")}}
        class FakeDws:
            create_calls = 0
            def resolve_exact(self, names): return {name: "LIVE-" + name for name in names}
            def search_todos(self, query): return []
            def create_todo(self, plan, priority=20):
                self.create_calls += 1
                return {"task_id": "should-not-exist"}
            def add_source_comment(self, *args): pass
            def verify_source_comment(self, *args): pass
            def get_todo(self, *args): return {}
        runtime = {"config_group": [{"group_name": "铁炉西工班群", "conversation_id": "conv-1", "purpose": "todo_scan"}],
                   "config_staff": [[1, "李林骁", "SELF", "leader"], [2, "谭继衡", "TAN", "executor"]],
                   "active_routing_loaded": True, "responsibility_rules": {"ledger": "谭继衡"},
                   "handover_evidence": [{
                       "period": "2026-09", "area_name": "杂品库", "active_owners": ["谭继衡"],
                       "reviewed": True, "status": "accepted", "hygiene_review": "passed",
                       "source": "dws_readback", "evidence_id": "synthetic-link-failure-handover",
                   }]}
        dws = FakeDws()
        processor = EventProcessor(runtime, {"name": "李林骁", "user_id": "SELF"}, mode="production",
            write_ledger=True, allow_external_writes=True, dws=dws, writer=FakeWriter())
        plans = processor.process_batch([{"conversationId": "conv-1", "messageId": "missing-id",
            "createTime": "2026-09-21 10:00:00", "text": "请于9月22日完成危废台账核对整改。"}])
        self.assertEqual(dws.create_calls, 0)
        self.assertEqual(plans[0]["decision"], "REVIEW")
        self.assertIn("durable_create_claim_missing_action_id", plans[0]["warnings"])

    def test_link_failure_after_create_does_not_recreate_claim_on_restart(self):
        class FakeWriter:
            def __init__(self): self.claimed = False
            def execute(self, request):
                if request["operation"] == "claim_action":
                    if self.claimed: return {"created": False, "action_id": 8}
                    self.claimed = True
                    return {"created": True, "action_id": 8}
                if request["operation"] == "link_external_task": raise RuntimeError("link failed")
                return {"ok": True, "committed": True, "action_id": request.get("action_id"), "row": {"status": request.get("status"), "external_task_id": request.get("external_task_id")}}
        class FakeDws:
            def __init__(self): self.create_calls = 0
            def resolve_exact(self, names): return {name: "LIVE-" + name for name in names}
            def search_todos(self, query): return []
            def create_todo(self, plan, priority=20):
                self.create_calls += 1
                return {"task_id": "remote-1"}
            def add_source_comment(self, *args): pass
            def verify_source_comment(self, *args): pass
        runtime = {"config_group": [{"group_name": "铁炉西工班群", "conversation_id": "conv-1", "purpose": "todo_scan"}],
                   "config_staff": [[1, "李林骁", "SELF", "leader"], [2, "谭继衡", "TAN", "executor"]],
                   "active_routing_loaded": True, "responsibility_rules": {"ledger": "谭继衡"},
                   "handover_evidence": [{
                       "period": "2026-09", "area_name": "杂品库", "active_owners": ["谭继衡"],
                       "reviewed": True, "status": "accepted", "hygiene_review": "passed",
                       "source": "dws_readback", "evidence_id": "synthetic-link-failure-handover",
                   }]}
        writer, dws = FakeWriter(), FakeDws()
        event = {"conversationId": "conv-1", "messageId": "link-fail", "createTime": "2026-09-21 10:00:00",
                 "text": "请于9月22日完成危废台账核对整改。"}
        for _ in range(2):
            EventProcessor(runtime, {"name": "李林骁", "user_id": "SELF"}, mode="production",
                write_ledger=True, allow_external_writes=True, dws=dws, writer=writer).process_batch([event])
        self.assertEqual(dws.create_calls, 1)

    def test_confirmed_comment_failure_after_link_does_not_recreate_same_source_on_restart(self):
        class DurableWriter:
            def __init__(self):
                self.claimed = False
                self.operations = []

            def execute(self, request):
                self.operations.append(request["operation"])
                if request["operation"] == "claim_action":
                    if self.claimed:
                        return {"created": False, "action_id": 19, "reason": "source_exists"}
                    self.claimed = True
                    return {"created": True, "action_id": 19}
                if request["operation"] == "link_external_task":
                    return {"ok": True, "committed": True, "action_id": request["action_id"],
                            "row": {"external_task_id": request["external_task_id"]}, "linked": True}
                return {"ok": True, "committed": True, "action_id": request.get("action_id"), "row": {"status": request.get("status"), "external_task_id": request.get("external_task_id")}}

        class AtomicCreateDws(DwsClient):
            def __init__(self):
                super().__init__(executable="dws-test", confirmed_official_writes=True)
                self.create_calls = 0

            def resolve_exact(self, names):
                return {name: "LIVE-" + name for name in names}

            def create_todo(self, plan, priority=20):
                self.create_calls += 1
                return {"task_id": "remote-19"}

        runtime = {
            "config_group": [{"group_name": "铁炉西工班群", "conversation_id": "conv-1", "purpose": "todo_scan"}],
            "config_staff": [[1, "李林骁", "SELF", "leader"], [2, "谭继衡", "TAN", "executor"]],
            "active_routing_loaded": True, "responsibility_rules": {"ledger": "谭继衡"},
            "handover_evidence": [{
                "period": "2026-09", "area_name": "杂品库", "active_owners": ["谭继衡"],
                "reviewed": True, "status": "accepted", "hygiene_review": "passed",
                "source": "dws_readback", "evidence_id": "synthetic-comment-failure-handover",
            }],
        }
        writer, dws, comment_commands = DurableWriter(), AtomicCreateDws(), []
        event = {
            "conversationId": "conv-1", "messageId": "comment-fail-source",
            "createTime": "2026-09-21 10:00:00", "text": "请于9月22日完成危废台账核对整改。",
        }

        def fake_run(command, **kwargs):
            if command[1:3] == ["todo", "+search"]:
                return subprocess.CompletedProcess(
                    command, 0,
                    json.dumps({
                        "ok": True,
                        "outcome": "success",
                        "data": {"complete": True, "count": 0, "todos": []},
                    }),
                    "",
                )
            if command[1:3] == ["todo", "+comment"]:
                comment_commands.append(command)
                return subprocess.CompletedProcess(
                    command, 1,
                    json.dumps({"ok": False, "outcome": "failure", "error": {"reason": "confirmation_required"}}),
                    "",
                )
            raise AssertionError(f"unexpected DWS invocation: {command!r}")

        processor_kwargs = {
            "mode": "production", "write_ledger": True, "allow_external_writes": True,
            "dws": dws, "writer": writer,
        }
        with patch("dws_adapter.subprocess.run", side_effect=fake_run):
            first = EventProcessor(runtime, {"name": "李林骁", "user_id": "SELF"}, **processor_kwargs).process_batch([event])
            second = EventProcessor(runtime, {"name": "李林骁", "user_id": "SELF"}, **processor_kwargs).process_batch([event])

        self.assertEqual(first[0]["decision"], "REVIEW")
        self.assertEqual(dws.create_calls, 1)
        self.assertEqual(len(comment_commands), 1)
        self.assertIn("--yes", comment_commands[0])
        self.assertIn("save_action_result", writer.operations[writer.operations.index("link_external_task") + 1:])
        self.assertEqual(second[0]["decision"], "CREATE")


class OneShotPollingTests(unittest.TestCase):
    def test_late_event_replays_by_time_through_the_shared_source_ledger(self):
        from listener_engine import AtomicPendingState, CatchupCoordinator, WakeupRouter

        run_at = datetime.fromisoformat("2026-11-07T10:00:00+08:00")
        older_event_time = "2026-11-07T08:55:00+08:00"
        newer_event_time = "2026-11-07T09:00:00+08:00"
        conversation_id = "synthetic-replay-conversation"
        messages = [
            {
                "conversation_id": conversation_id,
                "message_id": "synthetic-older-message",
                "create_time": older_event_time,
                "group": "synthetic-replay-group",
                "sender_id": "synthetic-sender",
                "text": "请核对本周设备清单并反馈。",
            },
            {
                "conversation_id": conversation_id,
                "message_id": "synthetic-newer-message",
                "create_time": newer_event_time,
                "group": "synthetic-replay-group",
                "sender_id": "synthetic-sender",
                "text": "请复核上述设备清单。",
            },
        ]
        runtime = {
            "config_calendar": [{"dt": run_at.date().isoformat(), "day_type": "workday"}],
            "config_group": [{
                "group_name": "synthetic-replay-group",
                "conversation_id": conversation_id,
                "purpose": "todo_scan",
            }],
            "config_staff": [],
            "active_routing_loaded": True,
        }

        class FakeAdapter:
            def query(self, request):
                if request["view"] == "runtime_config":
                    return {"config": runtime}
                if request["view"] == "scan_state":
                    return {"rows": [{
                        "conversation_id": conversation_id,
                        "last_msg_time": "2026-11-07T09:59:00+08:00",
                        "last_msg_id": "synthetic-newer-cursor",
                    }], "has_more": False}
                if request["view"] == "actions":
                    return {"rows": list(writer.actions), "has_more": False}
                raise AssertionError(request["view"])

        class FakeReader:
            def __init__(self):
                self.calls = []

            def read_group(self, **kwargs):
                self.calls.append(kwargs)
                return {
                    "complete": True,
                    "source_order_provenance": "complete_dws_chronological_page",
                    "messages": [dict(message) for message in messages],
                }

        class FakeDws:
            def get_self(self):
                return {"name": "synthetic-self", "user_id": "synthetic-self-id"}

            def create_todo(self, *_args, **_kwargs):
                raise AssertionError("offline replay must not create a todo")

        class FakeWriter:
            def __init__(self):
                self.requests = []
                self.actions = []

            def contract_info(self):
                return {
                    "contract_version": "polardb-work-writer/0.2.0",
                    "schema_supported": True,
                    "operations": [
                        "claim_child_action", "save_assignment_audit",
                        "link_external_task", "save_action_result",
                    ],
                }

            def execute(self, request):
                self.requests.append(dict(request))
                if request["operation"] == "claim_action":
                    self.actions.append({
                        "conversation_id": request["conversation_id"],
                        "message_id": request["message_id"],
                        "action_key": request["action_key"],
                        "status": request["status"],
                    })
                    return {"created": True, "action_id": len(self.actions)}
                return {"created": False, "changed": False}

        writer = FakeWriter()
        adapter = FakeAdapter()
        reader = FakeReader()
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        state = AtomicPendingState(Path(temporary_directory.name) / "state")
        router = WakeupRouter(conversation_id, (conversation_id,), state)
        newer_event = {
            "conversationId": conversation_id,
            "messageId": "synthetic-newer-message",
            "createTime": newer_event_time,
            "text": "not persisted by the wakeup state",
        }
        older_event = {
            "conversationId": conversation_id,
            "messageId": "synthetic-older-message",
            "createTime": older_event_time,
            "text": "not persisted by the wakeup state",
        }
        self.assertTrue(router.handle(newer_event))
        self.assertTrue(router.handle(older_event))
        saved_wake_state = state.path.read_text(encoding="utf-8")
        saved_history_state = state.history_path.read_text(encoding="utf-8")
        self.assertNotIn("not persisted by the wakeup state", saved_wake_state + saved_history_state)
        self.assertNotIn("synthetic-older-message", saved_wake_state + saved_history_state)
        self.assertNotIn("synthetic-newer-message", saved_wake_state + saved_history_state)
        poller = event_worker.OneShotHistoryPoller(
            adapter=adapter,
            reader=reader,
            dws=FakeDws(),
            writer=writer,
            mode="production",
            write_ledger=True,
            now_factory=lambda: run_at,
            history_replay_state=state,
        )
        coordinator = CatchupCoordinator(
            state, poller.run_once, state.state_dir / "work.lock",
            mode="production", write_ledger=True,
        )
        with patch.object(DwsParityLedger, "from_environment", return_value=None):
            result = coordinator.run_once()
            self.assertEqual(result["status"], "COMPLETED")
            self.assertEqual(reader.calls[0]["last_msg_time"], older_event_time)
            self.assertEqual(state.history_event_snapshot(), {})
            self.assertEqual(
                [
                    request["operation"] for request in writer.requests
                    if request["operation"] in {"record_message", "claim_action"}
                ],
                ["record_message", "claim_action", "record_message", "claim_action"],
            )

            self.assertTrue(router.handle(newer_event))
            repeated = coordinator.run_once()
            self.assertEqual(repeated["status"], "COMPLETED")
            self.assertEqual(reader.calls[1]["last_msg_time"], newer_event_time)
            self.assertEqual(
                [request["operation"] for request in writer.requests].count("claim_action"),
                2,
            )
            self.assertEqual(state.snapshot(), {})

    def test_shadow_complete_history_replay_preserves_hint_until_production_resident_ledger_write(self):
        import threading

        from listener_engine import AtomicPendingState, CatchupCoordinator, WakeupRouter
        from polling_resident import _run_cycle

        run_at = datetime.fromisoformat("2026-11-07T10:00:00+08:00")
        event_time = "2026-11-07T09:15:00+08:00"
        conversation_id = "synthetic-shadow-replay-conversation"
        runtime = {
            "config_calendar": [{"dt": run_at.date().isoformat(), "day_type": "workday"}],
            "config_group": [{
                "group_name": "synthetic-shadow-replay-group",
                "conversation_id": conversation_id,
                "purpose": "todo_scan",
            }],
            "config_staff": [],
            "active_routing_loaded": True,
        }

        class FakeAdapter:
            def query(self, request):
                if request["view"] == "runtime_config":
                    return {"config": runtime}
                if request["view"] in {"scan_state", "actions"}:
                    return {"rows": [], "has_more": False}
                raise AssertionError(request["view"])

        class FakeReader:
            def __init__(self):
                self.calls = []

            def read_group(self, **kwargs):
                self.calls.append(kwargs)
                return {
                    "complete": True,
                    "source_order_provenance": "complete_dws_chronological_page",
                    "messages": [{
                        "conversation_id": conversation_id,
                        "message_id": "synthetic-shadow-replay-message",
                        "create_time": event_time,
                        "group": "synthetic-shadow-replay-group",
                        "sender_id": "synthetic-shadow-sender",
                        "text": "合成消息，仅用于离线回放测试。",
                    }],
                }

        class FakeDws:
            def get_self(self):
                return {"name": "synthetic-self", "user_id": "synthetic-self-id"}

        class FakeProcessor:
            def __init__(self, *_args, **_kwargs):
                self.stats = {"errors": 0}

            def process_batch(self, _messages):
                return []

        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        state = AtomicPendingState(Path(temporary_directory.name) / "state")
        router = WakeupRouter(conversation_id, (conversation_id,), state)
        event = {
            "conversationId": conversation_id,
            "messageId": "synthetic-shadow-replay-message",
            "createTime": event_time,
        }
        self.assertTrue(router.handle(event))
        original_hint = state.history_event_snapshot()
        reader = FakeReader()
        shadow_poller = event_worker.OneShotHistoryPoller(
            adapter=FakeAdapter(),
            reader=reader,
            dws=FakeDws(),
            mode="shadow",
            write_ledger=False,
            processor_factory=FakeProcessor,
            now_factory=lambda: run_at,
            history_replay_state=state,
        )
        shadow_coordinator = CatchupCoordinator(
            state, shadow_poller.run_once, state.state_dir / "work.lock",
            mode="shadow", write_ledger=False,
        )
        with patch.object(DwsParityLedger, "from_environment", return_value=None):
            shadow_result = shadow_coordinator.run_once()

        self.assertEqual(shadow_result["status"], "COMPLETED")
        self.assertEqual(len(reader.calls), 1)
        self.assertEqual(reader.calls[0]["last_msg_time"], event_time)
        self.assertEqual(state.history_event_snapshot(), original_hint)

        ledger_requests = []

        class FakeLedgerWriter:
            def __init__(self, poller):
                self.poller = poller

            def execute(self, request):
                if self.poller.history_replay_state.history_event_snapshot() != original_hint:
                    raise AssertionError("resident acknowledged replay hint before ledger write")
                ledger_requests.append(dict(request))
                return {"created": True, "action_id": 1}

        class ProductionResidentPoller:
            mode = "production"
            write_ledger = True

            def __init__(self):
                self.history_replay_state = None
                self.writer = FakeLedgerWriter(self)

            def run_once(self):
                self.writer.execute({
                    "operation": "record_message",
                    "conversation_id": conversation_id,
                    "message_id": "synthetic-shadow-replay-message",
                })
                return {"status": "COMPLETED", "history_replay_complete": True}

        resident_outcome = _run_cycle(
            ProductionResidentPoller(),
            state=state,
            scope_loader=lambda: [conversation_id],
            lock_path=state.state_dir / "work.lock",
            stop_event=threading.Event(),
        )
        self.assertEqual(resident_outcome["status"], "COMPLETED")
        self.assertEqual([request["operation"] for request in ledger_requests], ["record_message"])
        self.assertEqual(state.history_event_snapshot(), {})

    def test_event_without_time_stays_auditable_and_incomplete_history_never_acks(self):
        from listener_engine import AtomicPendingState, CatchupCoordinator, WakeupRouter

        run_at = datetime.fromisoformat("2026-11-07T10:00:00+08:00")
        conversation_id = "synthetic-incomplete-conversation"
        runtime = {
            "config_calendar": [{"dt": run_at.date().isoformat(), "day_type": "workday"}],
            "config_group": [{
                "group_name": "synthetic-incomplete-group",
                "conversation_id": conversation_id,
                "purpose": "todo_scan",
            }],
            "config_staff": [],
            "active_routing_loaded": True,
        }

        class FakeAdapter:
            def query(self, request):
                if request["view"] == "runtime_config":
                    return {"config": runtime}
                if request["view"] in {"scan_state", "actions"}:
                    return {"rows": [], "has_more": False}
                raise AssertionError(request["view"])

        class FakeReader:
            def __init__(self):
                self.calls = 0

            def read_group(self, **_kwargs):
                self.calls += 1
                return {
                    "complete": True,
                    "source_order_provenance": "complete_dws_chronological_page",
                    "messages": [],
                }

        class FakeDws:
            def get_self(self):
                return {"name": "synthetic-self", "user_id": "synthetic-self-id"}

        with tempfile.TemporaryDirectory() as directory:
            state = AtomicPendingState(Path(directory) / "state")
            router = WakeupRouter(conversation_id, (conversation_id,), state)
            self.assertTrue(router.handle({
                "conversationId": conversation_id,
                "createTime": "2026-11-07T09:00:00+08:00",
            }))
            self.assertTrue(router.handle({
                "conversationId": conversation_id,
                "messageId": "synthetic-no-time",
            }))
            self.assertEqual(
                state.history_event_errors_snapshot(),
                {conversation_id: [
                    "history_event_message_id_missing", "history_event_time_missing",
                ]},
            )
            reader = FakeReader()
            poller = event_worker.OneShotHistoryPoller(
                adapter=FakeAdapter(), reader=reader, dws=FakeDws(),
                mode="test", now_factory=lambda: run_at,
                history_replay_state=state,
            )
            coordinator = CatchupCoordinator(state, poller.run_once, Path(directory) / "work.lock")
            result = coordinator.run_once()
            self.assertEqual(result["status"], "REVIEW")
            self.assertEqual(result["reason"], "history_event_identity_unavailable")
            self.assertEqual(reader.calls, 0)
            self.assertEqual(state.history_event_errors_snapshot()[conversation_id], [
                "history_event_message_id_missing", "history_event_time_missing",
            ])
            self.assertIn(conversation_id, state.snapshot())

    def test_complete_history_page_missing_event_identity_remains_pending(self):
        from listener_engine import AtomicPendingState, CatchupCoordinator, WakeupRouter

        run_at = datetime.fromisoformat("2026-11-07T10:00:00+08:00")
        conversation_id = "synthetic-late-history-conversation"
        event_time = "2026-11-07T09:00:00+08:00"
        runtime = {
            "config_calendar": [{"dt": run_at.date().isoformat(), "day_type": "workday"}],
            "config_group": [{
                "group_name": "synthetic-late-history-group",
                "conversation_id": conversation_id,
                "purpose": "todo_scan",
            }],
            "config_staff": [],
            "active_routing_loaded": True,
        }

        class FakeAdapter:
            def query(self, request):
                if request["view"] == "runtime_config":
                    return {"config": runtime}
                if request["view"] in {"scan_state", "actions"}:
                    return {"rows": [], "has_more": False}
                raise AssertionError(request["view"])

        class FakeReader:
            def __init__(self):
                self.calls = []
                self.complete = False

            def read_group(self, **kwargs):
                self.calls.append(kwargs)
                return {
                    "complete": self.complete,
                    "source_order_provenance": "complete_dws_chronological_page",
                    "messages": [],
                }

        class FakeDws:
            def get_self(self):
                return {"name": "synthetic-self", "user_id": "synthetic-self-id"}

        with tempfile.TemporaryDirectory() as directory:
            state = AtomicPendingState(Path(directory) / "state")
            router = WakeupRouter(conversation_id, (conversation_id,), state)
            router.handle({
                "conversationId": conversation_id,
                "messageId": "synthetic-late-history-message",
                "createTime": event_time,
            })
            reader = FakeReader()
            poller = event_worker.OneShotHistoryPoller(
                adapter=FakeAdapter(), reader=reader, dws=FakeDws(), mode="test",
                now_factory=lambda: run_at, history_replay_state=state,
            )
            coordinator = CatchupCoordinator(state, poller.run_once, Path(directory) / "work.lock")
            result = coordinator.run_once()
            self.assertEqual(result["status"], "REVIEW")
            self.assertEqual(result["reason"], "history_not_complete_or_valid")
            self.assertEqual(reader.calls[0]["last_msg_time"], event_time)
            self.assertTrue(state.history_event_snapshot()[conversation_id])
            self.assertIn(conversation_id, state.snapshot())
            reader.complete = True
            result = coordinator.run_once(force=True)
            self.assertEqual(result["status"], "REVIEW")
            self.assertEqual(result["reason"], "history_event_not_in_complete_scan")
            self.assertTrue(state.history_event_snapshot()[conversation_id])
            self.assertEqual(len(reader.calls), 2)

    def test_history_poller_records_only_complete_scan_identities_in_parity_ledger(self):
        run_at = datetime.fromisoformat("2026-09-22T10:00:00+08:00")
        message = {
            "conversation_id": "conv-parity",
            "message_id": "msg-parity",
            "create_time": "2026-09-22 09:30:00",
            "text": "private historical message",
        }
        runtime = {
            "config_calendar": [{"dt": "2026-09-22", "day_type": "workday"}],
            "config_group": [{
                "group_name": "synthetic-group", "conversation_id": "conv-parity", "purpose": "todo_scan",
            }],
            "config_staff": [],
            "active_routing_loaded": True,
        }

        class FakeAdapter:
            def query(self, request):
                if request["view"] == "runtime_config":
                    return {"config": runtime}
                if request["view"] in {"scan_state", "actions"}:
                    return {"rows": [], "has_more": False}
                raise AssertionError(request)

        class FakeReader:
            def read_group(self, **_kwargs):
                return {
                    "complete": True,
                    "source_order_provenance": "complete_dws_chronological_page",
                    "messages": [message],
                }

        class FakeDws:
            def get_self(self):
                return {"name": "synthetic-self", "user_id": "self-id"}

        class FakeProcessor:
            def __init__(self, *_args, **_kwargs):
                self.stats = {"errors": 0}

            def process_batch(self, _messages):
                return []

        with tempfile.TemporaryDirectory() as directory:
            state_dir = Path(directory) / "parity"
            ledger = DwsParityLedger(state_dir, now=lambda: run_at.timestamp() - 3600)
            with patch.object(DwsParityLedger, "from_environment", return_value=ledger):
                poller = event_worker.OneShotHistoryPoller(
                    adapter=FakeAdapter(), reader=FakeReader(), dws=FakeDws(),
                    mode="shadow", processor_factory=FakeProcessor, now_factory=lambda: run_at,
                )
                result = poller.run_once()

            self.assertEqual(result["status"], "COMPLETED")
            self.assertEqual(result["parity_observer"]["history_messages_added"], 1)
            self.assertEqual(result["parity_observer"]["complete_history_scans"], 1)
            self.assertEqual(ledger.summary()["history_messages"], 1)
            self.assertNotIn("private historical message", (state_dir / "event-history-parity.json").read_text())

    def test_jev_poll_hydrates_linked_candidate_context_before_processing(self):
        runtime = {
            "config_calendar": [{"dt": "2026-09-22", "day_type": "workday"}],
            "config_group": [{"group_name": "铁炉西工班群", "conversation_id": "conv-current", "purpose": "todo_scan"}],
            "config_staff": [[1, "李林骁", "SELF", "leader"], [2, "苗笑天", "MIAO", "executor"]],
            "active_routing_loaded": True,
            "responsibility_rules": {"materials": "苗笑天"},
        }

        class FakeAdapter:
            def __init__(self):
                self.requests = []

            def query(self, request):
                self.requests.append(dict(request))
                if request["view"] == "runtime_config":
                    return {"config": runtime}
                if request["view"] == "scan_state":
                    return {"rows": [], "has_more": False}
                if request["view"] == "actions":
                    return {"rows": [{
                        "conversation_id": "conv-current",
                        "message_id": "prior-msg",
                        "business_key": "prior-key",
                        "status": "EXTERNAL_CREATED",
                        "external_task_id": "prior-task-id",
                        "created_at": "2026-09-20 09:00:00",
                    }], "has_more": False}
                if request["view"] == "messages":
                    return {"rows": [{
                        "conversation_id": "conv-current",
                        "message_id": "prior-msg",
                        "group_name": "铁炉西工班群",
                        "sender_id": "prior-sender-id",
                        "sender_name": "王萌",
                        "message_time": "2026-09-20 08:30:00",
                        "text": "请于9月24日前完成回购物资核验工作。",
                    }], "has_more": False}
                raise AssertionError(request)

        class FakeReader:
            def read_group(self, **kwargs):
                return {"complete": True, "messages": []}

        class FakeDws:
            def get_self(self):
                return {"name": "李林骁", "user_id": "SELF"}

        class FakeProcessor:
            def __init__(self, *args, **kwargs):
                self.kwargs = kwargs
                self.stats = {}
                observed["processor_kwargs"] = kwargs

            def process_batch(self, messages):
                self.messages = list(messages)
                return []

        observed = {}
        adapter = FakeAdapter()
        poller = event_worker.OneShotHistoryPoller(
            adapter=adapter,
            reader=FakeReader(),
            dws=FakeDws(),
            resolver=llm_adapter.JevResolver(api_key="fake-key", transport=lambda *_: self.fail("no API call")),
            processor_factory=FakeProcessor,
            now_factory=lambda: datetime.fromisoformat("2026-09-22T09:05:00+08:00"),
        )
        with patch(
            "event_worker.load_business_candidate_context",
            wraps=load_business_candidate_context,
        ) as hydrate_context:
            result = poller.run_once()

        self.assertEqual(result["status"], "COMPLETED")
        self.assertIs(hydrate_context.call_args.kwargs["dws"], poller.dws)
        self.assertTrue(any(request["view"] == "messages" for request in adapter.requests))
        self.assertIsNone(observed["processor_kwargs"]["business_candidate_pool_issue"])
        self.assertEqual(observed["processor_kwargs"]["source_order_provenance"], "unverified_input_order")
        candidate = observed["processor_kwargs"]["existing_actions"][0]
        self.assertEqual(candidate["route_module"], "materials")
        self.assertEqual(candidate["route_owner"], "苗笑天")
        self.assertEqual(candidate["source_text"], "请于9月24日前完成回购物资核验工作。")

    def test_incomplete_configured_group_blocks_processing_of_the_whole_poll(self):
        runtime = {
            "config_calendar": [{"dt": "2026-09-22", "day_type": "workday"}],
            "config_group": [
                {"group_name": "目标一", "conversation_id": "conv-1", "purpose": "todo_scan"},
                {"group_name": "目标二", "conversation_id": "conv-2", "purpose": "todo_scan"},
                {"group_name": "不扫描", "conversation_id": "conv-other", "purpose": "other"},
            ],
        }

        class FakeAdapter:
            def query(self, request):
                if request["view"] == "runtime_config":
                    return {"config": runtime}
                if request["view"] == "scan_state":
                    return {"rows": [], "has_more": False}
                if request["view"] == "actions":
                    return {"rows": [], "has_more": False}
                raise AssertionError(request)

        class FakeDws:
            def get_self(self):
                return {"name": "李林骁", "user_id": "SELF"}

        class FakeReader:
            def __init__(self):
                self.calls = []

            def read_group(self, **kwargs):
                self.calls.append(kwargs)
                if kwargs["conversation_id"] == "conv-1":
                    return {"complete": True, "messages": []}
                return {"complete": False, "messages": [{"message_id": "partial"}]}

        class FakeProcessor:
            def __init__(self, *args, **kwargs):
                raise AssertionError("partial history must not reach the processor")

        poller_type = getattr(event_worker, "OneShotHistoryPoller", None)
        self.assertIsNotNone(poller_type, "OneShotHistoryPoller is not available")
        reader = FakeReader()
        poller = poller_type(
            adapter=FakeAdapter(),
            reader=reader,
            dws=FakeDws(),
            processor_factory=FakeProcessor,
            now_factory=lambda: datetime.fromisoformat("2026-09-22T09:05:00+08:00"),
        )
        result = poller.run_once()

        self.assertEqual(result["status"], "REVIEW")
        self.assertEqual(result["groups_attempted"], 2)
        self.assertEqual(result["groups_complete"], 1)
        self.assertEqual([call["conversation_id"] for call in reader.calls], ["conv-1", "conv-2"])
        self.assertTrue(all(call["end"].isoformat() == "2026-09-22T09:05:00+08:00" for call in reader.calls))


class LlmAdapterTests(unittest.TestCase):
    def test_codex_resolver_sends_only_bounded_source_text_and_create_time(self):
        seen = {}

        class FakeAgent:
            def run_conversation(self, user_message, **kwargs):
                seen["user_message"] = user_message
                return {"completed": True, "final_response": '{"semantic_intent":"follow_up"}'}

        resolver = llm_adapter.HermesCodexLlmResolver(
            agent_factory=lambda **kwargs: FakeAgent(), max_source_chars=12
        )
        source_text = "source text longer than bound"
        create_time = "2026-09-27 10:00:00"
        message = {
            "conversation_id": "PRIVATE-CONVERSATION-ID",
            "message_id": "PRIVATE-MESSAGE-ID",
            "group": "PRIVATE-GROUP-NAME",
            "sender": "PRIVATE-SENDER",
            "create_time": create_time,
            "text": source_text,
        }
        plan = {
            "decision": "CREATE",
            "route_module": "PRIVATE-ROUTE-MODULE",
            "route_owner": "PRIVATE-ROUTE-OWNER",
            "owner": "PRIVATE-OWNER",
        }

        result = resolver.resolve(message, plan)

        self.assertEqual(result, {"semantic_intent": "follow_up"})
        self.assertEqual(
            json.loads(seen["user_message"]),
            {"text": source_text[:12], "create_time": create_time},
        )
        self.assertFalse(any(value in seen["user_message"] for value in plan.values()))

    def test_codex_resolver_uses_date_only_contract_for_explicit_clock_deadline(self):
        seen = {}
        deadline_text = "请在2026年9月28日14:00前核对虚构演示库存清单并反馈。"

        class FakeAgent:
            def run_conversation(self, user_message, **kwargs):
                instruction = kwargs["system_message"]
                seen["instruction"] = instruction
                seen["source"] = json.loads(user_message)
                # Model fixture emits a timestamp when the contract leaves the
                # date/time serialization ambiguous, matching the invalid form.
                if "YYYY-MM-DD" in instruction:
                    response = '{"semantic_intent":"follow_up","due_date":"2026-09-28"}'
                else:
                    response = (
                        '{"semantic_intent":"follow_up",'
                        '"due_date":"2026-09-28T14:00:00+08:00"}'
                    )
                return {"completed": True, "final_response": response}

        resolver = llm_adapter.HermesCodexLlmResolver(
            agent_factory=lambda **kwargs: FakeAgent()
        )
        result = resolver.resolve(
            {"text": deadline_text, "create_time": "2026-09-27 10:00:00"}, {}
        )

        self.assertEqual(
            result, {"semantic_intent": "follow_up", "due_date": "2026-09-28"}
        )
        self.assertIn("YYYY-MM-DD", seen["instruction"])
        self.assertIn("只能输出公历日期部分", seen["instruction"])
        self.assertEqual(seen["source"]["text"], deadline_text)

    def test_codex_resolver_suppresses_request_debug_dump_on_api_error_per_agent(self):
        dumped_requests = []
        private_text = "PRIVATE-DINGTALK-MESSAGE-TEXT"

        class FakeAgent:
            def _dump_api_request_debug(self, request, **kwargs):
                dumped_requests.append((request, kwargs))

            def run_conversation(self, user_message, **kwargs):
                self._dump_api_request_debug(
                    {"messages": [{"role": "user", "content": user_message}]},
                    reason="non_retryable_client_error",
                    error=RuntimeError("upstream rejected request"),
                )
                raise RuntimeError("upstream rejected request")

        # The original helper remains intact for other agent instances.
        other_agent = FakeAgent()
        other_agent._dump_api_request_debug({"text": "other-agent"}, reason="test")
        self.assertEqual(dumped_requests[0][0], {"text": "other-agent"})
        dumped_requests.clear()

        resolver = llm_adapter.HermesCodexLlmResolver(
            agent_factory=lambda **kwargs: FakeAgent()
        )
        with self.assertRaisesRegex(LlmError, "codex_runtime_failed:RuntimeError"):
            resolver.resolve({"text": private_text}, {})

        self.assertEqual(dumped_requests, [])

    def test_codex_resolver_reuses_native_hermes_agent_with_explicit_route(self):
        resolver_type = getattr(llm_adapter, "HermesCodexLlmResolver", None)
        self.assertIsNotNone(resolver_type, "native Hermes Codex resolver is not available")

        seen = {"factory_calls": 0, "turns": []}

        class FakeAgent:
            def run_conversation(self, user_message, **kwargs):
                seen["turns"].append((user_message, kwargs))
                return {"completed": True, "final_response": '{"semantic_intent":"follow_up"}'}

        def factory(**kwargs):
            seen["factory_calls"] += 1
            seen["factory_kwargs"] = kwargs
            seen["agent"] = FakeAgent()
            return seen["agent"]

        resolver = resolver_type(agent_factory=factory, max_source_chars=12)
        first = resolver.resolve({"text": "source text longer than bound"}, {"decision": "REVIEW"})
        second = resolver.resolve({"text": "another source"}, {"decision": "REVIEW"})

        self.assertEqual(first, {"semantic_intent": "follow_up"})
        self.assertEqual(second, first)
        self.assertEqual(seen["factory_calls"], 1)
        options = seen["factory_kwargs"]
        self.assertEqual(options["provider"], "openai-codex")
        self.assertEqual(options["requested_provider"], "openai-codex")
        self.assertEqual(options["api_mode"], "codex_responses")
        self.assertEqual(options["model"], "gpt-6-luna")
        self.assertEqual(options["reasoning_config"], {"enabled": True, "effort": "max"})
        self.assertEqual(options["enabled_toolsets"], [])
        self.assertTrue(options["skip_memory"])
        self.assertTrue(options["skip_context_files"])
        self.assertTrue(options["skip_background_review"])
        self.assertEqual(options["max_iterations"], 1)
        self.assertNotIn("api_key", options)
        self.assertNotIn("base_url", options)
        self.assertTrue(seen["agent"]._persist_disabled)
        self.assertEqual(len(seen["turns"]), 2)
        first_message, first_turn_options = seen["turns"][0]
        self.assertLessEqual(len(__import__("json").loads(first_message)["text"]), 12)
        self.assertEqual(first_turn_options["conversation_history"], [])

    def test_codex_resolver_wraps_native_timeout_without_exposing_payload(self):
        resolver_type = llm_adapter.HermesCodexLlmResolver

        class FakeAgent:
            def run_conversation(self, *args, **kwargs):
                raise TimeoutError("private request details")

        resolver = resolver_type(agent_factory=lambda **kwargs: FakeAgent())
        with self.assertRaisesRegex(LlmError, "codex_runtime_failed:TimeoutError") as raised:
            resolver.resolve({"text": "message"}, {})
        self.assertNotIn("private request details", str(raised.exception))

    def test_codex_resolver_rejects_incomplete_or_unauthorized_native_output(self):
        resolver_type = llm_adapter.HermesCodexLlmResolver
        invalid_responses = (
            {"completed": False, "failed": True, "final_response": '{"semantic_intent":"follow_up"}'},
            {"completed": True, "final_response": "not json"},
            {"completed": True, "final_response": '{"title":"task","route_owner":"someone"}'},
        )
        for response in invalid_responses:
            with self.subTest(response=response):
                class FakeAgent:
                    def run_conversation(self, *args, **kwargs):
                        return response

                resolver = resolver_type(agent_factory=lambda **kwargs: FakeAgent())
                with self.assertRaises(LlmError):
                    resolver.resolve({"text": "message"}, {})

    def test_deepseek_resolver_uses_in_process_transport_and_bounded_source(self):
        from llm_adapter import DeepSeekLlmResolver
        seen = {}
        def transport(request, timeout):
            seen.update(request=request, timeout=timeout)
            return {"choices": [{"message": {"content": '{"semantic_intent":"follow_up"}'}}]}
        resolver = DeepSeekLlmResolver(api_key="test-secret", transport=transport, max_source_chars=20)
        self.assertEqual(resolver.resolve({"text": "x" * 100}, {"decision": "REVIEW"}), {"semantic_intent": "follow_up"})
        self.assertEqual(seen["request"]["model"], "deepseek-flash")
        self.assertLessEqual(len(seen["request"]["messages"][1]["content"]), 4000)

    def test_jev_business_candidate_keys_must_be_synthetic_ordinals(self):
        calls = []
        resolver = llm_adapter.JevResolver(
            api_key="fake-key",
            transport=lambda payload, timeout: calls.append(payload) or {},
        )
        with self.assertRaisesRegex(LlmError, "jev_business_candidate_invalid"):
            resolver.resolve(
                {"text": "当前事项", "create_time": "2026-09-22 10:00:00"},
                business_candidates=[{
                    "key": "business_task-123",
                    "text": "既有事项内容",
                    "create_time": "2026-09-20 10:00:00",
                }],
            )
        self.assertEqual(calls, [])

    def test_jev_evaluate_scores_only_program_recalled_bounded_business_candidates(self):
        seen = {}
        candidate = {
            "key": "business_0",
            "text": "请于2026年9月24日前完成集中库房物资复核。",
            "create_time": "2026-09-20 09:00:00",
        }

        def transport(payload, timeout):
            seen["payload"] = payload
            return {
                "answers": {
                    "action_signal": {"type": "noul", "noul": 0.90},
                    "item_status": {
                        "type": "choice", "choice": "new",
                        "probabilities": {"new": 0.90, "change": 0.02, "complete": 0.02, "notice": 0.02, "uncertain": 0.04},
                        "confidence": 0.90,
                    },
                    "due_date": {
                        "type": "choice", "choice": "none",
                        "probabilities": {"none": 0.90, "uncertain": 0.10},
                        "confidence": 0.90,
                    },
                    "business_match_0": {
                        "type": "score", "score": 2.0, "confidence": 0.92,
                        "legend": {"0": "different", "1": "related", "2": "same business task"},
                        "probabilities": {"0": 0.02, "1": 0.03, "2": 0.95},
                    },
                },
            }

        resolver = llm_adapter.JevResolver(api_key="fake-key", transport=transport)
        result = resolver.resolve(
            {"text": "请于9月24日前完成集中库房物资复核。", "create_time": "2026-09-22 10:00:00"},
            date_candidates=[],
            business_candidates=[candidate],
        )
        payload = seen["payload"]
        state = json.loads(payload["state"])
        self.assertEqual(state["business_candidates"], [candidate])
        self.assertEqual(payload["questions"]["business_match_0"]["type"], "score")
        self.assertEqual(result["business_matches"], [{"candidate_key": "business_0", "score": 2.0, "confidence": 0.92}])
        self.assertNotIn("task_id", json.dumps(payload))
        self.assertNotIn("owner", json.dumps(payload))

    def test_jev_rejects_choice_probability_distribution_that_is_not_normalized(self):
        resolver = llm_adapter.JevResolver(
            api_key="fake-key",
            transport=lambda payload, timeout: {"answers": {
                "action_signal": {"type": "noul", "noul": 0.90},
                "item_status": {
                    "type": "choice", "choice": "new",
                    "probabilities": {"new": 0.90, "change": 0.20, "complete": 0.10, "notice": 0.05, "uncertain": 0.05},
                    "confidence": 0.90,
                },
                "due_date": {
                    "type": "choice", "choice": "none",
                    "probabilities": {"none": 0.90, "uncertain": 0.10},
                    "confidence": 0.90,
                },
            }},
        )

        with self.assertRaisesRegex(LlmError, "jev_invalid_response:item_status_probability_sum"):
            resolver.resolve({"text": "work", "create_time": "2026-09-22 10:00:00"})

    def test_jev_evaluate_combines_bounded_action_status_and_date_candidates(self):
        seen = {}
        candidate = {
            "key": "date_0",
            "due_date": "2026-09-29",
            "due_iso": "2026-09-29T17:00:00+08:00",
        }

        def transport(payload, timeout):
            seen["payload"] = payload
            seen["timeout"] = timeout
            return {
                "answers": {
                    "action_signal": {"type": "noul", "noul": 0.88},
                    "item_status": {
                        "type": "choice", "choice": "new",
                        "probabilities": {"new": 0.91, "change": 0.02, "complete": 0.02, "notice": 0.02, "uncertain": 0.03},
                        "confidence": 0.91,
                    },
                    "due_date": {
                        "type": "choice", "choice": "date_0",
                        "probabilities": {"date_0": 0.90, "none": 0.05, "uncertain": 0.05},
                        "confidence": 0.90,
                    },
                },
                "usage": {"total_tokens": 17},
            }

        resolver_type = getattr(llm_adapter, "JevResolver", None)
        self.assertIsNotNone(resolver_type, "JevResolver is not available")
        resolver = resolver_type(api_key="fake-key", max_source_chars=32, transport=transport)
        message = {
            "text": "请在2026年9月29日前完成资料核对。",
            "create_time": "2026-09-27 10:00:00",
            "conversation_id": "PRIVATE-CONVERSATION",
            "message_id": "PRIVATE-MESSAGE",
            "group": "PRIVATE-GROUP",
            "sender": "PRIVATE-SENDER",
        }
        result = resolver.resolve(
            message, {"route_owner": "PRIVATE-OWNER"}, date_candidates=[candidate]
        )

        payload = seen["payload"]
        state = json.loads(payload["state"])
        self.assertEqual(set(payload), {"state", "model", "questions"})
        self.assertEqual(set(state), {"text", "create_time", "date_candidates"})
        self.assertEqual(state["text"], message["text"])
        self.assertEqual(state["create_time"], message["create_time"])
        self.assertEqual(state["date_candidates"], [candidate])
        self.assertEqual(set(payload["questions"]), {"action_signal", "item_status", "due_date"})
        self.assertEqual(
            {spec["type"] for spec in payload["questions"].values()},
            {"noul", "choice"},
        )
        self.assertEqual(payload["questions"]["item_status"]["criteria"].keys(), {
            "new", "change", "complete", "notice", "uncertain"
        })
        self.assertEqual(result["action_probability"], 0.88)
        self.assertEqual(result["action_signal"], "yes")
        self.assertEqual(result["item_status"], "new")
        self.assertEqual(result["due_candidate_key"], "date_0")
        self.assertEqual(result["due_date"], candidate["due_date"])
        self.assertEqual(result["due_iso"], candidate["due_iso"])
        self.assertFalse(result["decision_boundary"]["calibrated"])
        self.assertIn("not_accuracy_or_write_authorization", result["decision_boundary"]["use"])
        self.assertNotIn("PRIVATE", json.dumps(payload))

    def test_llm_result_allows_only_fact_completion_fields(self):
        result = validate_llm_result({"title": "反馈资料", "due_date": "2026-09-23"})
        self.assertEqual(result["title"], "反馈资料")
        self.assertEqual(result["due_date"], "2026-09-23")

    def test_llm_result_allows_semantic_follow_up_hint(self):
        result = validate_llm_result(
            {"semantic_intent": "follow_up", "title": "回购物资领用安排跟进"}
        )
        self.assertEqual(result["semantic_intent"], "follow_up")
        self.assertEqual(result["title"], "回购物资领用安排跟进")

    def test_llm_result_rejects_unknown_semantic_intent(self):
        with self.assertRaises(ValueError):
            validate_llm_result({"semantic_intent": "create"})

    def test_llm_result_rejects_route_or_executor_override(self):
        with self.assertRaises(ValueError):
            validate_llm_result({"title": "反馈资料", "route_owner": "张志斌"})


class AttachmentAdapterTests(unittest.TestCase):
    def test_download_resource_args_keep_message_and_conversation_identity(self):
        args = build_download_resource_args(
            resource_id="media-1",
            message_id="msg-1",
            conversation_id="conv-1",
            output=".",
        )
        joined = " ".join(args)
        self.assertIn("messages-resource-download", joined)
        self.assertIn("media-1", joined)
        self.assertIn("msg-1", joined)
        self.assertIn("conv-1", joined)

    def test_add_attachment_args_require_existing_task_and_file(self):
        args = build_add_attachment_args("task-1", "/scratch/image.png")
        self.assertEqual(
            args,
            ["todo", "task", "add-attachment", "--task-id", "task-1", "--file", "/scratch/image.png"],
        )

    def test_attachment_readback_matches_file_name(self):
        payload = {"attachments": [{"attachmentId": "a1", "fileName": "image.png", "fileSize": "10"}]}
        self.assertTrue(attachment_is_present(payload, "image.png"))
        self.assertFalse(attachment_is_present(payload, "other.png"))
    def test_command_llm_resolver_accepts_semantic_json_contract(self):
        resolver = CommandLlmResolver(
            [sys.executable, "-c", "import json; print(json.dumps({'semantic_intent': 'follow_up'}))"]
        )
        result = resolver.resolve(
            {"conversation_id": "c", "message_id": "m", "text": "回购物资"},
            {"decision": "IGNORE", "warnings": ["semantic_intent_needs_llm"]},
        )
        self.assertEqual(result, {"semantic_intent": "follow_up"})

    def test_command_llm_resolver_uses_json_contract(self):
        resolver = CommandLlmResolver(
            [sys.executable, "-c", "import json; print(json.dumps({'title': '核对危废台账'}))"]
        )
        result = resolver.resolve(
            {"conversation_id": "c", "message_id": "m", "text": "请处理"},
            {"warnings": ["title_needs_llm"]},
        )
        self.assertEqual(result, {"title": "核对危废台账"})


if __name__ == "__main__":
    unittest.main()


def test_jev_v2_clause_split_preserves_letter_n_and_full_task_constraint():
    import jev_v2
    captured = {}
    def transport(payload, timeout):
        captured.update(payload)
        questions = payload["questions"]
        answers = {}
        for key, question in questions.items():
            if question["type"] == "choice":
                options = question["criteria"]
                choice = "clause_0" if key == "source_clause" else "none"
                answers[key] = {"type": "choice", "choice": choice, "probabilities": {k: (0.9 if k == choice else 0.1/(len(options)-1)) for k in options}, "confidence": 0.9}
            else:
                answers[key] = {"type": "noul", "noul": 0.9}
        return {"answers": answers}
    resolver = jev_v2.JevV2Resolver(transport=transport)
    source = "Plan inventory\nComplete inventory"
    result = resolver.resolve({"text": source}, {"decision": "CREATE"})
    clauses = captured["questions"]["source_clause"]["criteria"]
    assert clauses["clause_0"] == "Plan inventory"
    assert clauses["clause_1"] == "Complete inventory"
    assert json.loads(captured["state"])["text"] == source
    assert result["source_clause"] == "Plan inventory"


def test_jev_v2_candidate_redaction_counts_are_included_in_total():
    import jev_v2
    def transport(payload, timeout):
        answers = {}
        for key, question in payload["questions"].items():
            if question["type"] == "choice":
                options = question["criteria"]
                choice = "clause_0" if key == "source_clause" else "none"
                answers[key] = {"type": "choice", "choice": choice, "probabilities": {k: (0.9 if k == choice else 0.1/(len(options)-1)) for k in options}, "confidence": 0.9}
            else:
                answers[key] = {"type": "noul", "noul": 0.9}
        return {"answers": answers}
    resolver = jev_v2.JevV2Resolver(transport=transport)
    resolver.resolve({"text": "Check inventory"}, {"decision": "CREATE"}, business_candidates=[{"text": "Call 13800138000"}])
    assert resolver.last_redaction_counts["business_candidates"]["phone_numbers"] == 1
    assert resolver.last_redaction_counts["total"]["phone_numbers"] == resolver.last_redaction_counts["message"]["phone_numbers"] + 1
