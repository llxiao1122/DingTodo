import json
import sys
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dws_adapter import DwsClient, DwsError  # noqa: E402
from event_worker import EventProcessor  # noqa: E402
from routing import build_source_comment, plan_message  # noqa: E402


SOURCE_TEXT = "请于10月20日前完成危险废物清单核对整改。"
OLD_SOURCE = {
    "conversation_id": "conv-1",
    "message_id": "older-source",
    "group": "铁炉西工班群",
    "sender": "张云云",
    "create_time": "2026-10-15 09:00:00",
    "text": SOURCE_TEXT,
}


class FakeWriter:
    def __init__(self):
        self.requests = []

    def execute(self, request):
        self.requests.append(dict(request))
        if request["operation"] == "claim_action":
            return {"created": True, "action_id": 73}
        return {"ok": True, "committed": True, "action_id": request.get("action_id"), "row": {"status": request.get("status"), "external_task_id": request.get("external_task_id")}}


class FakeDws:
    def __init__(self, *, search_rows=None, source_text=SOURCE_TEXT, search_error=None):
        self.search_rows = search_rows if search_rows is not None else [{"taskId": "existing-task"}]
        self.source_text = source_text
        self.search_error = search_error
        self.search_queries = []
        self.get_calls = []
        self.create_calls = 0
        self.created_plan = None
        self.linked_comments = {}
        self.task_details = {}

    def resolve_exact(self, names):
        return {name: "LIVE-" + name for name in names}

    def search_todos(self, query):
        self.search_queries.append(query)
        if self.search_error:
            raise self.search_error
        for row in self.search_rows:
            task_id = str(row.get("taskId") or row.get("todoTaskId") or "")
            if task_id and task_id not in self.task_details:
                self.task_details[task_id] = self._detail(
                    task_id, query, "2026-10-20T18:00:00+08:00",
                    ["苗笑天", "李林骁"], ["LIVE-苗笑天", "SELF"],
                )
        return list(self.search_rows)

    def _detail(self, task_id, title, due, names, ids, *, done=False):
        return {
            "taskId": task_id,
            "subject": title,
            "isDone": done,
            "dueTime": due,
            "priority": 20,
            "executorInfos": [
                {"name": name, "userId": user_id} for name, user_id in zip(names, ids)
            ],
        }

    def get_todo(self, task_id):
        self.get_calls.append(task_id)
        if task_id in self.task_details:
            return dict(self.task_details[task_id])
        plan = self.created_plan
        return self._detail(
            task_id,
            plan["title"],
            plan.get("due_iso"),
            plan["executor_names"],
            plan["executor_ids"],
        )

    def list_comments(self, task_id):
        if task_id in self.linked_comments:
            return {"data": {"comments": list(self.linked_comments[task_id])}}
        source = dict(OLD_SOURCE, text=self.source_text)
        return {"data": {"comments": [{"content": build_source_comment(source)}]}}

    def list_task_comments(self, task_id):
        return self.list_comments(task_id)["data"]["comments"]

    def create_todo(self, plan, priority=20):
        self.create_calls += 1
        self.created_plan = dict(plan)
        return {"task_id": "new-task"}

    def add_source_comment(self, task_id, content):
        self.linked_comments[task_id] = [{"content": content}]

    def verify_source_comment(self, task_id, content):
        assert any(row.get("content") == content for row in self.linked_comments[task_id])


class DwsSearchAdapterTests(unittest.TestCase):
    def test_search_uses_cross_page_shortcut_and_accepts_complete_v1064_mock_envelope(self):
        response = {
            "ok": True,
            "outcome": "success",
            "data": {
                "complete": True,
                "count": 1,
                "todos": [{"taskId": "synthetic-1"}],
            },
        }
        with patch("dws_adapter.subprocess.run") as run:
            run.return_value.returncode = 0
            run.return_value.stdout = json.dumps(response)
            run.return_value.stderr = ""
            rows = DwsClient(executable="dws-test").search_todos("合成标题")
        self.assertEqual(rows, [{"taskId": "synthetic-1"}])
        self.assertEqual(run.call_args.args[0][1:5], ["todo", "+search", "--query", "合成标题"])

    def test_search_rejects_partial_and_unknown_shapes(self):
        for response in (
            {
                "ok": True,
                "outcome": "success",
                "data": {
                    "complete": True,
                    "count": 0,
                    "todos": [],
                    "hasMore": True,
                },
            },
            {"ok": True, "outcome": "success", "data": {"unexpected": []}},
            {
                "ok": True,
                "outcome": "success",
                "data": {
                    "complete": True,
                    "count": 1,
                    "todos": [{"subject": "no stable id"}],
                },
            },
        ):
            with self.subTest(response=response), patch("dws_adapter.subprocess.run") as run:
                run.return_value.returncode = 0
                run.return_value.stdout = json.dumps(response)
                run.return_value.stderr = ""
                with self.assertRaises(DwsError):
                    DwsClient(executable="dws-test").search_todos("合成标题")

    def test_comment_readback_rejects_partial_pages(self):
        client = DwsClient(executable="dws-test")
        client.list_comments = lambda task_id: {"data": {"comments": [], "hasMore": True}}
        with self.assertRaises(DwsError):
            client.list_task_comments("synthetic-task")


class BusinessSearchPipelineTests(unittest.TestCase):
    def setUp(self):
        class FixedDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 10, 18, 12, 0, tzinfo=tz)

        clock = patch("event_worker.datetime", FixedDatetime)
        clock.start()
        self.addCleanup(clock.stop)

    def _processor(self, dws, writer, existing_actions=None):
        runtime = {
            "config_group": [{"group_name": "铁炉西工班群", "conversation_id": "conv-1", "purpose": "todo_scan"}],
            "config_staff": [[1, "李林骁", "STALE-SELF", "leader"], [2, "苗笑天", "STALE-MIAO", "executor"]],
            "active_routing_loaded": True,
            "responsibility_rules": {},
        }
        return EventProcessor(
            runtime,
            {"name": "李林骁", "user_id": "SELF"},
            mode="production",
            write_ledger=True,
            allow_external_writes=True,
            dws=dws,
            writer=writer,
            existing_actions=existing_actions,
        )

    @staticmethod
    def _event(message_id="new-source", text=SOURCE_TEXT):
        return {
            "conversation_id": "conv-1",
            "message_id": message_id,
            "group": "铁炉西工班群",
            "sender": "王萌",
            "sender_id": "OTHER",
            "create_time": "2026-10-18 10:00:00",
            "text": text,
        }

    def test_existing_business_task_is_read_back_linked_and_not_recreated(self):
        dws, writer = FakeDws(), FakeWriter()
        processor = self._processor(dws, writer)
        plans = processor.process_batch([self._event()])
        plan = plans[0]
        self.assertEqual(plan["decision"], "IGNORE")
        self.assertEqual(plan["reason"], "business_todo_already_exists")
        self.assertEqual(plan["external_task_id"], "existing-task")
        self.assertEqual(dws.create_calls, 0)
        self.assertEqual(dws.get_calls, ["existing-task"])
        self.assertEqual(len(dws.search_queries), 1)
        claim = next(row for row in writer.requests if row["operation"] == "claim_action")
        self.assertEqual(claim["business_key"], plans[0]["business_key"])
        self.assertTrue(claim["business_key"])
        link = next(row for row in writer.requests if row["operation"] == "link_external_task")
        self.assertEqual(link["external_task_id"], "existing-task")

    def test_new_review_records_its_decision_reason(self):
        dws, writer = FakeDws(search_rows=[]), FakeWriter()
        processor = self._processor(dws, writer)
        processor._write_ledger(self._event(), {
            "decision": "REVIEW", "reason": "title_requires_context",
        })
        saved = [request for request in writer.requests
                 if request["operation"] == "save_action_result"]
        self.assertEqual(len(saved), 1)
        self.assertEqual((saved[0]["status"], saved[0]["last_error"]),
                         ("REVIEW", "title_requires_context"))

    def test_business_key_collision_without_source_action_fails_closed_and_keeps_unique_key(self):
        class CollisionWriter:
            def __init__(self):
                self.requests = []
            def execute(self, request):
                self.requests.append(dict(request))
                if request["operation"] == "claim_action":
                    return {"created": False, "reason": "business_exists"}
                return {"ok": True, "committed": True, "action_id": request.get("action_id"), "row": {"status": request.get("status"), "external_task_id": request.get("external_task_id")}}

        dws, writer = FakeDws(search_rows=[]), CollisionWriter()
        plan = self._processor(dws, writer).process_batch([self._event()])[0]

        self.assertEqual(plan["decision"], "REVIEW")
        self.assertEqual(plan["reason"], "business_claim_source_action_missing")
        self.assertIn("business_claim_source_action_missing", plan["warnings"])
        claim = next(row for row in writer.requests if row["operation"] == "claim_action")
        self.assertTrue(claim["business_key"])
        self.assertEqual(dws.create_calls, 0)

    def test_writer_duplicate_pointer_without_verified_remote_task_stays_review(self):
        class DuplicateWriter:
            def __init__(self):
                self.requests = []
            def execute(self, request):
                self.requests.append(dict(request))
                if request["operation"] == "claim_action":
                    return {"created": True, "action_id": 72, "duplicate_of_action_id": 11}
                return {"ok": True, "committed": True, "action_id": request.get("action_id"), "row": {"status": request.get("status"), "external_task_id": request.get("external_task_id")}}

        dws, writer = FakeDws(search_rows=[]), DuplicateWriter()
        plan = self._processor(dws, writer).process_batch([self._event()])[0]

        assert plan["decision"] == "REVIEW"
        assert plan["reason"] == "business_claim_duplicate_unverified"
        assert dws.create_calls == 0
        claim = next(row for row in writer.requests if row["operation"] == "claim_action")
        assert claim["business_key"] == plan["business_key"]

    def test_search_failure_fails_closed_before_external_create(self):
        dws, writer = FakeDws(search_error=DwsError("partial synthetic search")), FakeWriter()
        plans = self._processor(dws, writer).process_batch([self._event()])
        self.assertEqual(plans[0]["decision"], "REVIEW")
        self.assertEqual(plans[0]["reason"], "business_search_incomplete")
        self.assertEqual(dws.create_calls, 0)

    def test_source_linked_task_id_is_read_back_and_never_recreated(self):
        source = self._event(message_id="already-linked")
        action = {
            "conversation_id": "conv-1", "message_id": "already-linked", "action_key": "todo",
            "status": "EXTERNAL_CREATED", "external_task_id": "linked-existing",
        }
        dws, writer = FakeDws(search_rows=[]), FakeWriter()
        dws.task_details["linked-existing"] = {"taskId": "linked-existing"}
        plan = self._processor(dws, writer, existing_actions=[action]).process_batch([source])[0]
        self.assertEqual(plan["decision"], "IGNORE")
        self.assertTrue(plan["linked_task_readback"]["task_id_verified"])
        self.assertEqual(plan["external_task_id"], "linked-existing")
        self.assertEqual(dws.get_calls, ["linked-existing"])
        self.assertEqual(dws.search_queries, [])
        self.assertEqual(dws.create_calls, 0)

    def test_per_run_search_cache_is_invalidated_after_create(self):
        class SearchReflectsCreate(FakeDws):
            def search_todos(self, query):
                self.search_rows = [{"taskId": "new-task"}] if self.create_calls else []
                return super().search_todos(query)

        dws, writer = SearchReflectsCreate(), FakeWriter()
        processor = self._processor(dws, writer)
        plans = processor.process_batch([
            self._event(message_id="first-source"),
            self._event(message_id="second-source"),
        ])
        self.assertEqual([plan["decision"] for plan in plans], ["CREATE", "IGNORE"])
        self.assertEqual(plans[1]["reason"], "business_todo_already_exists")
        self.assertEqual(dws.create_calls, 1)
        self.assertEqual(len(dws.search_queries), 2)

    def test_exact_title_with_different_source_scope_does_not_suppress_new_task(self):
        dws, writer = FakeDws(source_text="请于10月20日前完成危险废物B-202清单核对整改。"), FakeWriter()
        plans = self._processor(dws, writer).process_batch([self._event(text="请于10月20日前完成危险废物A-101清单核对整改。")])
        self.assertEqual(plans[0]["decision"], "CREATE")
        self.assertEqual(dws.create_calls, 1)
        self.assertEqual(dws.get_calls, ["existing-task", "new-task"])

    def test_completed_or_expired_candidate_does_not_cover_new_round(self):
        dws, writer = FakeDws(), FakeWriter()
        dws.task_details["existing-task"] = dws._detail(
            "existing-task", "请于10月20日前完成危险废物清单核对整改", "2026-10-20T18:00:00+08:00",
            ["苗笑天", "李林骁"], ["LIVE-苗笑天", "SELF"], done=True,
        )
        plan = self._processor(dws, writer).process_batch([self._event()])[0]
        self.assertEqual(plan["decision"], "CREATE")
        self.assertEqual(dws.create_calls, 1)

    def test_local_business_key_collision_does_not_replace_source_key_idempotency_or_live_search(self):
        message = {
            "conversation_id": "conv-1", "message_id": "new-source",
            "group": "铁炉西工班群", "create_time": "2026-10-18 10:00:00",
            "text": SOURCE_TEXT,
        }
        config = {"staff": {}, "active_routing_loaded": True, "responsibility_rules": {}}
        self_user = {"name": "李林骁", "user_id": "SELF"}
        first = plan_message(message, config, self_user, run_date=date(2026, 10, 18), mode="test")
        repeated = plan_message(
            message, config, self_user, run_date=date(2026, 10, 18), mode="test",
            existing_actions=[{
                "business_key": first["business_key"], "status": "EXTERNAL_CREATED",
                "external_task_id": "older-task",
            }],
        )
        self.assertEqual(repeated["decision"], "CREATE")


if __name__ == "__main__":
    unittest.main()
