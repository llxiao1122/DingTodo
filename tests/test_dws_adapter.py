import json
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dws_adapter import (  # noqa: E402
    CifraCardCredentialSource,
    DwsClient,
    DwsError,
    build_complete_args,
    build_comment_args,
    build_create_args,
    build_create_sub_args,
    build_reopen_args,
    build_update_args,
    comment_is_present,
    verify_todo,
)
from review_card_adapter import (
    CARD_DECISION_OPTIONS,
    CARD_FORM_FIELDS_TEMPLATE_KEY,
    CARD_TEMPLATE_STATIC_FIELDS,
)


def card_template_fields(label="synthetic review"):
    return {
        **CARD_TEMPLATE_STATIC_FIELDS,
        CARD_FORM_FIELDS_TEMPLATE_KEY: json.dumps([{
            "type": "SELECT",
            "required": True,
            "name": "dtd_review_1",
            "label": label,
            "placeholder": "请选择创建待办或忽略",
            "options": CARD_DECISION_OPTIONS,
        }], ensure_ascii=False, separators=(",", ":")),
    }


class DwsAdapterTests(unittest.TestCase):
    def test_search_todos_accepts_official_todos_shape_and_tasks_compatibility(self):
        row = {
            "taskId": "synthetic-task-1",
            "subject": "synthetic",
            "finalStatusStage": 0,
            "priority": 20,
            "dueTime": None,
        }
        for list_key in ("todos", "tasks"):
            with self.subTest(list_key=list_key):
                payload = {
                    "ok": True,
                    "outcome": "success",
                    "data": {"complete": True, "count": 1, list_key: [row]},
                }
                completed = subprocess.CompletedProcess(
                    args=[], returncode=0, stdout=json.dumps(payload), stderr=""
                )
                with patch("dws_adapter.subprocess.run", return_value=completed) as run:
                    result = DwsClient(executable="dws-test").search_todos("synthetic")
                self.assertEqual(result, [row])
                self.assertEqual(
                    run.call_args.args[0],
                    ["dws-test", "todo", "+search", "--query", "synthetic", "--format", "json"],
                )
        empty_payload = {
            "ok": True,
            "outcome": "success",
            "data": {"complete": True, "count": 0, "todos": []},
        }
        completed = subprocess.CompletedProcess(
            args=[], returncode=0, stdout=json.dumps(empty_payload), stderr=""
        )
        with patch("dws_adapter.subprocess.run", return_value=completed):
            self.assertEqual(DwsClient().search_todos("no matches"), [])

    def test_search_todos_checks_complete_count_shape_and_duplicate_semantics(self):
        row = {"taskId": "synthetic-task-1", "subject": "synthetic"}
        duplicate_payload = {
            "ok": True,
            "outcome": "success",
            "data": {"complete": True, "count": 2, "todos": [row, row]},
        }
        completed = subprocess.CompletedProcess(
            args=[], returncode=0, stdout=json.dumps(duplicate_payload), stderr=""
        )
        with patch("dws_adapter.subprocess.run", return_value=completed):
            self.assertEqual(DwsClient().search_todos("synthetic"), [row])

        invalid_payloads = [
            {"ok": True, "outcome": "success", "data": {"count": 1, "todos": [row]}},
            {"ok": True, "outcome": "success", "data": {"complete": False, "count": 1, "todos": [row]}},
            {"ok": True, "outcome": "success", "data": {"complete": True, "partial": True, "count": 1, "todos": [row]}},
            {"ok": True, "outcome": "success", "data": {"complete": True, "hasMore": True, "count": 1, "todos": [row]}},
            {"ok": True, "outcome": "success", "data": {"complete": True, "truncated": True, "count": 1, "todos": [row]}},
            {"ok": True, "outcome": "success", "data": {"complete": True, "nextCursor": "next", "count": 1, "todos": [row]}},
            {"ok": True, "outcome": "success", "data": {"complete": True, "count": 2, "todos": [row]}},
            {"ok": True, "outcome": "success", "data": {"complete": True, "count": 0, "todos": [row]}},
            {"ok": True, "outcome": "success", "data": {"complete": True, "count": True, "todos": [row]}},
            {"ok": True, "outcome": "success", "data": {"complete": True, "count": 1, "todos": [row, row]}},
            {"ok": True, "outcome": "success", "data": {"complete": True, "count": 2, "todos": [row, {**row, "subject": "conflict"}]}},
            {"ok": True, "outcome": "success", "data": {"complete": True, "count": 1, "todos": [row], "tasks": []}},
            {"ok": True, "outcome": "success", "data": {"complete": True, "count": 1, "todos": [row]}, "truncated": True},
            {"ok": True, "outcome": "success", "data": {"complete": True, "nextcursor": "next", "count": 1, "todos": [row]}},
        ]
        for payload in invalid_payloads:
            with self.subTest(payload=payload):
                completed = subprocess.CompletedProcess(
                    args=[], returncode=0, stdout=json.dumps(payload), stderr=""
                )
                with patch("dws_adapter.subprocess.run", return_value=completed):
                    with self.assertRaises(DwsError):
                        DwsClient().search_todos("synthetic")

    def test_create_args_keep_self_in_test_executor_list(self):
        args = build_create_args(
            title="完成台账核对",
            executor_ids=["SELF-LIVE-ID"],
            due_iso="2026-09-22T18:00:00+08:00",
            priority=20,
        )
        self.assertEqual(args[:3], ["todo", "task", "create"])
        self.assertIn("--executors", args)
        self.assertIn("SELF-LIVE-ID", args)
        self.assertNotIn("--recurrence", args)
        self.assertNotIn("chat", args)

    def test_create_args_production_contains_owner_and_self(self):
        args = build_create_args(
            title="完成物资核对",
            executor_ids=["OWNER-ID", "SELF-LIVE-ID"],
            due_iso="2026-09-22T18:00:00+08:00",
            priority=20,
        )
        joined = " ".join(args)
        self.assertIn("OWNER-ID,SELF-LIVE-ID", joined)

    def test_create_sub_uses_documented_parent_and_stable_id_arguments(self):
        args = build_create_sub_args(
            parent_task_id="synthetic-parent", title="盘点权限核对",
            executor_ids=["LIVE-OWNER", "LIVE-SELF"],
            due_iso="2026-10-20T18:00:00+08:00", priority=20,
        )
        self.assertEqual(args[:4], ["todo", "task", "create-sub", "--parent-id"])
        self.assertIn("synthetic-parent", args)
        self.assertIn("LIVE-OWNER,LIVE-SELF", args)
        self.assertEqual(args[args.index("--due") + 1], "2026-10-20T18:00:00+08:00")
        with self.assertRaises(ValueError):
            build_create_sub_args(
                parent_task_id="", title="盘点权限核对", executor_ids=["LIVE-OWNER"],
                due_iso="2026-10-20T18:00:00+08:00",
            )

    def test_comment_args_preserve_original_source(self):
        args = build_comment_args("123456", "来源群：铁炉西工班群\n原文：请完成盘点")
        self.assertEqual(
            args,
            [
                "todo",
                "+comment",
                "--task-id",
                "123456",
                "--content",
                "来源群：铁炉西工班群\n原文：请完成盘点",
            ],
        )

    def test_comment_uses_official_command_and_validates_its_json_envelope(self):
        content = "来源群：铁炉西工班群\n原文：请完成盘点"
        response = {
            "ok": True,
            "outcome": "success",
            "data": {
                "taskId": "123456",
                "commentId": "comment-7",
                "content": content,
                "verified": True,
            }
        }
        completed = subprocess.CompletedProcess(
            args=[], returncode=0, stdout=json.dumps(response, ensure_ascii=False), stderr=""
        )
        with patch("dws_adapter.subprocess.run", return_value=completed) as run:
            payload = DwsClient(
                executable="dws-test", confirmed_official_writes=True,
            ).add_source_comment("123456", content)

        self.assertEqual(payload, response)
        self.assertEqual(
            run.call_args.args[0],
            [
                "dws-test", "todo", "+comment", "--task-id", "123456",
                "--content", content, "--yes", "--format", "json",
            ],
        )
        self.assertEqual(run.call_args.kwargs["timeout"], 60)

    def test_comment_rejects_partial_error_or_mismatched_official_envelopes(self):
        content = "原文：请完成盘点"
        result = {
            "taskId": "123456",
            "commentId": "comment-7",
            "content": content,
            "verified": True,
        }
        invalid_payloads = [
            {"data": result},
            {"ok": True, "outcome": "success", "data": {**result, "verified": False}},
            {"ok": True, "outcome": "partial", "data": result},
            {"ok": True, "outcome": "success", "data": result, "hasMore": True},
            {"ok": True, "outcome": "success", "data": result, "complete": False},
            {"ok": True, "outcome": "success", "data": result, "stopReason": "pagination_limit_reached"},
            {"ok": True, "outcome": "success", "data": result, "failures": [{"code": "readback_failed"}]},
            {"ok": True, "outcome": "success", "data": result, "error": {"code": "write_failed"}},
            {"ok": False, "outcome": "failure", "data": result},
            {"ok": True, "outcome": "success", "data": result, "result": {"data": result}},
            {"ok": True, "outcome": "success", "data": {"data": result}},
            {"ok": True, "outcome": "success", "data": {**result, "taskId": "another-task"}},
            {"ok": True, "outcome": "success", "data": {**result, "content": "different"}},
            {"ok": True, "outcome": "success", "data": {**result, "commentId": ""}},
        ]
        for payload in invalid_payloads:
            with self.subTest(payload=payload):
                completed = subprocess.CompletedProcess(
                    args=[], returncode=0, stdout=json.dumps(payload), stderr=""
                )
                with patch("dws_adapter.subprocess.run", return_value=completed):
                    with self.assertRaises(DwsError):
                        DwsClient(confirmed_official_writes=True).add_source_comment("123456", content)

    def test_shortcut_write_without_confirmed_authorization_never_calls_subprocess(self):
        create_plan = {
            "decision": "CREATE", "title": "synthetic", "executor_ids": ["owner"],
        }
        calls = (
            ("create", lambda client: client.create_todo(create_plan)),
            ("create_sub", lambda client: client.create_sub_todo(
                "parent", "synthetic child", ["owner"], "2026-10-20T18:00:00+08:00",
            )),
            ("attachment", lambda client: client.add_attachment("task-42", "/tmp/synthetic.pdf")),
            ("comment", lambda client: client.add_source_comment("task-42", "comment")),
            ("update", lambda client: client.update_linked_todo("task-42", title="new")),
            ("complete", lambda client: client.complete_linked_todo("task-42")),
            ("reopen", lambda client: client.reopen_linked_todo("task-42")),
        )
        for operation, call in calls:
            with self.subTest(operation=operation):
                completed = subprocess.CompletedProcess(args=[], returncode=0, stdout="{}", stderr="")
                with patch("dws_adapter.subprocess.run", return_value=completed) as run:
                    with self.assertRaises(DwsError):
                        call(DwsClient(executable="dws-test"))
                self.assertEqual(run.call_count, 0)

    def test_atomic_task_create_requires_adapter_authorization_and_preserves_cli_args(self):
        completed = subprocess.CompletedProcess(
            args=[], returncode=0, stdout=json.dumps({"data": {"taskId": "created-1"}}), stderr="",
        )
        plan = {
            "decision": "CREATE", "title": "创建原子任务", "executor_ids": ["owner", "self"],
        }
        with patch("dws_adapter.subprocess.run") as run:
            with self.assertRaises(DwsError):
                DwsClient(executable="dws-test").create_todo(plan)
        self.assertEqual(run.call_count, 0)

        with patch("dws_adapter.subprocess.run", return_value=completed) as run:
            result = DwsClient(
                executable="dws-test", confirmed_official_writes=True,
            ).create_todo(plan)
        self.assertEqual(result["task_id"], "created-1")
        command = run.call_args.args[0]
        self.assertEqual(command[1:4], ["todo", "task", "create"])
        self.assertNotIn("--yes", command)
        self.assertEqual(command[-2:], ["--format", "json"])

    def test_create_sub_and_attachment_require_authorized_write(self):
        create_response = subprocess.CompletedProcess(
            args=[], returncode=0, stdout=json.dumps({"data": {"taskId": "child-1"}}), stderr="",
        )
        with patch("dws_adapter.subprocess.run", return_value=create_response) as run:
            result = DwsClient(
                executable="dws-test", confirmed_official_writes=True,
            ).create_sub_todo(
                "parent-1", "synthetic child", ["owner"], "2026-10-20T18:00:00+08:00",
            )
        self.assertEqual(result["task_id"], "child-1")
        self.assertEqual(run.call_args.args[0][1:4], ["todo", "task", "create-sub"])
        self.assertNotIn("--yes", run.call_args.args[0])

        attachment_response = subprocess.CompletedProcess(
            args=[], returncode=0, stdout=json.dumps({"data": {"attachmentIds": ["attachment-1"]}}), stderr="",
        )
        with patch("dws_adapter.subprocess.run", return_value=attachment_response) as run:
            result = DwsClient(
                executable="dws-test", confirmed_official_writes=True,
            ).add_attachment("task-42", "/tmp/synthetic.pdf")
        self.assertEqual(result["attachment_ids"], ["attachment-1"])
        self.assertEqual(run.call_args.args[0][1:4], ["todo", "task", "add-attachment"])
        self.assertNotIn("--yes", run.call_args.args[0])

    def test_generic_run_rejects_direct_and_unclassified_mutations(self):
        calls = (
            ("todo_create", lambda client: client.run([
                "todo", "task", "create", "--title", "synthetic", "--executors", "owner",
            ])),
            ("todo_comment", lambda client: client.run([
                "todo", "+comment", "--task-id", "task-42", "--content", "synthetic",
            ])),
            ("todo_create_sub", lambda client: client.run([
                "todo", "task", "create-sub", "--parent-id", "parent-1", "--title", "synthetic",
                "--executors", "owner",
            ])),
            ("todo_attachment", lambda client: client.run([
                "todo", "task", "add-attachment", "--task-id", "task-42", "--file", "/tmp/file",
            ])),
            ("chat_send", lambda client: client.run([
                "chat", "message", "send", "--text", "synthetic",
            ])),
            ("raw_card_api", lambda client: client.run([
                "api", "POST", "/v1.0/card/instances/createAndDeliver",
                "--data", "{}",
            ])),
            ("run_at_create", lambda client: client.run_at([
                "todo", "task", "create", "--title", "synthetic", "--executors", "owner",
            ], "/tmp")),
        )
        for operation, call in calls:
            with self.subTest(operation=operation):
                with patch("dws_adapter.subprocess.run") as run:
                    with self.assertRaises(DwsError):
                        call(DwsClient(executable="dws-test", confirmed_official_writes=True))
                self.assertEqual(run.call_count, 0)

    def test_review_card_delivery_fails_closed_without_authorized_cifra_configuration(self):
        source = CifraCardCredentialSource(
            "DINGTODO_CIFRA_CARD_CLIENT_ID",
            "DINGTODO_CIFRA_CARD_CLIENT_SECRET",
        )
        common = {
            "batch_id": "batch_12345678",
            "template_fields": card_template_fields(),
            "open_space_id": "dtv1.card//IM_ROBOT.owner-42",
            "credential_source": source,
            "authorization_enabled": True,
            "robot_code": "cifra-robot",
            "expected_robot_code": "cifra-robot",
            "published_template_id": "published-template-1",
            "published_template_version": "version-3",
        }
        denied = [
            ("client_gate", DwsClient(executable="dws-test"), common),
            (
                "feature_gate",
                DwsClient(executable="dws-test", confirmed_official_writes=True),
                {**common, "authorization_enabled": False},
            ),
            (
                "missing_source",
                DwsClient(executable="dws-test", confirmed_official_writes=True),
                {**common, "credential_source": None},
            ),
            (
                "missing_source_env_values",
                DwsClient(executable="dws-test", confirmed_official_writes=True),
                common,
            ),
            (
                "generic_source",
                DwsClient(executable="dws-test", confirmed_official_writes=True),
                {**common, "credential_source": CifraCardCredentialSource(
                    "DWS_CLIENT_ID", "DWS_CLIENT_SECRET",
                )},
            ),
            (
                "mismatched_robot",
                DwsClient(executable="dws-test", confirmed_official_writes=True),
                {**common, "expected_robot_code": "other-robot"},
            ),
            (
                "missing_template_version",
                DwsClient(executable="dws-test", confirmed_official_writes=True),
                {**common, "published_template_version": None},
            ),
            (
                "missing_template_id",
                DwsClient(executable="dws-test", confirmed_official_writes=True),
                {**common, "published_template_id": None},
            ),
            (
                "invalid_template_field_type",
                DwsClient(executable="dws-test", confirmed_official_writes=True),
                {**common, "template_fields": {"form_fields": ["not", "a", "string"]}},
            ),
            (
                "unexpected_template_variable",
                DwsClient(executable="dws-test", confirmed_official_writes=True),
                {**common, "template_fields": {"review_text": "not the loop variable"}},
            ),
        ]
        for case, client, call_kwargs in denied:
            with self.subTest(case=case):
                with patch.dict("os.environ", {}, clear=True):
                    with patch("dws_adapter.subprocess.run") as run:
                        with self.assertRaises(DwsError):
                            client.create_and_deliver_review_card(**call_kwargs)
                self.assertEqual(run.call_count, 0)

    def test_review_card_delivery_uses_cifra_app_credentials_and_verifies_receipt(self):
        source = CifraCardCredentialSource(
            "DINGTODO_CIFRA_CARD_CLIENT_ID",
            "DINGTODO_CIFRA_CARD_CLIENT_SECRET",
        )
        client_id = "synthetic-cifra-client-id"
        client_secret = "synthetic-cifra-client-secret"
        response = {
            "success": True,
            "result": {
                "outTrackId": "batch_12345678",
                "deliverResults": [{
                    "spaceType": "IM_ROBOT",
                    "spaceId": "owner-42",
                    "success": True,
                    "carrierId": "synthetic-carrier",
                }],
            },
        }
        completed = subprocess.CompletedProcess(
            args=[], returncode=0, stdout=json.dumps(response), stderr="",
        )
        fields = card_template_fields("synthetic review " + "x" * 2048)
        with patch.dict("os.environ", {
            source.client_id_env: client_id,
            source.client_secret_env: client_secret,
            "DWS_CLIENT_ID": "unrelated-generic-id",
            "DWS_CLIENT_SECRET": "unrelated-generic-secret",
        }, clear=False):
            with patch("dws_adapter.subprocess.run", return_value=completed) as run:
                result = DwsClient(
                    executable="dws-test", confirmed_official_writes=True,
                ).create_and_deliver_review_card(
                    batch_id="batch_12345678",
                    template_fields=fields,
                    open_space_id="dtv1.card//IM_ROBOT.owner-42",
                    credential_source=source,
                    authorization_enabled=True,
                    robot_code="cifra-robot",
                    expected_robot_code="cifra-robot",
                    published_template_id="published-template-1",
                    published_template_version="version-3",
                )

        self.assertTrue(result["delivered"])
        self.assertEqual(result["out_track_id"], "batch_12345678")
        self.assertEqual(result["configured_template_version"], "version-3")
        self.assertEqual(result["carrier_id"], "synthetic-carrier")
        command = run.call_args.args[0]
        self.assertEqual(
            command[1:4],
            ["api", "POST", "/v1.0/card/instances/createAndDeliver"],
        )
        self.assertEqual(command[-2:], ["--format", "json"])
        self.assertNotIn("--yes", command)
        self.assertNotIn(client_secret, command)
        self.assertEqual(command[command.index("--data") + 1], "-")
        body = json.loads(run.call_args.kwargs["input"])
        self.assertEqual(body["cardTemplateId"], "published-template-1")
        self.assertEqual(body["outTrackId"], "batch_12345678")
        self.assertEqual(body["callbackType"], "STREAM")
        self.assertEqual(body["cardData"], {"cardParamMap": fields})
        self.assertGreater(len(fields["form_fields"].encode("utf-8")), 1024)
        self.assertEqual(body["openSpaceId"], "dtv1.card//IM_ROBOT.owner-42")
        self.assertEqual(body["imRobotOpenDeliverModel"], {
            "spaceType": "IM_ROBOT", "robotCode": "cifra-robot",
        })
        self.assertNotIn("templateVersion", body)
        child_env = run.call_args.kwargs["env"]
        self.assertEqual(child_env["DWS_CLIENT_ID"], client_id)
        self.assertEqual(child_env["DWS_CLIENT_SECRET"], client_secret)
        self.assertNotEqual(child_env["DWS_CLIENT_SECRET"], "unrelated-generic-secret")

    def test_review_card_delivery_rejects_failed_partial_or_mismatched_receipts(self):
        source = CifraCardCredentialSource(
            "DINGTODO_CIFRA_CARD_CLIENT_ID",
            "DINGTODO_CIFRA_CARD_CLIENT_SECRET",
        )
        common = {
            "batch_id": "batch_12345678",
            "template_fields": card_template_fields(),
            "open_space_id": "dtv1.card//IM_ROBOT.owner-42",
            "credential_source": source,
            "authorization_enabled": True,
            "robot_code": "cifra-robot",
            "expected_robot_code": "cifra-robot",
            "published_template_id": "published-template-1",
            "published_template_version": "version-3",
        }
        good_delivery = {
            "spaceType": "IM_ROBOT", "spaceId": "owner-42", "success": True,
        }
        invalid_receipts = [
            {"success": False, "result": {"outTrackId": "batch_12345678", "deliverResults": [good_delivery]}},
            {"success": True, "result": {"outTrackId": "different-batch", "deliverResults": [good_delivery]}},
            {"success": True, "result": {"outTrackId": "batch_12345678", "deliverResults": []}},
            {"success": True, "result": {"outTrackId": "batch_12345678", "deliverResults": [{**good_delivery, "success": False}]}},
            {"success": True, "result": {"outTrackId": "batch_12345678", "deliverResults": [{**good_delivery, "spaceId": "other-owner"}]}},
            {"success": True, "result": {"outTrackId": "batch_12345678", "deliverResults": [good_delivery, good_delivery]}},
        ]
        with patch.dict("os.environ", {
            source.client_id_env: "synthetic-cifra-client-id",
            source.client_secret_env: "synthetic-cifra-client-secret",
        }, clear=False):
            for receipt in invalid_receipts:
                completed = subprocess.CompletedProcess(
                    args=[], returncode=0, stdout=json.dumps(receipt), stderr="",
                )
                with self.subTest(receipt=receipt):
                    with patch("dws_adapter.subprocess.run", return_value=completed):
                        with self.assertRaises(DwsError):
                            DwsClient(
                                executable="dws-test", confirmed_official_writes=True,
                            ).create_and_deliver_review_card(**common)

    def test_review_card_api_failure_does_not_expose_secret_or_claim_delivery(self):
        source = CifraCardCredentialSource(
            "DINGTODO_CIFRA_CARD_CLIENT_ID",
            "DINGTODO_CIFRA_CARD_CLIENT_SECRET",
        )
        secret = "synthetic-secret-must-not-escape"
        completed = subprocess.CompletedProcess(
            args=[], returncode=22, stdout="{}", stderr=secret,
        )
        with patch.dict("os.environ", {
            source.client_id_env: "synthetic-cifra-client-id",
            source.client_secret_env: secret,
        }, clear=False):
            with patch("dws_adapter.subprocess.run", return_value=completed):
                with self.assertRaises(DwsError) as caught:
                    DwsClient(
                        executable="dws-test", confirmed_official_writes=True,
                    ).create_and_deliver_review_card(
                        batch_id="batch_12345678",
                        template_fields=card_template_fields(),
                        open_space_id="dtv1.card//IM_ROBOT.owner-42",
                        credential_source=source,
                        authorization_enabled=True,
                        robot_code="cifra-robot",
                        expected_robot_code="cifra-robot",
                        published_template_id="published-template-1",
                        published_template_version="version-3",
                    )
        self.assertNotIn(secret, str(caught.exception))

    def test_generic_run_keeps_create_dry_run_available_without_authorization(self):
        preview = {"dry_run": True, "executed": False}
        completed = subprocess.CompletedProcess(
            args=[], returncode=0, stdout=json.dumps(preview), stderr="",
        )
        with patch("dws_adapter.subprocess.run", return_value=completed) as run:
            result = DwsClient(executable="dws-test").run([
                "todo", "task", "create", "--title", "synthetic", "--executors", "owner",
                "--dry-run",
            ])
        self.assertEqual(result, preview)
        command = run.call_args.args[0]
        self.assertIn("--dry-run", command)
        self.assertNotIn("--yes", command)
        self.assertEqual(command[-2:], ["--format", "json"])

    def test_generic_run_rejects_malformed_or_ambiguous_create_dry_runs(self):
        malformed_calls = (
            ("dry_run_used_as_title_value", [
                "todo", "task", "create", "--title", "--dry-run", "--executors", "owner",
            ]),
            ("dry_run_used_as_executor_value", [
                "todo", "task", "create", "--title", "synthetic", "--executors", "--dry-run",
            ]),
            ("duplicate_dry_run", [
                "todo", "task", "create", "--title", "synthetic", "--executors", "owner",
                "--dry-run", "--dry-run",
            ]),
            ("dry_run_not_last", [
                "todo", "task", "create", "--title", "synthetic", "--executors", "owner",
                "--dry-run", "--priority", "20",
            ]),
            ("unknown_preview_flag", [
                "todo", "task", "create", "--title", "synthetic", "--executors", "owner",
                "--unexpected", "value", "--dry-run",
            ]),
            ("write_and_preview_flags", [
                "todo", "task", "create", "--title", "synthetic", "--executors", "owner",
                "--yes", "--dry-run",
            ]),
        )
        for case, args in malformed_calls:
            with self.subTest(case=case):
                with patch("dws_adapter.subprocess.run") as run:
                    with self.assertRaises(DwsError):
                        DwsClient(executable="dws-test").run(args)
                self.assertEqual(run.call_count, 0)

    def test_confirmed_write_authorization_does_not_change_generic_read_commands(self):
        completed = subprocess.CompletedProcess(
            args=[], returncode=0, stdout=json.dumps({"data": {"todoDetailModel": {"taskId": "task-42"}}}), stderr="",
        )
        with patch("dws_adapter.subprocess.run", return_value=completed) as run:
            DwsClient(executable="dws-test", confirmed_official_writes=True).run(
                ["todo", "task", "get", "--task-id", "task-42"]
            )
        self.assertEqual(
            run.call_args.args[0],
            ["dws-test", "todo", "task", "get", "--task-id", "task-42", "--format", "json"],
        )

    def test_shortcut_write_rejects_conflicting_unified_wire_envelopes_before_readback(self):
        verified = {
            "taskId": "task-42", "commentId": "comment-7", "content": "source",
            "verified": True, "isDone": True, "alreadyInTargetState": False,
            "todo": {"taskId": "task-42", "subject": "new"},
        }
        invalid_payloads = [
            {"ok": False, "outcome": "failure", "data": verified},
            {"ok": True, "outcome": "partial", "data": verified},
            {"ok": True, "outcome": "pending", "data": verified},
            {"ok": True, "outcome": "success", "dry_run": True, "data": verified},
            {"ok": True, "outcome": "success", "data": {**verified, "executed": False}},
        ]
        for payload in invalid_payloads:
            with self.subTest(payload=payload):
                completed = subprocess.CompletedProcess(
                    args=[], returncode=0, stdout=json.dumps(payload), stderr=""
                )
                with patch("dws_adapter.subprocess.run", return_value=completed) as run:
                    with self.assertRaises(DwsError):
                        DwsClient(confirmed_official_writes=True).add_source_comment("task-42", "source")
                self.assertEqual(run.call_count, 1)

    def test_nonzero_official_error_content_is_not_exposed(self):
        secret = "synthetic-test-secret"
        responses = [
            subprocess.CompletedProcess(
                args=[], returncode=1,
                stdout=json.dumps({"ok": False, "outcome": "failure", "error": secret}),
                stderr="",
            ),
            subprocess.CompletedProcess(
                args=[], returncode=0,
                stdout=json.dumps({
                    "ok": True, "outcome": "success", "error": secret,
                    "data": {"taskId": "task-42", "commentId": "comment-1", "content": "source", "verified": True},
                }),
                stderr="",
            ),
        ]
        with patch("dws_adapter.subprocess.run", side_effect=responses):
            with self.assertRaises(DwsError) as error:
                DwsClient().run(["todo", "task", "get", "--task-id", "task-42"])
        self.assertNotIn(secret, str(error.exception))
        with patch("dws_adapter.subprocess.run", return_value=responses[1]):
            with self.assertRaises(DwsError) as error:
                DwsClient(confirmed_official_writes=True).add_source_comment("task-42", "source")
        self.assertNotIn(secret, str(error.exception))

    def test_comment_timeout_is_unknown_and_is_not_retried(self):
        with patch(
            "dws_adapter.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd=["dws", "todo", "+comment"], timeout=60),
        ) as run:
            with self.assertRaises(DwsError):
                DwsClient(confirmed_official_writes=True).add_source_comment("123456", "原文")
        self.assertEqual(run.call_count, 1)

    def test_comment_readback_timeout_is_reported_without_retry(self):
        with patch(
            "dws_adapter.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd=["dws", "todo", "comment", "list"], timeout=60),
        ) as run:
            with self.assertRaises(DwsError):
                DwsClient().verify_source_comment("123456", "原文")
        self.assertEqual(run.call_count, 1)

    def test_linked_update_and_done_args_target_only_the_existing_task(self):
        self.assertEqual(
            build_update_args(
                "task-42", title="新标题", due_iso="2026-10-10T18:00:00+08:00", priority=30
            ),
            [
                "todo", "+update", "--task-id", "task-42", "--title", "新标题",
                "--due", "2026-10-10T18:00:00+08:00", "--priority", "30",
            ],
        )
        self.assertEqual(build_complete_args("task-42"), ["todo", "+complete", "--task-id", "task-42"])
        self.assertEqual(build_reopen_args("task-42"), ["todo", "+reopen", "--task-id", "task-42"])

    def test_linked_update_requires_explicit_valid_values(self):
        for values in (
            {}, {"title": ""}, {"due_iso": "2026-10-10"},
            {"priority": 25}, {"priority": 20.5},
        ):
            with self.subTest(values=values):
                with self.assertRaises(ValueError):
                    build_update_args("task-42", **values)

    def test_linked_update_uses_official_shortcut_then_checks_remote_fields(self):
        due_iso = "2026-10-10T18:00:00+08:00"
        due_ms = 1791626400000
        detail = {
            "taskId": "task-42", "subject": "新标题", "dueTime": due_ms,
            "priority": 30, "isDone": False,
        }
        outputs = [
            {"ok": True, "outcome": "success", "data": {"taskId": "task-42", "verified": True, "todo": detail}},
            {"data": {"todoDetailModel": detail}},
        ]

        def fake_run(command, **kwargs):
            output = outputs.pop(0)
            return subprocess.CompletedProcess(command, 0, json.dumps(output), "")

        with patch("dws_adapter.subprocess.run", side_effect=fake_run) as run:
            result = DwsClient(
                executable="dws-test", confirmed_official_writes=True,
            ).update_linked_todo(
                "task-42", title="新标题", due_iso=due_iso, priority=30
            )

        self.assertEqual(result["task_id"], "task-42")
        self.assertEqual(result["verified_fields"], ["title", "due_iso", "priority"])
        self.assertEqual(
            run.call_args_list[0].args[0],
            [
                "dws-test", "todo", "+update", "--task-id", "task-42", "--title", "新标题",
                "--due", due_iso, "--priority", "30", "--yes", "--format", "json",
            ],
        )
        self.assertEqual(run.call_args_list[1].args[0][1:5], ["todo", "task", "get", "--task-id"])

    def test_linked_complete_and_reopen_use_official_status_and_remote_readback(self):
        for command, method_name, target in (
            ("+complete", "complete_linked_todo", True),
            ("+reopen", "reopen_linked_todo", False),
        ):
            with self.subTest(command=command):
                detail = {"taskId": "task-42", "subject": "任务", "isDone": target}
                outputs = [
                    {"ok": True, "outcome": "success", "data": {
                        "taskId": "task-42", "isDone": target,
                        "verified": True, "alreadyInTargetState": False,
                    }},
                    {"data": {"todoDetailModel": detail}},
                ]

                def fake_run(args, **kwargs):
                    output = outputs.pop(0)
                    return subprocess.CompletedProcess(args, 0, json.dumps(output), "")

                with patch("dws_adapter.subprocess.run", side_effect=fake_run) as run:
                    result = getattr(DwsClient(
                        executable="dws-test", confirmed_official_writes=True,
                    ), method_name)("task-42")

                self.assertEqual(result["task_id"], "task-42")
                self.assertIs(result["is_done"], target)
                self.assertEqual(
                    run.call_args_list[0].args[0],
                    ["dws-test", "todo", command, "--task-id", "task-42", "--yes", "--format", "json"],
                )

    def test_linked_done_rejects_mismatched_task_or_readback_state(self):
        bad_outputs = [
            {"ok": True, "outcome": "success", "data": {"taskId": "other", "isDone": True, "verified": True}},
            {"ok": True, "outcome": "success", "data": {"taskId": "task-42", "isDone": True, "verified": False}},
            {"ok": True, "outcome": "success", "data": {"taskId": "task-42", "isDone": False, "verified": True}},
        ]
        for first_output in bad_outputs:
            with self.subTest(output=first_output):
                outputs = [first_output]

                def fake_run(args, **kwargs):
                    output = outputs.pop(0)
                    return subprocess.CompletedProcess(args, 0, json.dumps(output), "")

                with patch("dws_adapter.subprocess.run", side_effect=fake_run):
                    with self.assertRaises(DwsError):
                        DwsClient(confirmed_official_writes=True).complete_linked_todo("task-42")

    def test_linked_update_rejects_remote_readback_mismatch(self):
        outputs = [
            {"ok": True, "outcome": "success", "data": {"taskId": "task-42", "verified": True, "todo": {}}},
            {"data": {"todoDetailModel": {"taskId": "task-42", "subject": "old title"}}},
        ]

        def fake_run(args, **kwargs):
            return subprocess.CompletedProcess(args, 0, json.dumps(outputs.pop(0)), "")

        with patch("dws_adapter.subprocess.run", side_effect=fake_run) as run:
            with self.assertRaises(DwsError):
                DwsClient(confirmed_official_writes=True).update_linked_todo("task-42", title="new title")
        self.assertEqual(run.call_count, 2)

    def test_linked_update_rejects_success_receipt_without_official_todo_detail(self):
        completed = subprocess.CompletedProcess(
            [], 0, json.dumps({"ok": True, "outcome": "success", "data": {"taskId": "task-42", "verified": True}}), ""
        )
        with patch("dws_adapter.subprocess.run", return_value=completed) as run:
            with self.assertRaises(DwsError):
                DwsClient(confirmed_official_writes=True).update_linked_todo("task-42", title="new title")
        self.assertEqual(run.call_count, 1)

    def test_comment_readback_requires_exact_content(self):
        comments = {"comments": [{"content": "old"}, {"content": "source-comment"}]}
        self.assertTrue(comment_is_present(comments, "source-comment"))
        self.assertFalse(comment_is_present(comments, "source-comment-2"))

    def test_task_readback_checks_due_time_and_priority(self):
        plan = {
            "title": "完成台账核对",
            "executor_names": ["李林骁"],
            "due_iso": "2026-09-22T18:00:00+08:00",
            "priority": 20,
        }
        detail = {
            "subject": "完成台账核对",
            "executorInfos": [{"name": "李林骁"}],
            "dueTime": "2026-09-22T18:00:00+08:00",
            "priority": 20,
            "isDone": False,
        }
        self.assertEqual(verify_todo(detail, plan), [])
        detail["dueTime"] = "2026-09-23T18:00:00+08:00"
        self.assertIn("due_time_mismatch", verify_todo(detail, plan))


if __name__ == "__main__":
    unittest.main()
