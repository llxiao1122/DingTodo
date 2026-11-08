from __future__ import annotations

import json
import subprocess
import unittest
from unittest.mock import patch

from dws_runtime import (
    DwsInvocationError,
    build_dws_argv,
    run_dws_json,
    run_dws_json_object,
    start_dws_stream,
)
from listener_engine import consume_argv, todo_consume_argv


class DwsRuntimeTests(unittest.TestCase):
    def test_build_dws_argv_adds_json_format_once(self):
        self.assertEqual(
            build_dws_argv(["chat", "+chat-messages"], executable="dws-test", json_output=True),
            ["dws-test", "chat", "+chat-messages", "--format", "json"],
        )
        self.assertEqual(
            build_dws_argv(
                ["chat", "+chat-messages", "-f", "json"], executable="dws-test", json_output=True,
            ),
            ["dws-test", "chat", "+chat-messages", "-f", "json"],
        )

    def test_json_runner_uses_shared_command_builder_and_returns_decoded_payload(self):
        response = {"ok": True, "data": {"count": 2}}
        completed = subprocess.CompletedProcess([], 0, json.dumps(response), "")

        with patch("dws_runtime.subprocess.run", return_value=completed) as run:
            self.assertEqual(
                run_dws_json(["chat", "+chat-messages"], executable="dws-test", timeout=9),
                response,
            )

        self.assertEqual(
            run.call_args.args[0],
            ["dws-test", "chat", "+chat-messages", "--format", "json"],
        )
        self.assertEqual(run.call_args.kwargs["timeout"], 9)

    def test_json_runner_uses_stable_errors_without_exposing_provider_output(self):
        with patch(
            "dws_runtime.subprocess.run",
            return_value=subprocess.CompletedProcess([], 0, "private-token", ""),
        ):
            with self.assertRaisesRegex(DwsInvocationError, "dws_non_json_response"):
                run_dws_json(["profile", "list"])

        with patch(
            "dws_runtime.subprocess.run",
            return_value=subprocess.CompletedProcess([], 7, json.dumps({"error": "private"}), ""),
        ):
            with self.assertRaisesRegex(DwsInvocationError, "dws_command_failed"):
                run_dws_json(["profile", "list"])

    def test_json_object_runner_shares_object_validation_and_caller_safe_error_codes(self):
        response = {"success": True, "data": {"count": 1}}
        with patch("dws_runtime.run_dws_json", return_value=response):
            self.assertEqual(
                run_dws_json_object(["chat", "+conversation-info"]), response,
            )

        with patch(
            "dws_runtime.run_dws_json",
            side_effect=DwsInvocationError("dws_non_json_response", returncode=0),
        ):
            with self.assertRaisesRegex(DwsInvocationError, "dws_response_invalid"):
                run_dws_json_object(
                    ["chat", "+conversation-info"],
                    invalid_json_reason="dws_response_invalid",
                )

        with patch(
            "dws_runtime.run_dws_json",
            side_effect=DwsInvocationError("dws_command_failed", returncode=5),
        ):
            with self.assertRaisesRegex(DwsInvocationError, "dws_read_failed"):
                run_dws_json_object(
                    ["chat", "+conversation-info"],
                    command_failure_reason="dws_read_failed",
                )

        with patch("dws_runtime.run_dws_json", return_value=[]):
            with self.assertRaisesRegex(DwsInvocationError, "invalid_dws_response"):
                run_dws_json_object(
                    ["chat", "+conversation-info"],
                    invalid_object_reason="invalid_dws_response",
                )

    def test_json_runner_rejects_non_json_format_and_stream_wrapper_preserves_ndjson_argv(self):
        with self.assertRaisesRegex(ValueError, "dws_json_output_required"):
            run_dws_json(["event", "consume", "-f", "ndjson"])

        calls = []

        def fake_popen(argv, **kwargs):
            calls.append((argv, kwargs))
            return object()

        process = start_dws_stream(
            ["event", "consume", "group-event", "-f", "ndjson"],
            executable="dws-test", popen_factory=fake_popen, text=True,
        )

        self.assertIsNotNone(process)
        self.assertEqual(
            calls[0][0], ["dws-test", "event", "consume", "group-event", "-f", "ndjson"],
        )
        self.assertTrue(calls[0][1]["text"])

    def test_im_group_stream_uses_official_listener_while_todo_lifecycle_stays_on_consume(self):
        self.assertEqual(
            consume_argv("conversation-id", executable="dws-test"),
            [
                "dws-test", "event", "+listen-im", "--kind", "group", "--chat-id",
                "conversation-id", "-f", "ndjson",
            ],
        )
        todo_args = todo_consume_argv(executable="dws-test")
        self.assertEqual(todo_args[1:3], ["event", "consume"])
