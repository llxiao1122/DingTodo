"""Small, explicit dws adapter used by the local auto-todo runner."""

from __future__ import annotations

import json
import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Mapping

from dws_runtime import DwsInvocationError, run_dws_json


class DwsError(RuntimeError):
    pass


@dataclass(frozen=True)
class CifraCardCredentialSource:
    """Names of explicitly configured Cifra-app credential environment vars."""

    client_id_env: str
    client_secret_env: str


def build_create_args(
    *, title: str, executor_ids: Iterable[str], due_iso: str | None, priority: int | None
) -> list[str]:
    ids = [str(value).strip() for value in executor_ids if str(value).strip()]
    if not title.strip():
        raise ValueError("title is required")
    if not ids:
        raise ValueError("at least one executor is required")
    args = ["todo", "task", "create", "--title", title, "--executors", ",".join(ids)]
    if due_iso:
        args.extend(["--due", due_iso])
    if priority is not None:
        if int(priority) not in (10, 20, 30, 40):
            raise ValueError("priority must be 10, 20, 30, or 40")
        args.extend(["--priority", str(int(priority))])
    return args


def build_create_sub_args(
    *, parent_task_id: str, title: str, executor_ids: Iterable[str],
    due_iso: str, priority: int = 20,
) -> list[str]:
    parent_id = str(parent_task_id or "").strip()
    if not parent_id:
        raise ValueError("parent_task_id is required")
    ids = [str(value).strip() for value in executor_ids if str(value).strip()]
    if not ids or len(ids) != len(set(ids)):
        raise ValueError("unique executor IDs are required")
    if not isinstance(title, str) or not title.strip():
        raise ValueError("title is required")
    # Reuse the strict timestamp validation already used by linked updates.
    build_update_args("validation-only", due_iso=due_iso)
    if isinstance(priority, bool) or not isinstance(priority, int) or priority not in (10, 20, 30, 40):
        raise ValueError("priority must be 10, 20, 30, or 40")
    return [
        "todo", "task", "create-sub", "--parent-id", parent_id,
        "--title", title.strip(), "--executors", ",".join(ids),
        "--due", due_iso, "--priority", str(priority),
    ]


def build_comment_args(task_id: str, content: str) -> list[str]:
    if not str(task_id).strip():
        raise ValueError("task_id is required")
    if not str(content).strip():
        raise ValueError("comment content is required")
    return ["todo", "+comment", "--task-id", str(task_id), "--content", content]


def build_update_args(
    task_id: str,
    *,
    title: str | None = None,
    due_iso: str | None = None,
    priority: int | None = None,
) -> list[str]:
    task_id_text = str(task_id)
    if not task_id_text.strip() or task_id_text != task_id_text.strip():
        raise ValueError("task_id is required")
    if title is None and due_iso is None and priority is None:
        raise ValueError("at least one explicit update field is required")
    args = ["todo", "+update", "--task-id", task_id_text]
    if title is not None:
        if not str(title).strip():
            raise ValueError("title must not be empty")
        args.extend(["--title", str(title)])
    if due_iso is not None:
        if not isinstance(due_iso, str) or not re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})",
            due_iso,
        ):
            raise ValueError("due_iso must be RFC3339")
        try:
            parsed = datetime.fromisoformat(due_iso.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("due_iso must be RFC3339") from exc
        if parsed.tzinfo is None:
            raise ValueError("due_iso must include a timezone")
        args.extend(["--due", str(due_iso)])
    if priority is not None:
        if isinstance(priority, bool) or not isinstance(priority, int) or priority not in (10, 20, 30, 40):
            raise ValueError("priority must be 10, 20, 30, or 40")
        args.extend(["--priority", str(priority)])
    return args


def build_complete_args(task_id: str) -> list[str]:
    task_id_text = str(task_id)
    if not task_id_text.strip() or task_id_text != task_id_text.strip():
        raise ValueError("task_id is required")
    return ["todo", "+complete", "--task-id", task_id_text]


def build_reopen_args(task_id: str) -> list[str]:
    task_id_text = str(task_id)
    if not task_id_text.strip() or task_id_text != task_id_text.strip():
        raise ValueError("task_id is required")
    return ["todo", "+reopen", "--task-id", task_id_text]


def build_download_resource_args(
    *, resource_id: str, message_id: str, conversation_id: str, output: str = "."
) -> list[str]:
    values = {
        "resource_id": resource_id,
        "message_id": message_id,
        "conversation_id": conversation_id,
        "output": output,
    }
    if any(not str(value).strip() for value in values.values()):
        raise ValueError("resource download identity and output are required")
    if str(output).startswith("/") or ".." in str(output).split("/"):
        raise ValueError("output must be a relative path without parent traversal")
    return [
        "chat",
        "+messages-resource-download",
        "--resource-id",
        str(resource_id),
        "--type",
        "mediaId",
        "--message-id",
        str(message_id),
        "--open-conversation-id",
        str(conversation_id),
        "--output",
        str(output),
    ]


def build_add_attachment_args(task_id: str, file_path: str) -> list[str]:
    if not str(task_id).strip():
        raise ValueError("task_id is required")
    if not str(file_path).strip():
        raise ValueError("file_path is required")
    return [
        "todo",
        "task",
        "add-attachment",
        "--task-id",
        str(task_id),
        "--file",
        str(file_path),
    ]


def attachment_is_present(payload: dict[str, Any], file_name: str) -> bool:
    result = _result(payload)
    if result is None:
        result = payload
    if not isinstance(result, dict):
        return False
    attachments = result.get("attachments") or []
    return any(
        isinstance(item, dict) and str(item.get("fileName") or "") == str(file_name)
        for item in attachments
    )


def comment_is_present(payload: dict[str, Any], content: str) -> bool:
    """Check exact comment content in the provider's list response."""
    result = _result(payload)
    if result is None:
        result = payload
    if isinstance(result, dict):
        comments = result.get("comments") or result.get("items") or []
    elif isinstance(result, list):
        comments = result
    else:
        comments = []
    return any(str(item.get("content", "")) == content for item in comments if isinstance(item, dict))


def _result(payload: dict[str, Any]) -> Any:
    if isinstance(payload.get("result"), dict) and "data" in payload["result"]:
        return payload["result"]["data"]
    if "data" in payload and isinstance(payload["data"], dict):
        data = payload["data"]
        return data.get("result", data)
    return payload.get("result")


def _require_official_success(payload: dict[str, Any]) -> dict[str, Any]:
    """Return only the live unified success data; reject legacy/ambiguous shapes."""
    if not isinstance(payload, dict):
        raise DwsError("official command returned a malformed JSON envelope")
    if payload.get("ok") is not True or payload.get("outcome") != "success":
        raise DwsError("official command did not report a complete success")
    if payload.get("error") not in (None, ""):
        raise DwsError("official command returned an error envelope")
    if "success" in payload and payload.get("success") is not True:
        raise DwsError("official command returned a conflicting success marker")
    if payload.get("partial") is True or payload.get("complete") is False:
        raise DwsError("official command returned an incomplete result")
    for key in ("hasMore", "has_more"):
        if payload.get(key) is True:
            raise DwsError("official command returned a paginated partial result")
    for key in ("stopReason", "stop_reason"):
        if payload.get(key) not in (None, ""):
            raise DwsError("official command stopped before a complete result")
    if payload.get("failures") not in (None, [], {}):
        raise DwsError("official command returned failures")
    if payload.get("dry_run") is True or payload.get("dryRun") is True:
        raise DwsError("official command returned a dry-run preview")
    if "result" in payload or not isinstance(payload.get("data"), dict):
        raise DwsError("official command returned a missing or ambiguous data envelope")

    data = payload["data"]
    if "data" in data or "result" in data:
        raise DwsError("official command returned a nested or ambiguous data envelope")
    if data.get("error") not in (None, ""):
        raise DwsError("official command returned an error envelope")
    if data.get("dry_run") is True or data.get("dryRun") is True or data.get("executed") is False:
        raise DwsError("official command did not execute the requested write")
    if data.get("partial") is True or data.get("complete") is False:
        raise DwsError("official command returned an incomplete result")
    for key in ("hasMore", "has_more"):
        if data.get(key) is True:
            raise DwsError("official command returned a paginated partial result")
    for key in ("stopReason", "stop_reason"):
        if data.get(key) not in (None, ""):
            raise DwsError("official command stopped before a complete result")
    if data.get("failures") not in (None, [], {}):
        raise DwsError("official command returned failures")
    return data


def _verified_shortcut_result(payload: dict[str, Any], task_id: str) -> dict[str, Any]:
    result = _require_official_success(payload)
    if not isinstance(result, dict):
        raise DwsError("official todo shortcut returned no result object")
    if result.get("taskId") != str(task_id) or result.get("verified") is not True:
        raise DwsError("official todo shortcut result is missing, mismatched, or unverified")
    return result


class DwsClient:
    """Read/write wrapper. There is intentionally no group-send method."""

    _READ_ONLY_COMMANDS = frozenset({
        ("contact", "user", "get-self"),
        ("contact", "user", "search"),
        ("todo", "task", "get"),
        ("todo", "+search"),
        ("todo", "comment", "list"),
        ("todo", "task", "list-attachment"),
        ("chat", "+messages-resource-download"),
    })
    _WRITE_COMMANDS = frozenset({
        ("todo", "task", "create"),
        ("todo", "task", "create-sub"),
        ("todo", "task", "add-attachment"),
        ("todo", "+comment"),
        ("todo", "+update"),
        ("todo", "+complete"),
        ("todo", "+reopen"),
        ("api", "POST", "/v1.0/card/instances/createAndDeliver"),
    })

    def __init__(
        self,
        executable: str = "dws",
        timeout: int = 60,
        *,
        confirmed_official_writes: bool = False,
    ):
        self.executable = executable
        self.timeout = timeout
        self.confirmed_official_writes = bool(confirmed_official_writes)

    def _run_confirmed_official_write(
        self, args: list[str], *, append_yes: bool = True,
        env: Mapping[str, str] | None = None,
        input_text: str | None = None,
    ) -> dict[str, Any]:
        if not self.confirmed_official_writes:
            raise DwsError("official write requires explicit confirmed authorization")
        command = self._command_path(args)
        if command not in self._WRITE_COMMANDS:
            raise DwsError("unsupported official write command")
        if "--yes" in args or "--dry-run" in args:
            raise DwsError("write confirmation flags are adapter-managed")
        write_args = [*args]
        if append_yes:
            write_args.append("--yes")
        return self._run(write_args, env=env, input_text=input_text)

    def run(self, args: list[str]) -> dict[str, Any]:
        return self._run_read_or_preview(args)

    def run_at(self, args: list[str], cwd: str) -> dict[str, Any]:
        return self._run_read_or_preview(args, cwd=cwd)

    @classmethod
    def _command_path(cls, args: list[str]) -> tuple[str, ...] | None:
        for command in cls._READ_ONLY_COMMANDS | cls._WRITE_COMMANDS:
            if len(args) >= len(command) and tuple(args[:len(command)]) == command:
                return command
        return None

    @classmethod
    def _is_create_dry_run(cls, args: list[str]) -> bool:
        if (
            cls._command_path(args) != ("todo", "task", "create")
            or args[-1:] != ["--dry-run"]
            or args.count("--dry-run") != 1
            or "--yes" in args
        ):
            return False

        allowed_flags = {"--title", "--executors", "--due", "--priority"}
        values: dict[str, str] = {}
        index = 3
        while index < len(args) - 1:
            flag = args[index]
            if flag not in allowed_flags or flag in values or index + 1 >= len(args) - 1:
                return False
            value = args[index + 1]
            if not value or value.startswith("--"):
                return False
            values[flag] = value
            index += 2

        return "--title" in values and "--executors" in values

    def _run_read_or_preview(
        self, args: list[str], *, cwd: str | None = None,
    ) -> dict[str, Any]:
        command = self._command_path(args)
        if command in self._READ_ONLY_COMMANDS:
            if "--yes" in args or "--dry-run" in args:
                raise DwsError("read command cannot use write confirmation flags")
            return self._run(args, cwd=cwd)
        if self._is_create_dry_run(args):
            return self._run(args, cwd=cwd)
        if command in self._WRITE_COMMANDS:
            raise DwsError("official writes must use a confirmed write method")
        raise DwsError("unclassified dws command rejected")

    def _run(
        self, args: list[str], *, cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        input_text: str | None = None,
    ) -> dict[str, Any]:
        try:
            options: dict[str, Any] = {
                "executable": self.executable,
                "timeout": self.timeout,
                "cwd": cwd,
            }
            if env is not None:
                options["env"] = dict(env)
            if input_text is not None:
                options["input_text"] = input_text
            payload = run_dws_json(args, **options)
        except DwsInvocationError as exc:
            if exc.reason == "dws_non_json_response":
                raise DwsError("dws returned a non-JSON response") from None
            raise DwsError("dws command failed") from None
        if not isinstance(payload, dict):
            raise DwsError("dws response must be an object")
        return payload

    def create_and_deliver_review_card(
        self,
        *,
        batch_id: str,
        template_fields: dict[str, Any],
        open_space_id: str,
        credential_source: CifraCardCredentialSource | None = None,
        authorization_enabled: bool = False,
        robot_code: str | None = None,
        expected_robot_code: str | None = None,
        published_template_id: str | None = None,
        published_template_version: str | None = None,
    ) -> dict[str, Any]:
        """Deliver one review card using an explicitly selected Cifra app.

        The template version is pinned by the caller for configuration audit;
        the official API accepts only the template ID in its request body.
        No workflow calls this method by default.
        """
        if not self.confirmed_official_writes or authorization_enabled is not True:
            raise DwsError("card_delivery_not_authorized")
        if not isinstance(credential_source, CifraCardCredentialSource):
            raise DwsError("card_credential_source_required")
        client_id_suffix = "_CLIENT_ID"
        client_secret_suffix = "_CLIENT_SECRET"
        client_id_stem = (
            credential_source.client_id_env[:-len(client_id_suffix)]
            if isinstance(credential_source.client_id_env, str)
            and credential_source.client_id_env.endswith(client_id_suffix)
            else ""
        )
        client_secret_stem = (
            credential_source.client_secret_env[:-len(client_secret_suffix)]
            if isinstance(credential_source.client_secret_env, str)
            and credential_source.client_secret_env.endswith(client_secret_suffix)
            else ""
        )
        if (not isinstance(credential_source.client_id_env, str)
                or not isinstance(credential_source.client_secret_env, str)
                or not client_id_stem.startswith(("CIFRA_", "DINGTODO_CIFRA_"))
                or client_id_stem != client_secret_stem
                or credential_source.client_id_env in {"DWS_CLIENT_ID", "DWS_CLIENT_SECRET"}
                or credential_source.client_secret_env in {"DWS_CLIENT_ID", "DWS_CLIENT_SECRET"}
                or credential_source.client_id_env == credential_source.client_secret_env):
            raise DwsError("card_credential_source_invalid")
        if (not isinstance(robot_code, str) or not robot_code.strip()
                or not isinstance(expected_robot_code, str)
                or not expected_robot_code.strip()
                or robot_code != expected_robot_code):
            raise DwsError("card_robot_identity_mismatch")
        if (not isinstance(published_template_id, str) or not published_template_id.strip()
                or len(published_template_id) > 256
                or not isinstance(published_template_version, str)
                or not published_template_version.strip()
                or len(published_template_version) > 128):
            raise DwsError("card_published_template_required")
        if not isinstance(template_fields, dict):
            raise DwsError("card_template_fields_invalid")
        from review_card_adapter import (
            CARD_FORM_FIELDS_TEMPLATE_KEY,
            CARD_MAX_TEMPLATE_FIELD_BYTES,
        )

        for key, value in template_fields.items():
            max_value_bytes = (CARD_MAX_TEMPLATE_FIELD_BYTES
                               if key == CARD_FORM_FIELDS_TEMPLATE_KEY else 1024)
            if (not isinstance(key, str) or not key or len(key.encode("utf-8")) > 100
                    or not isinstance(value, str)
                    or len(value.encode("utf-8")) > max_value_bytes):
                raise DwsError("card_template_fields_invalid")
        robot_space_prefix = "dtv1.card//IM_ROBOT."
        if (not isinstance(open_space_id, str)
                or not open_space_id.startswith(robot_space_prefix)
                or not open_space_id[len(robot_space_prefix):]
                or ";" in open_space_id[len(robot_space_prefix):]):
            raise DwsError("card_target_invalid")

        from review_card_adapter import build_create_and_deliver_body

        try:
            body = build_create_and_deliver_body(
                batch_id=batch_id,
                template_id=published_template_id,
                template_fields=template_fields,
                open_space_id=open_space_id,
                robot_code=robot_code,
            )
            encoded_body = json.dumps(
                body, ensure_ascii=False, allow_nan=False, separators=(",", ":"),
            )
        except (TypeError, ValueError):
            raise DwsError("card_request_invalid") from None

        client_id = os.environ.get(credential_source.client_id_env)
        client_secret = os.environ.get(credential_source.client_secret_env)
        if not isinstance(client_id, str) or not client_id.strip() or not isinstance(client_secret, str) or not client_secret:
            raise DwsError("card_credential_source_unavailable")

        # DWS reads this complete pair to acquire an App Token. Override generic
        # environment credentials so this request cannot fall back to a user or
        # unrelated app identity. The secret never enters argv or the request body.
        child_env = os.environ.copy()
        child_env["DWS_CLIENT_ID"] = client_id
        child_env["DWS_CLIENT_SECRET"] = client_secret
        args = [
            "api", "POST", "/v1.0/card/instances/createAndDeliver",
            "--data", "-",
        ]
        payload = self._run_confirmed_official_write(
            args, append_yes=False, env=child_env, input_text=encoded_body,
        )
        target_id = open_space_id[len(robot_space_prefix):]
        result = payload.get("result")
        deliveries = result.get("deliverResults") if isinstance(result, dict) else None
        if (payload.get("success") is not True or not isinstance(result, dict)
                or result.get("outTrackId") != batch_id
                or not isinstance(deliveries, list) or len(deliveries) != 1):
            raise DwsError("card_delivery_not_verified")
        delivery = deliveries[0]
        if (not isinstance(delivery, dict) or delivery.get("success") is not True
                or delivery.get("spaceType") != "IM_ROBOT"
                or delivery.get("spaceId") != target_id
                or delivery.get("errorMsg") not in (None, "")):
            raise DwsError("card_delivery_not_verified")
        return {
            "delivered": True,
            "out_track_id": batch_id,
            "template_id": published_template_id,
            "configured_template_version": published_template_version,
            "robot_code": robot_code,
            "space_type": "IM_ROBOT",
            "space_id": target_id,
            "carrier_id": delivery.get("carrierId"),
        }

    def get_self(self) -> dict[str, str]:
        payload = self.run(["contact", "user", "get-self"])
        rows = _result(payload) or []
        if not isinstance(rows, list) or len(rows) != 1:
            raise DwsError("current user response is not unique")
        model = rows[0].get("orgEmployeeModel") or rows[0]
        return {
            "name": str(model.get("orgUserName") or model.get("name") or ""),
            "user_id": str(model.get("userId") or ""),
        }

    def resolve_exact(self, names: Iterable[str]) -> dict[str, str]:
        resolved: dict[str, str] = {}
        requested = list(dict.fromkeys(str(name).strip() for name in names))
        if not requested or any(not name for name in requested):
            raise DwsError("contact names are empty or missing")
        for wanted in requested:
            payload = self.run(["contact", "user", "search", "--query", wanted])
            if payload.get("hasMore") is True or payload.get("has_more") is True:
                raise DwsError("contact search is incomplete")
            rows = _result(payload) or []
            if isinstance(rows, dict):
                rows = next(
                    (rows[key] for key in ("items", "list", "contacts", "users", "records") if isinstance(rows.get(key), list)),
                    [],
                )
            if not isinstance(rows, list):
                raise DwsError("contact search response has an unsupported shape")
            exact: dict[str, str] = {}
            for row in rows:
                if not isinstance(row, dict):
                    continue
                nested = row.get("orgEmployeeModel")
                nested = nested if isinstance(nested, dict) else {}
                candidate_name = str(
                    row.get("name")
                    or row.get("orgUserName")
                    or row.get("displayName")
                    or nested.get("orgUserName")
                    or nested.get("name")
                    or ""
                ).strip()
                candidate_id = str(row.get("userId") or row.get("user_id") or nested.get("userId") or "").strip()
                if candidate_name == wanted and candidate_id:
                    exact[candidate_id] = candidate_name
            if len(exact) != 1:
                raise DwsError(f"contact is not uniquely resolvable: {wanted}")
            resolved[wanted] = next(iter(exact))
        if len(set(resolved.values())) != len(resolved):
            raise DwsError("contact identities are not unique")
        return resolved

    def create_todo(self, plan: dict[str, Any], priority: int = 20) -> dict[str, Any]:
        if plan.get("decision") != "CREATE":
            raise ValueError("only CREATE plans can be written")
        args = build_create_args(
            title=str(plan["title"]),
            executor_ids=plan["executor_ids"],
            due_iso=plan.get("due_iso"),
            priority=priority,
        )
        payload = self._run_confirmed_official_write(args, append_yes=False)
        result = _result(payload) or {}
        task_id = result.get("taskId") or result.get("todoTaskId")
        if not task_id:
            raise DwsError("create response has no taskId")
        return {"task_id": str(task_id), "raw": payload}

    def create_sub_todo(
        self, parent_task_id: str, title: str, executor_ids: Iterable[str],
        due_iso: str, priority: int = 20,
    ) -> dict[str, Any]:
        """Create one child under a previously persisted parent task ID."""
        args = build_create_sub_args(
            parent_task_id=parent_task_id, title=title, executor_ids=executor_ids,
            due_iso=due_iso, priority=priority,
        )
        payload = self._run_confirmed_official_write(args, append_yes=False)
        result = _result(payload) or {}
        task_id = result.get("taskId") or result.get("todoTaskId")
        if not task_id:
            raise DwsError("create-sub response has no taskId")
        return {"task_id": str(task_id), "raw": payload}

    def update_linked_todo(
        self,
        task_id: str,
        *,
        title: str | None = None,
        due_iso: str | None = None,
        priority: int | None = None,
    ) -> dict[str, Any]:
        """Update explicit fields on an existing taskId and check remote detail again."""
        args = build_update_args(task_id, title=title, due_iso=due_iso, priority=priority)
        payload = self._run_confirmed_official_write(args)
        shortcut_result = _verified_shortcut_result(payload, task_id)
        if not isinstance(shortcut_result.get("todo"), dict):
            raise DwsError("official todo +update result has no verified todo detail")
        detail = self.get_todo(task_id)
        if not isinstance(detail, dict):
            raise DwsError("todo update read-back returned no detail")
        returned_id = detail.get("taskId")
        if returned_id not in (None, "") and returned_id != str(task_id):
            raise DwsError("todo update read-back returned a different taskId")
        verified_fields: list[str] = []
        if title is not None:
            if detail.get("subject") != title:
                raise DwsError("todo update read-back subject mismatch")
            verified_fields.append("title")
        if due_iso is not None:
            expected_due = _epoch_ms(due_iso)
            actual_due = _epoch_ms(detail.get("dueTime") or detail.get("due"))
            if expected_due is None or actual_due != expected_due:
                raise DwsError("todo update read-back dueTime mismatch")
            verified_fields.append("due_iso")
        if priority is not None:
            actual_priority = detail.get("priority")
            if isinstance(actual_priority, bool) or actual_priority != int(priority):
                raise DwsError("todo update read-back priority mismatch")
            verified_fields.append("priority")
        return {"task_id": str(task_id), "verified_fields": verified_fields, "todo": detail, "raw": payload}

    def _set_linked_todo_done(self, task_id: str, target: bool) -> dict[str, Any]:
        args = build_complete_args(task_id) if target else build_reopen_args(task_id)
        payload = self._run_confirmed_official_write(args)
        result = _verified_shortcut_result(payload, task_id)
        if result.get("isDone") is not target or not isinstance(result.get("alreadyInTargetState"), bool):
            raise DwsError("official todo status result is malformed or mismatched")
        detail = self.get_todo(task_id)
        if not isinstance(detail, dict):
            raise DwsError("todo status read-back returned no detail")
        returned_id = detail.get("taskId")
        if returned_id not in (None, "") and returned_id != str(task_id):
            raise DwsError("todo status read-back returned a different taskId")
        if not isinstance(detail.get("isDone"), bool) or detail["isDone"] is not target:
            raise DwsError("todo status read-back mismatch")
        return {"task_id": str(task_id), "is_done": target, "todo": detail, "raw": payload}

    def complete_linked_todo(self, task_id: str) -> dict[str, Any]:
        """Use official +complete, which sets isDone; the source has no role-specific branch."""
        return self._set_linked_todo_done(task_id, True)

    def reopen_linked_todo(self, task_id: str) -> dict[str, Any]:
        """Use official +reopen, which clears isDone; the source has no role-specific branch."""
        return self._set_linked_todo_done(task_id, False)

    def get_todo(self, task_id: str) -> dict[str, Any]:
        payload = self.run(["todo", "task", "get", "--task-id", str(task_id)])
        result = _result(payload) or {}
        return result.get("todoDetailModel", result)

    def search_todos(self, query: str) -> list[dict[str, Any]]:
        """Search all pages through the documented shortcut and reject partial output.

        The shortcut owns pagination. Its successful complete envelope is validated
        here; callers still read every returned task by stable taskId before acting.
        """
        query_text = str(query).strip()
        if not query_text:
            raise DwsError("todo search query is empty")
        payload = self.run(["todo", "+search", "--query", query_text])
        data = _require_official_success(payload)
        if not isinstance(data, dict):
            raise DwsError("todo search returned an unsupported result object")
        for metadata in (payload, data):
            if "complete" in metadata and metadata["complete"] is not True:
                raise DwsError("todo search returned an incomplete result")
            for key in ("partial", "hasMore", "has_more", "truncated"):
                if key in metadata and metadata[key] is not False and metadata[key] is not None and metadata[key] != "":
                    raise DwsError("todo search returned an incomplete result")
            for key in (
                "nextPageToken", "next_page_token", "nextCursor", "next_cursor", "nextcursor",
            ):
                if metadata.get(key) not in (None, ""):
                    raise DwsError("todo search returned a paginated partial result")
            for key in ("stopReason", "stop_reason"):
                if metadata.get(key) not in (None, ""):
                    raise DwsError("todo search stopped before a complete result")
            if metadata.get("failures") not in (None, [], {}):
                raise DwsError("todo search returned failures")
        if data.get("complete") is not True:
            raise DwsError("todo search did not confirm a complete result")
        has_todos = "todos" in data
        has_tasks = "tasks" in data
        if not has_todos and not has_tasks:
            raise DwsError("todo search returned an unsupported task list shape")
        if has_todos and has_tasks and data["todos"] != data["tasks"]:
            raise DwsError("todo search returned conflicting task lists")
        rows = data["todos"] if has_todos else data["tasks"]
        if not isinstance(rows, list):
            raise DwsError("todo search returned an unsupported task list shape")
        count = data.get("count")
        if type(count) is not int or count < 0 or count != len(rows):
            raise DwsError("todo search count does not match its task list")
        unique: dict[str, dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                raise DwsError("todo search returned a malformed task row")
            task_id = row.get("taskId") or row.get("todoTaskId")
            if not isinstance(task_id, str) or not task_id.strip():
                raise DwsError("todo search row has no stable taskId")
            normalized = dict(row)
            normalized["taskId"] = task_id.strip()
            previous = unique.get(task_id.strip())
            if previous is not None and previous != normalized:
                raise DwsError("todo search returned conflicting duplicate taskIds")
            unique[task_id.strip()] = normalized
        if len(unique) > count:
            raise DwsError("todo search unique task count exceeds its declared count")
        return list(unique.values())

    def list_task_comments(self, task_id: str) -> list[dict[str, Any]]:
        """Return a complete comment list or reject an unknown/partial response."""
        payload = self.list_comments(task_id)
        if not isinstance(payload, dict):
            raise DwsError("todo comment list returned a malformed envelope")
        for obj in (payload, payload.get("data"), payload.get("result")):
            if not isinstance(obj, dict):
                continue
            if obj.get("partial") is True or obj.get("complete") is False:
                raise DwsError("todo comment list returned a partial result")
            for key in ("hasMore", "has_more"):
                if obj.get(key) is True:
                    raise DwsError("todo comment list returned a paginated partial result")
            for key in ("nextPageToken", "next_page_token", "nextCursor", "next_cursor"):
                if obj.get(key) not in (None, ""):
                    raise DwsError("todo comment list returned a paginated partial result")
        result = _result(payload)
        if isinstance(result, dict):
            rows = result.get("comments")
            if rows is None:
                rows = result.get("items")
        elif isinstance(result, list):
            rows = result
        else:
            rows = None
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise DwsError("todo comment list returned an unsupported shape")
        return list(rows)

    def add_source_comment(self, task_id: str, content: str) -> dict[str, Any]:
        # The official shortcut owns comment pagination, write, and stable-ID read-back.
        try:
            payload = self._run_confirmed_official_write(build_comment_args(task_id, content))
        except subprocess.TimeoutExpired as exc:
            raise DwsError("todo +comment timed out; remote result is unknown and must not be retried") from exc
        result = _require_official_success(payload)
        if (
            result.get("taskId") != str(task_id)
            or not isinstance(result.get("commentId"), str)
            or not result["commentId"].strip()
            or result.get("content") != content
            or result.get("verified") is not True
        ):
            raise DwsError("official todo +comment result is missing or unverified")
        return payload

    def list_comments(self, task_id: str) -> dict[str, Any]:
        try:
            return self.run(["todo", "comment", "list", "--task-id", str(task_id)])
        except subprocess.TimeoutExpired as exc:
            raise DwsError("source comment read-back timed out; remote result is unknown and must not be retried") from exc

    def verify_source_comment(self, task_id: str, content: str) -> None:
        if not comment_is_present(self.list_comments(task_id), content):
            raise DwsError("source comment was not found on read-back")

    def download_message_resource(
        self,
        *,
        resource_id: str,
        message_id: str,
        conversation_id: str,
        workdir: str,
    ) -> dict[str, Any]:
        from pathlib import Path

        directory = Path(workdir)
        if directory.is_symlink():
            raise DwsError("resource_workspace_symlink_rejected")
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        directory.chmod(0o700)
        if directory.is_symlink() or not directory.is_dir():
            raise DwsError("resource_workspace_invalid")
        payload = self.run_at(
            build_download_resource_args(
                resource_id=resource_id,
                message_id=message_id,
                conversation_id=conversation_id,
                output=".",
            ),
            cwd=str(directory),
        )
        result = _result(payload) or payload
        local_path = result.get("localPath") if isinstance(result, dict) else None
        if not local_path:
            raise DwsError("resource download returned no localPath")
        path = (directory / str(local_path)).resolve()
        if directory.resolve() not in path.parents:
            raise DwsError("resource download path escaped workdir")
        if path.is_symlink() or not path.is_file():
            raise DwsError("resource download file is missing")
        try:
            path.chmod(0o600)
        except OSError as exc:
            raise DwsError("resource download permissions unavailable") from exc
        if path.is_symlink():
            raise DwsError("resource download path is a symlink")
        return {"path": str(path), "file_name": path.name, "raw": payload}

    def add_attachment(self, task_id: str, file_path: str) -> dict[str, Any]:
        payload = self._run_confirmed_official_write(
            build_add_attachment_args(task_id, file_path), append_yes=False,
        )
        result = _result(payload) or payload
        attachment_ids = result.get("attachmentIds") if isinstance(result, dict) else None
        if not attachment_ids:
            raise DwsError("attachment upload returned no attachmentIds")
        return {"attachment_ids": [str(value) for value in attachment_ids], "raw": payload}

    def list_attachments(self, task_id: str) -> dict[str, Any]:
        return self.run(["todo", "task", "list-attachment", "--task-id", str(task_id)])

    def verify_attachment(self, task_id: str, file_name: str) -> None:
        if not attachment_is_present(self.list_attachments(task_id), file_name):
            raise DwsError("attachment was not found on read-back")


def _epoch_ms(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        if isinstance(value, (int, float)) or str(value).strip().isdigit():
            number = int(value)
            return number if number > 10_000_000_000 else number * 1000
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("due time has no timezone")
        return int(parsed.timestamp() * 1000)
    except (TypeError, ValueError, OverflowError):
        return None


def verify_todo(detail: dict[str, Any], plan: dict[str, Any]) -> list[str]:
    problems: list[str] = []
    if detail.get("subject") != plan.get("title"):
        problems.append("subject_mismatch")
    if detail.get("isDone"):
        problems.append("unexpected_completed")
    expected_names = [str(name).strip() for name in (plan.get("executor_names") or []) if str(name).strip()]
    actual_by_name: dict[str, list[str | None]] = {}
    for item in (detail.get("executorInfos") or []):
        if not isinstance(item, dict):
            continue
        nested = item.get("orgEmployeeModel")
        nested = nested if isinstance(nested, dict) else {}
        name = str(item.get("name") or item.get("orgUserName") or nested.get("orgUserName") or nested.get("name") or "").strip()
        user_id = item.get("userId") or item.get("user_id") or nested.get("userId")
        if name:
            actual_by_name.setdefault(name, []).append(str(user_id).strip() if user_id not in (None, "") else None)
    unique_names = len(set(expected_names)) == len(expected_names)
    exact_names = bool(expected_names) and unique_names and all(
        len(actual_by_name.get(name, [])) == 1 for name in expected_names
    )
    if not exact_names:
        problems.append("executor_mismatch")
    expected_ids = [str(value).strip() for value in (plan.get("executor_ids") or [])]
    if exact_names and expected_ids:
        if len(expected_ids) != len(expected_names) or any(not value for value in expected_ids):
            problems.append("executor_mismatch")
        else:
            actual_ids = [actual_by_name[name][0] for name in expected_names]
            if actual_ids != expected_ids:
                remote_present = [value for value in actual_ids if value]
                remote_ids_are_unique = (
                    len(remote_present) == len(actual_ids)
                    and len(set(remote_present)) == len(remote_present)
                )
                if plan.get("allow_executor_namespace_fallback") is True and remote_ids_are_unique:
                    plan["identity_fallback_evidence"] = {
                        "method": "exact_unique_name_after_id_namespace_mismatch",
                        "verified_names": list(expected_names),
                        "remote_ids_present": len(remote_present) == len(actual_ids),
                    }
                else:
                    problems.append("executor_mismatch")
    expected_due = _epoch_ms(plan.get("due_iso"))
    actual_due = _epoch_ms(detail.get("dueTime") or detail.get("due"))
    if expected_due is not None and (actual_due is None or abs(actual_due - expected_due) > 1000):
        problems.append("due_time_mismatch")
    expected_priority = plan.get("priority", 20)
    if detail.get("priority") is None or detail.get("priority") != expected_priority:
        problems.append("priority_mismatch")
    return problems
