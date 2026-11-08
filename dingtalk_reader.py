"""Read-only DingTalk group reader for the local auto-todo runner."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import re
import subprocess
from typing import Any

from dws_runtime import DwsInvocationError, run_dws_json


CHINA_TZ = timezone(timedelta(hours=8))
MGET_BATCH_SIZE = 50
MAX_THREAD_CONTEXTS_PER_GROUP = 20


class DwsReadError(RuntimeError):
    pass


def _aware_china(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=CHINA_TZ)
    return parsed.astimezone(CHINA_TZ)


_RFC3339_CHINA_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?\+08:00\Z"
)


def parse_not_before(value: str | datetime) -> datetime:
    """Parse the supervised collector floor as an explicit RFC3339 +08:00 time."""
    if isinstance(value, datetime):
        parsed = value
        if parsed.tzinfo is None or parsed.utcoffset() != timedelta(hours=8):
            raise ValueError("not_before_must_be_rfc3339_plus_08")
        return parsed.astimezone(CHINA_TZ)
    raw = str(value)
    if not _RFC3339_CHINA_RE.fullmatch(raw):
        raise ValueError("not_before_must_be_rfc3339_plus_08")
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        raise ValueError("not_before_must_be_rfc3339_plus_08") from None
    if parsed.utcoffset() != timedelta(hours=8):
        raise ValueError("not_before_must_be_rfc3339_plus_08")
    return parsed.astimezone(CHINA_TZ)


def start_from_cursor(value: str | None, *, overlap_seconds: int = 60) -> datetime:
    if value:
        return _aware_china(value) - timedelta(seconds=overlap_seconds)
    return datetime.now(CHINA_TZ) - timedelta(hours=24)


def is_complete_ledger(payload: dict[str, Any]) -> bool:
    failures = payload.get("failures") or []
    stop_reason = str(payload.get("stopReason") or "").lower()
    try:
        failed_count = int(payload.get("failedCount") or 0)
    except (TypeError, ValueError):
        return False
    bad_stop = any(token in stop_reason for token in ("error", "fail", "timeout", "truncat", "limit"))
    return (
        payload.get("complete") is True
        and payload.get("hasMore") is False
        and failed_count == 0
        and payload.get("partial") is not True
        and payload.get("truncated") is not True
        and not failures
        and not bad_stop
    )


def _messages_are_chronological(messages: list[dict[str, Any]]) -> bool:
    previous = None
    for message in messages:
        if not isinstance(message, dict):
            return False
        value = message.get("createTime") or message.get("create_time")
        if not value:
            return False
        try:
            moment = _aware_china(value)
        except (TypeError, ValueError):
            return False
        if previous is not None and moment < previous:
            return False
        previous = moment
    return True


def _sender_name(value: Any) -> str | None:
    if isinstance(value, dict):
        return str(value.get("name") or value.get("nick") or value.get("senderName") or "") or None
    return str(value).strip() if value not in (None, "") else None


def normalize_message(message: dict[str, Any], *, group_name: str) -> dict[str, Any]:
    message_id = str(message.get("messageId") or message.get("message_id") or "").strip()
    conversation_id = str(message.get("conversationId") or message.get("conversation_id") or "").strip()
    sender_id = (
        message.get("senderId")
        or message.get("sender_id")
        or message.get("senderOpenDingTalkId")
        or message.get("sender_open_dingtalk_id")
    )
    text = message.get("text")
    if isinstance(text, dict):
        text = text.get("content") or text.get("text") or ""
    return {
        "conversation_id": conversation_id,
        "message_id": message_id,
        "group": group_name,
        "sender_id": str(sender_id).strip() if sender_id not in (None, "") else None,
        "sender": _sender_name(message.get("sender") or message.get("senderName")),
        "create_time": str(message.get("createTime") or message.get("create_time") or ""),
        "text": str(text or ""),
        "at_users": message.get("atUsers") or message.get("at_users") or message.get("mentions") or [],
        "msg_type": message.get("msgType") or message.get("msg_type"),
        "resource_refs": message.get("resourceRefs") or message.get("resource_refs"),
        "quoted_message": message.get("quotedMessage") or message.get("quoted_message"),
        "reply_to_message_id": message.get("replyToMessageId") or message.get("reply_to_message_id"),
        "quoted_message_id": message.get("quotedMessageId") or message.get("quoted_message_id"),
        "related_message_id": message.get("relatedMessageId") or message.get("related_message_id"),
        "thread_id": (
            message.get("openConvThreadId") or message.get("open_conv_thread_id")
            or message.get("threadId") or message.get("thread_id")
        ),
        "thread_root_message_id": (
            message.get("rootMessageId") or message.get("root_message_id")
            or message.get("threadRootMessageId") or message.get("thread_root_message_id")
        ),
        "raw_payload": message,
    }


class DingTalkReader:
    def __init__(self, executable: str = "dws", timeout: int = 120):
        self.executable = executable
        self.timeout = timeout

    def read_group(
        self,
        *,
        conversation_id: str,
        group_name: str,
        last_msg_time: str | None,
        end: datetime | None = None,
        overlap_seconds: int = 60,
        page_limit: int = 50,
        not_before: datetime | None = None,
    ) -> dict[str, Any]:
        start = start_from_cursor(last_msg_time, overlap_seconds=overlap_seconds)
        end = end or datetime.now(CHINA_TZ)
        floor = parse_not_before(not_before) if not_before is not None else None
        if floor is not None:
            start = max(start, floor)
        end = _aware_china(end)
        if floor is not None and floor > end:
            return {
                "complete": True,
                "source_order_provenance": "complete_dws_chronological_page",
                "ledger": {"complete": True, "hasMore": False, "messages": []},
                "group_name": group_name,
                "conversation_id": conversation_id,
                "messages": [],
                "start": start.isoformat(),
                "end": end.isoformat(),
            }
        command = [
            self.executable,
            "chat",
            "+chat-messages",
            "--group",
            conversation_id,
            "--start",
            start.isoformat(),
            "--end",
            end.isoformat(),
            "--order",
            "asc",
            "--page-all",
            "--page-limit",
            str(page_limit),
            "--no-reactions",
            "--format",
            "json",
        ]
        try:
            payload = run_dws_json(
                command[1:], executable=command[0], timeout=self.timeout,
            )
        except DwsInvocationError as exc:
            reason = "dws_non_json_response" if exc.reason == "dws_non_json_response" else "dws_read_failed"
            raise DwsReadError(reason) from None
        if not isinstance(payload, dict):
            raise DwsReadError("dws_read_failed")
        if not is_complete_ledger(payload):
            return {
                "complete": False,
                "ledger": payload,
                "group_name": group_name,
                "conversation_id": conversation_id,
                "messages": [],
            }
        messages = payload.get("messages") or []
        if not isinstance(messages, list):
            raise DwsReadError("dws_message_page_invalid")
        if not messages:
            order_provenance = "complete_dws_chronological_page"
            return {
                "complete": True,
                "source_order_provenance": order_provenance,
                "ledger": payload,
                "group_name": group_name,
                "conversation_id": conversation_id,
                "messages": [],
                "context_status": "complete",
                "start": start.isoformat(),
                "end": end.isoformat(),
            }
        try:
            normalized_messages, context_status = self._enrich_messages(
                messages,
                conversation_id=conversation_id,
                group_name=group_name,
            )
        except DwsReadError:
            return {
                "complete": False,
                "ledger": payload,
                "group_name": group_name,
                "conversation_id": conversation_id,
                "messages": [],
                "context_status": "partial",
            }
        order_provenance = (
            "complete_dws_chronological_page"
            if isinstance(messages, list) and _messages_are_chronological(messages)
            else "unverified_input_order"
        )
        return {
            "complete": True,
            "source_order_provenance": order_provenance,
            "ledger": payload,
            "group_name": group_name,
            "conversation_id": conversation_id,
            "messages": normalized_messages,
            "context_status": context_status,
            "start": start.isoformat(),
            "end": end.isoformat(),
        }

    def _run_message_query(self, command: list[str]) -> dict[str, Any]:
        try:
            payload = run_dws_json(
                command[1:], executable=command[0], timeout=self.timeout,
            )
        except DwsInvocationError as exc:
            reason = "dws_non_json_response" if exc.reason == "dws_non_json_response" else "dws_read_failed"
            raise DwsReadError(reason) from None
        if not isinstance(payload, dict):
            raise DwsReadError("dws_read_failed")
        if "ok" in payload:
            if payload.get("ok") is not True or payload.get("outcome") != "success":
                raise DwsReadError("dws_read_failed")
            data = payload.get("data")
            if not isinstance(data, dict):
                raise DwsReadError("dws_message_result_invalid")
            return data
        return payload

    def _mget(self, message_ids: list[str], *, conversation_id: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for offset in range(0, len(message_ids), MGET_BATCH_SIZE):
            batch = message_ids[offset:offset + MGET_BATCH_SIZE]
            command = [
                self.executable, "chat", "+messages-mget", "--msg-ids", ",".join(batch),
                "--no-reactions", "--no-threads", "--format", "json",
            ]
            payload = self._run_message_query(command)
            batch_rows = payload.get("messages")
            if not is_complete_ledger(payload) or not isinstance(batch_rows, list):
                raise DwsReadError("dws_message_mget_incomplete")
            if len(batch_rows) != len(batch):
                raise DwsReadError("dws_message_mget_missing_rows")
            returned: dict[str, dict[str, Any]] = {}
            for row in batch_rows:
                if not isinstance(row, dict):
                    raise DwsReadError("dws_message_mget_invalid_row")
                normalized = normalize_message(row, group_name="")
                message_id = normalized["message_id"]
                if not message_id or message_id in returned:
                    raise DwsReadError("dws_message_mget_identity_invalid")
                if normalized["conversation_id"] != conversation_id:
                    raise DwsReadError("dws_message_mget_scope_mismatch")
                returned[message_id] = row
            if set(returned) != set(batch):
                raise DwsReadError("dws_message_mget_identity_mismatch")
            rows.extend(returned[message_id] for message_id in batch)
        return rows

    @staticmethod
    def _reference_ids(message: dict[str, Any]) -> list[str]:
        values: list[str] = []
        for key in ("replyToMessageId", "reply_to_message_id", "quotedMessageId", "quoted_message_id",
                    "relatedMessageId", "related_message_id"):
            value = message.get(key)
            if isinstance(value, str) and value.strip():
                values.append(value.strip())
        quoted = message.get("quotedMessage") or message.get("quoted_message")
        if isinstance(quoted, dict):
            for key in ("messageId", "message_id", "openMessageId", "open_message_id"):
                value = quoted.get(key)
                if isinstance(value, str) and value.strip():
                    values.append(value.strip())
        return list(dict.fromkeys(values))

    def _read_thread_replies(
        self,
        *,
        conversation_id: str,
        thread_id: str,
    ) -> tuple[list[dict[str, Any]], bool]:
        command = [
            self.executable, "chat", "+thread-replies", "--group", conversation_id,
            "--thread-id", thread_id, "--no-reactions", "--format", "json",
        ]
        payload = self._run_message_query(command)
        messages = payload.get("messages")
        if not isinstance(messages, list):
            raise DwsReadError("dws_thread_replies_partial")
        return messages, is_complete_ledger(payload)

    def _enrich_messages(
        self,
        page_rows: list[dict[str, Any]],
        *,
        conversation_id: str,
        group_name: str,
    ) -> tuple[list[dict[str, Any]], str]:
        source_ids: list[str] = []
        for row in page_rows:
            if not isinstance(row, dict):
                raise DwsReadError("dws_message_page_invalid_row")
            normalized = normalize_message(row, group_name=group_name)
            if normalized["conversation_id"] != conversation_id or not normalized["message_id"]:
                raise DwsReadError("dws_message_page_identity_invalid")
            source_ids.append(normalized["message_id"])
        if len(source_ids) != len(set(source_ids)):
            raise DwsReadError("dws_message_page_identity_duplicate")

        # Re-read each source in strict batches. This carries attachment refs
        # and full message bodies while preserving the page's source ID/CID.
        full_rows = self._mget(source_ids, conversation_id=conversation_id)
        by_id = {
            normalize_message(row, group_name=group_name)["message_id"]: row
            for row in full_rows
        }
        referenced = list(dict.fromkeys(
            ref for row in full_rows for ref in self._reference_ids(row)
            if ref not in by_id
        ))
        related_rows: list[dict[str, Any]] = []
        if referenced:
            try:
                related_rows = self._mget(referenced, conversation_id=conversation_id)
            except DwsReadError:
                # A missing or inaccessible related message belongs to its
                # referencing source. Preserve that source for a REVIEW plan
                # instead of discarding every other message in this group page.
                for ref in referenced:
                    try:
                        related_rows.extend(self._mget([ref], conversation_id=conversation_id))
                    except DwsReadError:
                        continue
        related_by_id = {
            normalize_message(row, group_name=group_name)["message_id"]: row
            for row in related_rows
        }
        all_rows = {**related_by_id, **by_id}

        thread_ids = list(dict.fromkeys(
            str(normalize_message(row, group_name=group_name)["thread_id"]).strip()
            for row in full_rows
            if normalize_message(row, group_name=group_name).get("thread_id")
        ))
        failed_threads: set[str] = set()
        if len(thread_ids) > MAX_THREAD_CONTEXTS_PER_GROUP:
            failed_threads.update(thread_ids[MAX_THREAD_CONTEXTS_PER_GROUP:])
            thread_ids = thread_ids[:MAX_THREAD_CONTEXTS_PER_GROUP]
        thread_rows: list[dict[str, Any]] = []
        for thread_id in thread_ids:
            try:
                replies, thread_complete = self._read_thread_replies(
                    conversation_id=conversation_id,
                    thread_id=thread_id,
                )
            except DwsReadError:
                # Topic replies are supplemental. A failed thread read is
                # attached to its own root source and cannot stop the group.
                failed_threads.add(thread_id)
                continue
            if not thread_complete:
                failed_threads.add(thread_id)
            for row in replies:
                if not isinstance(row, dict):
                    raise DwsReadError("dws_thread_reply_invalid")
                normalized = normalize_message(row, group_name=group_name)
                if not normalized["message_id"]:
                    raise DwsReadError("dws_thread_reply_identity_invalid")
                # Thread replies are scoped by the explicit parent group and
                # thread query. Keep their own message ID and the parent CID.
                row = dict(row)
                row["conversationId"] = conversation_id
                row["threadId"] = thread_id
                if not thread_complete:
                    row["_thread_context_status"] = "partial"
                thread_rows.append(row)

        result: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for source_id in source_ids:
            source_row = by_id[source_id]
            event = normalize_message(source_row, group_name=group_name)
            refs = self._reference_ids(source_row)
            contexts: list[dict[str, Any]] = []
            missing: list[str] = []
            for ref in refs:
                context_row = all_rows.get(ref)
                if context_row is None:
                    missing.append(ref)
                    continue
                context = normalize_message(context_row, group_name=group_name)
                if context["conversation_id"] != conversation_id or context["message_id"] != ref:
                    missing.append(ref)
                    continue
                contexts.append({
                    "conversation_id": conversation_id,
                    "message_id": ref,
                    "group": group_name,
                    "text": context["text"],
                    "resource_refs": context["resource_refs"],
                    "create_time": context["create_time"],
                    "sender_id": context["sender_id"],
                    "sender": context["sender"],
                })
            event["related_contexts"] = contexts
            event["context_missing_message_ids"] = missing
            event["context_status"] = "partial" if missing else "complete"
            source_thread_id = str(event.get("thread_id") or "").strip()
            if source_thread_id in failed_threads:
                event["thread_context_status"] = "partial"
                event["thread_context_thread_id"] = source_thread_id
            else:
                event["thread_context_status"] = "complete"
            source_key = (conversation_id, source_id)
            if source_key not in seen:
                result.append(event)
                seen.add(source_key)

        for row in thread_rows:
            event = normalize_message(row, group_name=group_name)
            refs = self._reference_ids(row)
            event["related_contexts"] = [
                {
                    "conversation_id": conversation_id,
                    "message_id": ref,
                    "group": group_name,
                    "text": normalize_message(all_rows[ref], group_name=group_name)["text"],
                    "resource_refs": normalize_message(all_rows[ref], group_name=group_name)["resource_refs"],
                    "create_time": normalize_message(all_rows[ref], group_name=group_name)["create_time"],
                    "sender_id": normalize_message(all_rows[ref], group_name=group_name)["sender_id"],
                    "sender": normalize_message(all_rows[ref], group_name=group_name)["sender"],
                }
                for ref in refs if ref in all_rows
            ]
            event["context_missing_message_ids"] = [ref for ref in refs if ref not in all_rows]
            event["context_status"] = "partial" if event["context_missing_message_ids"] else "complete"
            event["thread_context_status"] = str(row.get("_thread_context_status") or "complete")
            source_key = (conversation_id, event["message_id"])
            if source_key not in seen:
                result.append(event)
                seen.add(source_key)
        result.sort(key=lambda row: _aware_china(row["create_time"]) if row["create_time"] else datetime.min.replace(tzinfo=CHINA_TZ))
        overall = "partial" if any(
            row["context_status"] == "partial" or row.get("thread_context_status") == "partial"
            for row in result
        ) else "complete"
        return result, overall
