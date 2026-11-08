"""Offline adapter seam for Cifra review cards.

The published template's field names and the callback's user-ID mapping are
deployment-owned. Callers must supply both explicitly; this module never
guesses either contract.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from review_inbox import parse_reply


CARD_CALLBACK_TOPIC = "/v1.0/card/instances/callback"
CARD_CALLBACK_TYPE = "actionCallback"
CARD_MAX_CONTENT_CHARS = 65536
CARD_MAX_ACTIONS = 5
CARD_MAX_TEMPLATE_FIELD_BYTES = 40 * 1024
CARD_SUBMIT_ACTION_ID = "dtd-review-submit"
CARD_FORM_FIELDS_PARAM = "submit_form_fields"
CARD_FORM_FIELDS_TEMPLATE_KEY = "form_fields"
CARD_TEMPLATE_STATIC_FIELDS = {
    "title": "待办复核",
    "form_status": "normal",
    "button_text": "提交复核",
}
CARD_TEMPLATE_FIELD_MAPPING = {
    "title": "title",
    "form_status": "form_status",
    "button_text": "button_text",
    CARD_FORM_FIELDS_TEMPLATE_KEY: CARD_FORM_FIELDS_TEMPLATE_KEY,
}
CARD_DECISION_OPTIONS = [
    {"value": 1, "text": {"zh_CN": "创建待办"}},
    {"value": 2, "text": {"zh_CN": "忽略"}},
]
_BATCH_ID = re.compile(r"[A-Za-z0-9_-]{8,64}\Z")
_FORM_FIELD_NAME = re.compile(r"dtd_review_([1-5])\Z")
_MESSAGE_TIME = re.compile(
    r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{3})?\Z"
)
_FORM_FIELD_STATIC_KEYS = {
    "type", "required", "name", "label", "placeholder", "options",
}


@dataclass(frozen=True)
class CardActionCallback:
    """Small normalized callback envelope; no raw card body is retained."""

    out_track_id: str
    user_id: str
    user_id_type: int | str
    action_ids: tuple[str, ...]
    decisions: tuple[tuple[int, str], ...]
    callback_id: str
    message_time: str


@dataclass(frozen=True)
class CardReviewInput:
    """A verified structured card decision; never represented as a chat message."""

    batch_id: str
    conversation_id: str
    owner_id: str
    content: str
    decisions: dict[int, str]


def build_create_and_deliver_body(
    *,
    batch_id: str,
    template_id: str,
    template_fields: dict[str, Any],
    open_space_id: str,
    robot_code: str,
) -> dict[str, Any]:
    """Build a bot-authenticated createAndDeliver request without guessing fields.

    ``template_fields`` must match the separately published template. The
    adapter only places them under the documented ``cardParamMap`` envelope.
    ``outTrackId`` is exactly the unique SQL batch ID.
    """
    _validate_batch_id(batch_id)
    _validate_required_string(template_id, "card_template_id_invalid", 256)
    _validate_required_string(open_space_id, "card_open_space_id_invalid", 512)
    _validate_required_string(robot_code, "card_robot_code_invalid", 512)
    space_prefix = "dtv1.card//IM_ROBOT."
    if not open_space_id.startswith(space_prefix) or not open_space_id[len(space_prefix):]:
        raise ValueError("card_open_space_id_invalid")
    if not isinstance(template_fields, dict):
        raise ValueError("card_template_fields_invalid")
    if (set(template_fields) != set(CARD_TEMPLATE_FIELD_MAPPING)
            or not isinstance(template_fields.get(CARD_FORM_FIELDS_TEMPLATE_KEY), str)):
        raise ValueError("card_template_fields_invalid")
    if any(not isinstance(value, str) for value in template_fields.values()):
        raise ValueError("card_template_fields_invalid")
    if any(template_fields.get(key) != value
           for key, value in CARD_TEMPLATE_STATIC_FIELDS.items()):
        raise ValueError("card_template_fields_invalid")
    if (len(template_fields[CARD_FORM_FIELDS_TEMPLATE_KEY].encode("utf-8"))
            > CARD_MAX_TEMPLATE_FIELD_BYTES):
        raise ValueError("card_template_fields_too_large")
    try:
        form_fields = json.loads(template_fields[CARD_FORM_FIELDS_TEMPLATE_KEY])
    except (json.JSONDecodeError, TypeError):
        raise ValueError("card_template_fields_invalid") from None
    if _validate_form_fields(form_fields, require_selection=False) is None:
        raise ValueError("card_template_fields_invalid")
    try:
        serialized_fields = json.dumps(
            template_fields, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        )
    except (TypeError, ValueError):
        raise ValueError("card_template_fields_invalid") from None
    if len(serialized_fields) > CARD_MAX_CONTENT_CHARS:
        raise ValueError("card_template_fields_too_large")

    return {
        "cardTemplateId": template_id,
        "outTrackId": batch_id,
        "cardData": {"cardParamMap": dict(template_fields)},
        "callbackType": "STREAM",
        "openSpaceId": open_space_id,
        "imRobotOpenDeliverModel": {
            "spaceType": "IM_ROBOT",
            "robotCode": robot_code,
        },
    }


def build_review_template_fields(
    *,
    batch_id: str,
    review_text: str,
    items: list[dict[str, Any]],
    field_mapping: dict[str, str],
) -> dict[str, str]:
    """Build the classic template's single serialized ``form_fields`` value."""
    _validate_batch_id(batch_id)
    prefix = f"复核编号：{batch_id}\n\n"
    if (not isinstance(review_text, str) or not review_text.startswith(prefix)
            or not review_text[len(prefix):].strip()):
        raise ValueError("card_review_text_invalid")
    if (not isinstance(items, list) or not 1 <= len(items) <= CARD_MAX_ACTIONS
            or not isinstance(field_mapping, dict)
            or field_mapping != CARD_TEMPLATE_FIELD_MAPPING):
        raise ValueError("card_template_mapping_invalid")

    form_fields: list[dict[str, Any]] = []
    for ordinal, item in enumerate(items, 1):
        if (not isinstance(item, dict) or set(item) != {"action_id", "source_date", "source"}
                or not isinstance(item["action_id"], str)
                or not re.fullmatch(r"[1-9][0-9]{0,18}", item["action_id"])
                or not isinstance(item["source_date"], str)
                or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", item["source_date"])
                or not isinstance(item["source"], dict)
                or not isinstance(item["source"].get("text"), str)
                or not item["source"]["text"].strip()
                or item["source"].get("text_truncated") is not False
                or len(item["source"]["text"]) > 1600):
            raise ValueError("card_template_source_invalid")
        source = item["source"]
        group_name = source.get("group_name")
        message_time = source.get("message_time")
        if ((group_name is not None and (not isinstance(group_name, str)
                or len(group_name.encode("utf-8")) > 2048))
                or (message_time is not None and (not isinstance(message_time, str)
                                                  or len(message_time.encode("utf-8")) > 128))):
            raise ValueError("card_template_source_invalid")
        label_parts = [f"复核第 {ordinal} 项", item["source_date"]]
        if group_name:
            label_parts.append(f"来源群：{group_name}")
        if message_time:
            label_parts.append(f"消息时间：{message_time}")
        label_parts.append(f"原文：{source['text']}")
        form_fields.append({
            "type": "SELECT",
            "required": True,
            "name": f"dtd_review_{ordinal}",
            "label": "\n".join(label_parts),
            "placeholder": "请选择创建待办或忽略",
            "options": CARD_DECISION_OPTIONS,
        })

    if _validate_form_fields(form_fields, require_selection=False) is None:
        raise ValueError("card_template_source_invalid")

    serialized_fields = json.dumps(
        form_fields, ensure_ascii=False, allow_nan=False, separators=(",", ":"),
    )
    if len(serialized_fields.encode("utf-8")) > CARD_MAX_TEMPLATE_FIELD_BYTES:
        raise ValueError("card_template_value_too_large")
    return {
        **CARD_TEMPLATE_STATIC_FIELDS,
        CARD_FORM_FIELDS_TEMPLATE_KEY: serialized_fields,
    }


def _validate_form_fields(
    value: Any, *, require_selection: bool,
) -> tuple[tuple[int, str], ...] | None:
    if not isinstance(value, list) or not 1 <= len(value) <= CARD_MAX_ACTIONS:
        return None

    expected_keys = _FORM_FIELD_STATIC_KEYS | ({"default_number"} if require_selection else set())
    decisions: dict[int, str] = {}
    for field in value:
        if not isinstance(field, dict) or set(field) != expected_keys:
            return None
        name = field.get("name")
        match = _FORM_FIELD_NAME.fullmatch(name) if isinstance(name, str) else None
        if (field.get("type") != "SELECT" or field.get("required") is not True
                or not match or field.get("placeholder") != "请选择创建待办或忽略"
                or not isinstance(field.get("label"), str) or not field["label"].strip()
                or len(field["label"].encode("utf-8")) > CARD_MAX_TEMPLATE_FIELD_BYTES
                or not _is_decision_options(field.get("options"))):
            return None
        ordinal = int(match.group(1))
        if ordinal in decisions:
            return None
        if require_selection:
            selected_value = field.get("default_number")
            if isinstance(selected_value, bool) or not isinstance(selected_value, int):
                return None
            if selected_value not in (1, 2):
                return None
            decisions[ordinal] = "create" if selected_value == 1 else "ignore"

    ordinals = [int(_FORM_FIELD_NAME.fullmatch(field["name"]).group(1)) for field in value]
    if sorted(ordinals) != list(range(1, len(value) + 1)):
        return None
    return tuple(sorted(decisions.items())) if require_selection else ()


def _is_decision_options(value: Any) -> bool:
    if not isinstance(value, list) or len(value) != 2:
        return False
    expected = ((1, "创建待办"), (2, "忽略"))
    for option, (expected_value, expected_label) in zip(value, expected, strict=True):
        if (not isinstance(option, dict) or set(option) != {"value", "text"}
                or type(option.get("value")) is not int
                or option["value"] != expected_value
                or not isinstance(option.get("text"), dict)
                or option["text"] != {"zh_CN": expected_label}):
            return False
    return True


def _parse_submit_form_fields(value: Any) -> tuple[tuple[int, str], ...] | None:
    return _validate_form_fields(value, require_selection=True)


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_card_callback_field")
        result[key] = value
    return result


def parse_stream_card_callback(payload: Any, *, message_time: Any) -> CardActionCallback | None:
    """Parse the documented STREAM action callback shape and fail closed."""
    if isinstance(payload, str):
        if not payload or len(payload) > CARD_MAX_CONTENT_CHARS:
            return None
        try:
            payload = json.loads(payload, object_pairs_hook=_unique_json_object)
        except (json.JSONDecodeError, TypeError, ValueError):
            return None
    if (not isinstance(payload, dict) or payload.get("type") != CARD_CALLBACK_TYPE
            or not _valid_message_time(message_time)):
        return None

    out_track_id = payload.get("outTrackId")
    user_id = payload.get("userId")
    user_id_type = payload.get("userIdType")
    content = payload.get("content")
    if (not isinstance(out_track_id, str) or not _BATCH_ID.fullmatch(out_track_id)
            or not isinstance(user_id, str) or not user_id.strip() or len(user_id) > 512
            or isinstance(user_id_type, bool)
            or not isinstance(user_id_type, (int, str))
            or (isinstance(user_id_type, str) and not user_id_type.strip())
            or not isinstance(content, str) or not content
            or len(content) > CARD_MAX_CONTENT_CHARS):
        return None
    try:
        decoded = json.loads(content, object_pairs_hook=_unique_json_object)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    if not isinstance(decoded, dict):
        return None
    private_data = decoded.get("cardPrivateData")
    if not isinstance(private_data, dict) or set(private_data) != {"actionIds", "params"}:
        return None
    action_ids = private_data.get("actionIds")
    params = private_data.get("params")
    if (action_ids != [CARD_SUBMIT_ACTION_ID] or not isinstance(params, dict)
            or set(params) != {CARD_FORM_FIELDS_PARAM}):
        return None
    decisions = _parse_submit_form_fields(params.get(CARD_FORM_FIELDS_PARAM))
    if decisions is None:
        return None

    event_id = payload.get("callbackId") or payload.get("eventId")
    if event_id is not None and (
            not isinstance(event_id, str) or not event_id.strip() or len(event_id) > 256):
        return None
    if event_id is None:
        fingerprint_input = json.dumps(
            [out_track_id, user_id_type, user_id, action_ids, decisions],
            ensure_ascii=False, separators=(",", ":"),
        )
        event_id = "card_" + hashlib.sha256(fingerprint_input.encode("utf-8")).hexdigest()

    return CardActionCallback(
        out_track_id=out_track_id,
        user_id=user_id,
        user_id_type=user_id_type,
        action_ids=tuple(action_ids),
        decisions=decisions,
        callback_id=event_id,
        message_time=message_time,
    )


def prepare_card_review_input(
    conn: Any,
    callback: CardActionCallback,
    *,
    message_time: str | None = None,
    identity_resolver: Callable[[str, int | str], str | None],
) -> CardReviewInput:
    """Verify one open batch, its owner, and every ordinal decision.

    ``message_time`` must come from a verified transport timestamp. Identity
    resolution is injected because ``userIdType`` values cannot be assumed to
    match the SQL ``owner_id`` representation.
    """
    if not isinstance(callback, CardActionCallback):
        raise ValueError("card_callback_invalid")
    if not callable(identity_resolver):
        raise ValueError("card_identity_resolver_required")
    _validate_batch_id(callback.out_track_id)
    if (not isinstance(callback.user_id, str) or not callback.user_id.strip()
            or len(callback.user_id) > 512 or isinstance(callback.user_id_type, bool)
            or not isinstance(callback.user_id_type, (int, str))
            or (isinstance(callback.user_id_type, str) and not callback.user_id_type.strip())
            or not isinstance(callback.callback_id, str) or not callback.callback_id.strip()
            or len(callback.callback_id) > 256
            or not isinstance(callback.action_ids, tuple)
            or callback.action_ids != (CARD_SUBMIT_ACTION_ID,)
            or not isinstance(callback.decisions, tuple)
            or not 1 <= len(callback.decisions) <= CARD_MAX_ACTIONS):
        raise ValueError("card_callback_invalid")
    if message_time is None:
        message_time = callback.message_time
    elif message_time != callback.message_time:
        raise ValueError("card_callback_time_mismatch")
    _validate_message_time(message_time)

    open_batches = conn.execute(
        "SELECT id,conversation_id,owner_id,prompt_time FROM batch WHERE state='open' LIMIT 2"
    ).fetchall()
    if len(open_batches) != 1 or open_batches[0][0] != callback.out_track_id:
        raise ValueError("card_batch_not_unique_open")
    batch_id, conversation_id, owner_id, prompt_time = open_batches[0]
    _validate_message_time(prompt_time)
    if message_time <= prompt_time:
        raise ValueError("card_callback_not_after_prompt")
    if conn.execute(
            "SELECT 1 FROM review_clarification WHERE batch_id=?", (batch_id,)
    ).fetchone():
        raise ValueError("card_clarification_not_supported")

    try:
        resolved_owner = identity_resolver(callback.user_id, callback.user_id_type)
    except Exception:
        raise ValueError("card_identity_unresolved") from None
    if not isinstance(resolved_owner, str) or resolved_owner != owner_id:
        raise ValueError("card_identity_mismatch")

    ordinals = [row[0] for row in conn.execute(
        "SELECT ordinal FROM item WHERE batch_id=? ORDER BY ordinal", (batch_id,)
    ).fetchall()]
    if not ordinals or ordinals != list(range(1, len(ordinals) + 1)):
        raise ValueError("card_batch_items_invalid")
    if len(ordinals) > CARD_MAX_ACTIONS:
        raise ValueError("card_action_set_incomplete")

    decisions = dict(callback.decisions)
    if (len(decisions) != len(callback.decisions)
            or sorted(decisions) != ordinals
            or any(decision not in ("create", "ignore") for decision in decisions.values())):
        raise ValueError("card_action_set_incomplete")

    reply_text = "；".join(
        f"{ordinal}，{'创建' if decisions[ordinal] == 'create' else '忽略'}"
        for ordinal in ordinals
    )
    try:
        parsed = parse_reply(reply_text, len(ordinals))
    except ValueError:
        raise ValueError("card_decision_invalid") from None
    if parsed != decisions:
        raise ValueError("card_decision_invalid")

    return CardReviewInput(
        batch_id=batch_id,
        conversation_id=conversation_id,
        owner_id=owner_id,
        content=reply_text,
        decisions=decisions,
    )


def process_card_callback(
    conn: Any,
    callback: CardActionCallback,
    *,
    template_id: str,
    message_time: str | None = None,
    identity_resolver: Callable[[str, int | str], str | None],
) -> dict[str, str]:
    """Persist a verified card decision with distinct source evidence atomically."""
    if not isinstance(callback, CardActionCallback):
        raise ValueError("card_callback_invalid")
    _validate_required_string(template_id, "card_template_id_invalid", 256)
    prior_batch = conn.execute(
        "SELECT state FROM batch WHERE id=?", (callback.out_track_id,)
    ).fetchone()
    if not prior_batch:
        raise ValueError("card_batch_unknown")
    if prior_batch[0] == "answered":
        prior_event = conn.execute(
            "SELECT callback_id FROM review_card_evidence WHERE batch_id=?",
            (callback.out_track_id,),
        ).fetchone()
        if prior_event and prior_event[0] == callback.callback_id:
            return {"status": "duplicate", "batch_id": callback.out_track_id}
        return {"status": "already_answered", "batch_id": callback.out_track_id}
    callback_time = callback.message_time if message_time is None else message_time
    conn.execute("BEGIN IMMEDIATE")
    try:
        prepared = prepare_card_review_input(
            conn,
            callback,
            message_time=callback_time,
            identity_resolver=identity_resolver,
        )
        message_id = _card_reply_id(callback.callback_id)
        digest = hashlib.sha256(prepared.content.encode("utf-8")).hexdigest()
        encoded_decisions = json.dumps(
            {str(key): value for key, value in prepared.decisions.items()},
            ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        )
        encoded_actions = json.dumps(
            list(callback.action_ids), ensure_ascii=False, separators=(",", ":"),
        )
        batch = conn.execute(
            "SELECT conversation_id,owner_id,prompt_time,state FROM batch WHERE id=?",
            (prepared.batch_id,),
        ).fetchone()
        open_batches = conn.execute(
            "SELECT id FROM batch WHERE state='open' LIMIT 2"
        ).fetchall()
        if not batch:
            raise ValueError("card_batch_unknown")
        if batch[3] == "answered":
            prior = conn.execute(
                "SELECT callback_id FROM review_card_evidence WHERE batch_id=?",
                (prepared.batch_id,),
            ).fetchone()
            if prior and prior[0] == callback.callback_id:
                conn.rollback()
                return {"status": "duplicate", "batch_id": prepared.batch_id}
            conn.rollback()
            return {"status": "already_answered", "batch_id": prepared.batch_id}
        if (len(open_batches) != 1 or open_batches[0][0] != prepared.batch_id
                or batch[0] != prepared.conversation_id or batch[1] != prepared.owner_id
                or callback_time <= batch[2]):
            raise ValueError("card_batch_state_changed")
        if conn.execute(
                "SELECT 1 FROM review_clarification WHERE batch_id=?",
                (prepared.batch_id,),
        ).fetchone():
            raise ValueError("card_clarification_not_supported")
        resolved_owner = identity_resolver(callback.user_id, callback.user_id_type)
        if not isinstance(resolved_owner, str) or resolved_owner != batch[1]:
            raise ValueError("card_identity_mismatch")
        prior_event = conn.execute(
            "SELECT batch_id FROM review_card_evidence WHERE callback_id=?",
            (callback.callback_id,),
        ).fetchone()
        if prior_event:
            if prior_event[0] == prepared.batch_id:
                conn.rollback()
                return {"status": "duplicate", "batch_id": prepared.batch_id}
            raise ValueError("card_callback_identity_conflict")

        conn.execute(
            "INSERT INTO reply "
            "(message_id,batch_id,message_time,content_sha256,content,decisions,source) "
            "VALUES (?,?,?,?,?,?,?)",
            (message_id, prepared.batch_id, callback_time, digest, prepared.content,
             encoded_decisions, "card"),
        )
        conn.execute(
            "INSERT INTO review_card_evidence "
            "(batch_id,callback_id,template_id,owner_id,message_time,content_sha256,action_ids,user_id_type,work_state) "
            "VALUES (?,?,?,?,?,?,?,?, 'pending')",
            (prepared.batch_id, callback.callback_id, template_id, batch[1], callback_time,
             digest, encoded_actions, "int" if isinstance(callback.user_id_type, int) else "str"),
        )
        conn.execute("UPDATE batch SET state='answered' WHERE id=? AND state='open'",
                     (prepared.batch_id,))
        if conn.execute("SELECT changes()").fetchone()[0] != 1:
            raise ValueError("card_batch_state_changed")
        conn.commit()
        return {"status": "recorded", "batch_id": prepared.batch_id}
    except BaseException:
        conn.rollback()
        raise


def recover_interrupted_card_work(conn: Any) -> list[str]:
    """Requeue only durable work interrupted by a prior process lifetime."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "UPDATE review_card_evidence SET work_state='pending' "
            "WHERE work_state='processing'"
        )
        rows = conn.execute(
            "SELECT callback_id FROM review_card_evidence "
            "WHERE work_state='pending' ORDER BY message_time,callback_id"
        ).fetchall()
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return [row[0] for row in rows if isinstance(row[0], str)]


def claim_card_callback_work(conn: Any, callback_id: str) -> str | None:
    """Claim one persisted callback before dispatching it to the review worker."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT batch_id FROM review_card_evidence "
            "WHERE callback_id=? AND work_state='pending'",
            (callback_id,),
        ).fetchone()
        if not row:
            conn.rollback()
            return None
        changed = conn.execute(
            "UPDATE review_card_evidence SET work_state='processing' "
            "WHERE callback_id=? AND work_state='pending'",
            (callback_id,),
        ).rowcount
        if changed != 1:
            conn.rollback()
            return None
        conn.commit()
        return row[0]
    except BaseException:
        conn.rollback()
        raise


def finish_card_callback_work(conn: Any, callback_id: str, *, unknown: bool = False) -> None:
    """Record that dispatch returned, or require reconciliation after an exception."""
    state = "unknown" if unknown else "dispatched"
    changed = conn.execute(
        "UPDATE review_card_evidence SET work_state=? "
        "WHERE callback_id=? AND work_state='processing'",
        (state, callback_id),
    ).rowcount
    if changed != 1:
        raise ValueError("card_callback_work_state_invalid")


def verify_card_reply_evidence(conn: Any, batch_id: str) -> dict[str, Any]:
    """Validate durable card source evidence without consulting or fabricating chat history."""
    batch = conn.execute(
        "SELECT conversation_id,owner_id,prompt_time,state FROM batch WHERE id=?", (batch_id,)
    ).fetchone()
    reply = conn.execute(
        "SELECT message_id,message_time,content_sha256,content,decisions,source "
        "FROM reply WHERE batch_id=?", (batch_id,),
    ).fetchall()
    evidence = conn.execute(
        "SELECT callback_id,template_id,owner_id,message_time,content_sha256,action_ids,user_id_type "
        "FROM review_card_evidence WHERE batch_id=?", (batch_id,),
    ).fetchone()
    if not batch or batch[3] != "answered" or len(reply) != 1 or not evidence:
        raise ValueError("card_reply_evidence_missing")
    conversation_id, owner_id, prompt_time, _state = batch
    message_id, message_time, digest, content, saved, source = reply[0]
    callback_id, template_id, evidence_owner, evidence_time, evidence_digest, encoded_actions, user_id_type = evidence
    if (source != "card" or not isinstance(callback_id, str) or not callback_id
            or not isinstance(template_id, str) or not template_id.strip()
            or evidence_owner != owner_id or evidence_time != message_time
            or evidence_digest != digest or message_id != _card_reply_id(callback_id)
            or not isinstance(message_time, str) or message_time <= prompt_time
            or not _valid_message_time(prompt_time)
            or not _valid_message_time(message_time)
            or not isinstance(content, str)
            or hashlib.sha256(content.encode("utf-8")).hexdigest() != digest
            or user_id_type not in ("int", "str")
            or conn.execute(
                "SELECT 1 FROM review_clarification WHERE batch_id=?", (batch_id,)
            ).fetchone()):
        raise ValueError("card_reply_evidence_mismatch")
    items = conn.execute(
        "SELECT ordinal FROM item WHERE batch_id=? ORDER BY ordinal", (batch_id,)
    ).fetchall()
    ordinals = [row[0] for row in items]
    if not ordinals or ordinals != list(range(1, len(ordinals) + 1)):
        raise ValueError("invalid_item_ordinals")
    decisions = parse_reply(content, len(ordinals))
    if json.loads(saved) != {str(key): value for key, value in decisions.items()}:
        raise ValueError("reply_decision_mismatch")
    action_ids = json.loads(encoded_actions)
    if action_ids != [CARD_SUBMIT_ACTION_ID]:
        raise ValueError("card_action_evidence_mismatch")
    return {
        "source": "card",
        "batch_id": batch_id,
        "conversation_id": conversation_id,
        "owner_id": owner_id,
        "reply_message_id": message_id,
        "reply_time": message_time,
        "reply_text": content,
        "decisions": decisions,
        "template_id": template_id,
        "callback_id": callback_id,
    }


def card_identity_map_key(user_id: str, user_id_type: int | str) -> str:
    """Return a type-preserving key for a configured callback identity map."""
    if not isinstance(user_id, str) or not user_id.strip() or len(user_id) > 512:
        raise ValueError("card_identity_key_invalid")
    if isinstance(user_id_type, bool) or not isinstance(user_id_type, (int, str)):
        raise ValueError("card_identity_key_invalid")
    type_name = "int" if isinstance(user_id_type, int) else "str"
    type_value = str(user_id_type)
    if not type_value or len(type_value) > 64 or type_value.strip() != type_value:
        raise ValueError("card_identity_key_invalid")
    return f"{type_name}:{type_value}:{user_id}"


def _card_reply_id(callback_id: str) -> str:
    if not isinstance(callback_id, str) or not callback_id:
        raise ValueError("card_callback_id_invalid")
    return "card:" + hashlib.sha256(callback_id.encode("utf-8")).hexdigest()


def load_card_identity_map(path: Path) -> dict[str, str]:
    """Read a private, operator-managed userId-to-owner_open_id map."""
    if not isinstance(path, Path) or path.is_symlink() or not path.is_file():
        raise ValueError("card_identity_map_unavailable")
    try:
        info = path.stat()
        if info.st_uid != os.geteuid() or info.st_mode & 0o077 or info.st_size > 262144:
            raise ValueError("card_identity_map_permissions_unsafe")
        payload = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_unique_object)
    except ValueError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise ValueError("card_identity_map_invalid") from None
    if not isinstance(payload, dict) or not payload or len(payload) > 4096:
        raise ValueError("card_identity_map_invalid")
    for key, owner_open_id in payload.items():
        if (not isinstance(key, str) or len(key) > 1100
                or not isinstance(owner_open_id, str) or not owner_open_id.strip()
                or owner_open_id != owner_open_id.strip() or len(owner_open_id) > 512):
            raise ValueError("card_identity_map_invalid")
        type_name, separator, remainder = key.partition(":")
        type_value, separator, user_id = remainder.partition(":")
        if (not separator or type_name not in ("int", "str")
                or not type_value or not user_id or len(user_id) > 512
                or (type_name == "int" and not type_value.isdigit())):
            raise ValueError("card_identity_map_invalid")
        try:
            expected_key = card_identity_map_key(user_id, int(type_value) if type_name == "int" else type_value)
        except ValueError:
            raise ValueError("card_identity_map_invalid") from None
        if key != expected_key:
            raise ValueError("card_identity_map_invalid")
    return payload


def _validate_batch_id(value: str) -> None:
    if not isinstance(value, str) or not _BATCH_ID.fullmatch(value):
        raise ValueError("card_batch_id_invalid")


def _validate_required_string(value: str, error: str, max_length: int) -> None:
    if (not isinstance(value, str) or not value.strip() or len(value) > max_length
            or value != value.strip()):
        raise ValueError(error)


def _validate_message_time(value: str) -> None:
    if not _valid_message_time(value):
        raise ValueError("card_callback_time_invalid")


def _valid_message_time(value: Any) -> bool:
    if not isinstance(value, str) or not _MESSAGE_TIME.fullmatch(value):
        return False
    base_value = value[:19]
    try:
        parsed = datetime.strptime(base_value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return False
    return parsed.strftime("%Y-%m-%d %H:%M:%S") == base_value


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_json_key")
        result[key] = value
    return result
