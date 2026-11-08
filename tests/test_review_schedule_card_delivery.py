import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from review_inbox import connect, prepare_dispatch, register_batch
from review_card_adapter import (
    CARD_DECISION_OPTIONS,
    CARD_TEMPLATE_FIELD_MAPPING,
    CARD_TEMPLATE_STATIC_FIELDS,
)
from review_schedule_dws import ReviewCardDeliveryConfig, ReviewScheduleDws


BATCH_ID = "sched_card_1234"
ITEMS = [{
    "action_id": "701",
    "source_date": "2026-11-07",
    "source": {"text": "synthetic source", "text_truncated": False,
                "group_name": "synthetic group", "message_time": "2026-11-07 09:00:00"},
}]
CONTEXT = {
    "conversation": "owner-conversation",
    "owner_user": "platform-user",
    "robot_code": "cifra-robot",
    "text": f"复核编号：{BATCH_ID}\n\nreview prompt with synthetic source",
    "items": ITEMS,
    "status": "delivered",
    "preview_hash": "digest",
    "digest": "digest",
    "sending_at": "2026-11-07 10:00:00",
}


def make_config(identity_map_file: Path) -> ReviewCardDeliveryConfig:
    return ReviewCardDeliveryConfig(
        enabled=True,
        template_id="published-template",
        template_version="v3",
        identity_map_file=identity_map_file,
        field_mapping=CARD_TEMPLATE_FIELD_MAPPING,
    )


def prepare_review_database(tmp_path: Path):
    connection = connect(str(tmp_path / "review.sqlite3"))
    prepare_dispatch(connection, BATCH_ID, "owner-conversation", "owner-open-id", ITEMS)
    connection.execute(
        "UPDATE dispatch SET status='delivered',prompt_message_id=?,prompt_time=? WHERE id=?",
        ("text-prompt", "2026-11-07 10:00:00", BATCH_ID),
    )
    register_batch(
        connection, BATCH_ID, "owner-conversation", "owner-open-id", "text-prompt",
        "2026-11-07 10:00:00", {1: ["701"]}, enable_automation=True,
    )
    return connection


def write_identity_map(path: Path, owner_open_id: str = "owner-open-id") -> None:
    path.write_text(json.dumps({"int:1:platform-user": owner_open_id}), encoding="utf-8")
    os.chmod(path, 0o600)


def make_adapter(connection, card_config: ReviewCardDeliveryConfig) -> ReviewScheduleDws:
    return ReviewScheduleDws(
        connection, profile="profile", bot_open_id="cifra-bot",
        owner_open_id="owner-open-id", app_id="cifra-app", card_config=card_config,
    )


def test_card_sender_uses_verified_user_identity_and_writes_delivery_receipt(tmp_path):
    connection = prepare_review_database(tmp_path)
    identity_map_file = tmp_path / "identity-map.json"
    write_identity_map(identity_map_file)
    adapter = make_adapter(connection, make_config(identity_map_file))
    client = MagicMock()
    client.create_and_deliver_review_card.return_value = {
        "delivered": True, "out_track_id": BATCH_ID,
    }

    try:
        with patch("review_schedule_dws.review_dispatch_send.verify_context", return_value=CONTEXT), \
             patch.object(adapter, "_check_identity"), \
             patch("review_schedule_dws.DwsClient", return_value=client) as client_type:
            result = adapter.send_card(BATCH_ID)
            duplicate = adapter.send_card(BATCH_ID)

        assert result == {"status": "delivered"}
        assert duplicate == {"status": "delivered", "reason": "already_attempted"}
        client_type.assert_called_once_with(confirmed_official_writes=True)
        call = client.create_and_deliver_review_card.call_args.kwargs
        assert call["batch_id"] == BATCH_ID
        assert call["open_space_id"] == "dtv1.card//IM_ROBOT.platform-user"
        assert call["template_fields"] == {
            **CARD_TEMPLATE_STATIC_FIELDS,
            "form_fields": json.dumps([{
                "type": "SELECT",
                "required": True,
                "name": "dtd_review_1",
                "label": "复核第 1 项\n2026-11-07\n来源群：synthetic group\n"
                        "消息时间：2026-11-07 09:00:00\n原文：synthetic source",
                "placeholder": "请选择创建待办或忽略",
                "options": CARD_DECISION_OPTIONS,
            }], ensure_ascii=False, separators=(",", ":")),
        }
        assert BATCH_ID not in json.dumps(call["template_fields"], ensure_ascii=False)
        assert call["authorization_enabled"] is True
        receipt = connection.execute(
            "SELECT status,template_id,template_version,error_code "
            "FROM review_card_delivery WHERE batch_id=?", (BATCH_ID,),
        ).fetchone()
        assert receipt == ("delivered", "published-template", "v3", None)
    finally:
        connection.close()


def test_identity_mismatch_fails_closed_before_claim_or_sender(tmp_path):
    connection = prepare_review_database(tmp_path)
    identity_map_file = tmp_path / "identity-map.json"
    write_identity_map(identity_map_file, "different-owner")
    adapter = make_adapter(connection, make_config(identity_map_file))

    try:
        with patch("review_schedule_dws.review_dispatch_send.verify_context", return_value=CONTEXT), \
             patch.object(adapter, "_check_identity"), \
             patch("review_schedule_dws.DwsClient") as client_type:
            result = adapter.send_card(BATCH_ID)

        assert result == {"status": "not_attempted", "reason": "identity_unmapped"}
        client_type.assert_not_called()
        assert connection.execute(
            "SELECT COUNT(*) FROM review_card_delivery",
        ).fetchone()[0] == 0
    finally:
        connection.close()


def test_api_exception_is_unknown_and_never_retried(tmp_path):
    connection = prepare_review_database(tmp_path)
    identity_map_file = tmp_path / "identity-map.json"
    write_identity_map(identity_map_file)
    adapter = make_adapter(connection, make_config(identity_map_file))
    client = MagicMock()
    client.create_and_deliver_review_card.side_effect = RuntimeError("synthetic-secret-marker")

    try:
        with patch("review_schedule_dws.review_dispatch_send.verify_context", return_value=CONTEXT), \
             patch.object(adapter, "_check_identity"), \
             patch("review_schedule_dws.DwsClient", return_value=client) as client_type:
            first = adapter.send_card(BATCH_ID)
            second = adapter.send_card(BATCH_ID)

        assert first == {"status": "delivery_unknown", "reason": "delivery_exception"}
        assert second == {"status": "delivery_unknown", "reason": "already_attempted"}
        client_type.assert_called_once_with(confirmed_official_writes=True)
        assert "synthetic-secret-marker" not in repr((first, second))
        assert connection.execute(
            "SELECT status,error_code FROM review_card_delivery WHERE batch_id=?",
            (BATCH_ID,),
        ).fetchone() == ("unknown", "delivery_exception")
    finally:
        connection.close()


def test_card_delivery_config_is_disabled_by_default_and_requires_explicit_true():
    assert ReviewCardDeliveryConfig.from_environment({}).enabled is False
    assert ReviewCardDeliveryConfig.from_environment({
        "DWS_CARD_REVIEW_ENABLED": "1",
    }).enabled is False
    assert ReviewCardDeliveryConfig.from_environment({
        "DWS_CARD_REVIEW_ENABLED": "true",
    }).enabled is True
