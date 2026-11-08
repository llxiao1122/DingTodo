import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from review_card_adapter import (
    CARD_DECISION_OPTIONS,
    CARD_FORM_FIELDS_PARAM,
    CARD_FORM_FIELDS_TEMPLATE_KEY,
    CARD_SUBMIT_ACTION_ID,
    CARD_TEMPLATE_FIELD_MAPPING,
    CARD_TEMPLATE_STATIC_FIELDS,
    build_create_and_deliver_body,
    build_review_template_fields,
    parse_stream_card_callback,
    prepare_card_review_input,
    process_card_callback,
    claim_card_callback_work,
    finish_card_callback_work,
    recover_interrupted_card_work,
    verify_card_reply_evidence,
)
from review_inbox import connect, register_batch
from review_auto_process import _verified_reply
from review_handoff import build_handoff


BATCH_ID = "batch_12345678"
PROMPT_TIME = "2026-11-07 10:00:00"
CALLBACK_TIME = "2026-11-07 10:02:00"


def callback_form_field(ordinal, selected_value, **updates):
    field = {
        "type": "SELECT",
        "required": True,
        "name": f"dtd_review_{ordinal}",
        "label": f"复核第 {ordinal} 项\n原文：synthetic source {ordinal}",
        "placeholder": "请选择创建待办或忽略",
        "options": CARD_DECISION_OPTIONS,
        "default_number": selected_value,
    }
    field.update(updates)
    return field


def callback_payload(action_ids=None, form_fields=None, **updates):
    if action_ids is None:
        action_ids = [CARD_SUBMIT_ACTION_ID]
    if form_fields is None:
        form_fields = [callback_form_field(1, 1), callback_form_field(2, 2)]
    private_data = {
        "actionIds": action_ids,
        "params": {CARD_FORM_FIELDS_PARAM: form_fields},
    }
    payload = {
        "type": "actionCallback",
        "callbackId": "card-event-1",
        "outTrackId": BATCH_ID,
        "userId": "platform-user-42",
        "userIdType": 1,
        "content": json.dumps({"cardPrivateData": private_data}),
    }
    payload.update(updates)
    return payload


class ReviewCardAdapterTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.connection = connect(str(Path(self.temporary_directory.name) / "review.db"))
        register_batch(
            self.connection,
            BATCH_ID,
            "cifra-conversation",
            "owner-open-id",
            "prompt-message",
            PROMPT_TIME,
            {1: ["701"], 2: ["702"]},
        )

    def test_template_select_option_schema_matches_numeric_callback_contract(self):
        template_path = Path(__file__).resolve().parents[1] / "review-card.template.json"
        template = json.loads(template_path.read_text(encoding="utf-8"))
        editor_data = json.loads(template["editorData"])
        form_fields = next(
            item for item in editor_data["variableList"] if item.get("name") == "form_fields"
        )
        options = next(item for item in form_fields["schema"] if item.get("name") == "options")
        option_value = next(item for item in options["schema"] if item.get("name") == "value")
        default_number = next(
            item for item in form_fields["schema"] if item.get("name") == "default_number"
        )

        self.assertEqual(option_value["type"], "number")
        self.assertEqual(default_number["type"], "number")
        self.assertTrue(all(type(option["value"]) is int for option in CARD_DECISION_OPTIONS))

    def tearDown(self):
        self.connection.close()
        self.temporary_directory.cleanup()

    def test_connect_migrates_legacy_reply_rows_as_chat_source(self):
        legacy_path = Path(self.temporary_directory.name) / "legacy.db"
        legacy = sqlite3.connect(legacy_path)
        legacy.executescript(
            "CREATE TABLE batch (id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL, "
            "owner_id TEXT NOT NULL, prompt_message_id TEXT NOT NULL UNIQUE, "
            "prompt_time TEXT NOT NULL, state TEXT NOT NULL);"
            "INSERT INTO batch VALUES "
            "('legacy_batch_123','legacy-conversation','legacy-owner','prompt','2026-11-07 10:00:00','answered');"
            "CREATE TABLE reply (message_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL, "
            "message_time TEXT NOT NULL, content_sha256 TEXT NOT NULL, content TEXT NOT NULL, "
            "decisions TEXT NOT NULL);"
            "INSERT INTO reply VALUES "
            "('legacy-reply','legacy_batch_123','2026-11-07 10:01:00','hash','1，忽略','{\"1\":\"ignore\"}');"
        )
        legacy.close()

        migrated = connect(str(legacy_path))
        try:
            columns = {row[1] for row in migrated.execute("PRAGMA table_info(reply)")}
            source = migrated.execute(
                "SELECT source FROM reply WHERE message_id='legacy-reply'",
            ).fetchone()[0]
        finally:
            migrated.close()

        self.assertIn("source", columns)
        self.assertEqual(source, "chat")

    def test_connect_migrates_legacy_card_evidence_as_non_replayable(self):
        legacy_path = Path(self.temporary_directory.name) / "legacy-card.db"
        legacy = sqlite3.connect(legacy_path)
        legacy.executescript(
            "CREATE TABLE batch (id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL, "
            "owner_id TEXT NOT NULL, prompt_message_id TEXT NOT NULL UNIQUE, "
            "prompt_time TEXT NOT NULL, state TEXT NOT NULL);"
            "INSERT INTO batch VALUES "
            "('legacy_batch_123','legacy-conversation','legacy-owner','prompt',"
            "'2026-11-07 10:00:00','answered');"
            "CREATE TABLE review_card_evidence (batch_id TEXT PRIMARY KEY, "
            "callback_id TEXT NOT NULL UNIQUE, template_id TEXT NOT NULL, "
            "owner_id TEXT NOT NULL, message_time TEXT NOT NULL, content_sha256 TEXT NOT NULL, "
            "action_ids TEXT NOT NULL, user_id_type TEXT NOT NULL);"
            "INSERT INTO review_card_evidence VALUES "
            "('legacy_batch_123','legacy-callback','template','legacy-owner',"
            "'2026-11-07 10:01:00','digest','[]','str');"
        )
        legacy.close()

        migrated = connect(str(legacy_path))
        try:
            state = migrated.execute(
                "SELECT work_state FROM review_card_evidence WHERE callback_id='legacy-callback'",
            ).fetchone()[0]
            replayable = recover_interrupted_card_work(migrated)
        finally:
            migrated.close()

        self.assertEqual(state, "legacy_unknown")
        self.assertEqual(replayable, [])

    def test_card_callback_work_is_durable_and_idempotent_across_restart(self):
        callback = parse_stream_card_callback(callback_payload(), message_time=CALLBACK_TIME)
        self.assertIsNotNone(callback)
        resolver = lambda _user_id, _user_id_type: "owner-open-id"

        recorded = process_card_callback(
            self.connection,
            callback,
            template_id="published-template-id",
            identity_resolver=resolver,
        )
        duplicate = process_card_callback(
            self.connection,
            callback,
            template_id="published-template-id",
            identity_resolver=resolver,
        )
        self.assertEqual(recorded["status"], "recorded")
        self.assertEqual(duplicate["status"], "duplicate")
        self.assertEqual(self.connection.execute(
            "SELECT work_state FROM review_card_evidence WHERE callback_id=?",
            (callback.callback_id,),
        ).fetchone()[0], "pending")

        self.assertEqual(recover_interrupted_card_work(self.connection), [callback.callback_id])
        self.assertEqual(claim_card_callback_work(self.connection, callback.callback_id), BATCH_ID)
        self.assertIsNone(claim_card_callback_work(self.connection, callback.callback_id))
        self.assertEqual(recover_interrupted_card_work(self.connection), [callback.callback_id])
        self.assertEqual(self.connection.execute(
            "SELECT work_state FROM review_card_evidence WHERE callback_id=?",
            (callback.callback_id,),
        ).fetchone()[0], "pending")

        self.assertEqual(claim_card_callback_work(self.connection, callback.callback_id), BATCH_ID)
        finish_card_callback_work(self.connection, callback.callback_id)
        self.assertEqual(self.connection.execute(
            "SELECT work_state FROM review_card_evidence WHERE callback_id=?",
            (callback.callback_id,),
        ).fetchone()[0], "dispatched")
        self.assertEqual(recover_interrupted_card_work(self.connection), [])
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM reply").fetchone()[0], 1)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM review_card_evidence").fetchone()[0], 1)

    def test_unknown_card_callback_work_is_not_automatically_replayed(self):
        callback = parse_stream_card_callback(callback_payload(), message_time=CALLBACK_TIME)
        process_card_callback(
            self.connection,
            callback,
            template_id="published-template-id",
            identity_resolver=lambda _user_id, _user_id_type: "owner-open-id",
        )
        self.assertEqual(claim_card_callback_work(self.connection, callback.callback_id), BATCH_ID)
        finish_card_callback_work(self.connection, callback.callback_id, unknown=True)

        self.assertEqual(recover_interrupted_card_work(self.connection), [])
        self.assertEqual(self.connection.execute(
            "SELECT work_state FROM review_card_evidence WHERE callback_id=?",
            (callback.callback_id,),
        ).fetchone()[0], "unknown")

    def test_builder_uses_batch_as_out_track_id_and_card_param_map_is_string(self):
        fields = {
            **CARD_TEMPLATE_STATIC_FIELDS,
            CARD_FORM_FIELDS_TEMPLATE_KEY: json.dumps([
            {
                "type": "SELECT",
                "required": True,
                "name": "dtd_review_1",
                "label": "synthetic item",
                "placeholder": "请选择创建待办或忽略",
                "options": CARD_DECISION_OPTIONS,
            },
            ], ensure_ascii=False, separators=(",", ":")),
        }

        body = build_create_and_deliver_body(
            batch_id=BATCH_ID,
            template_id="published-template-id",
            template_fields=fields,
            open_space_id="dtv1.card//IM_ROBOT.owner-open-id",
            robot_code="cifra-app-code",
        )

        self.assertEqual(body["outTrackId"], BATCH_ID)
        self.assertEqual(body["callbackType"], "STREAM")
        self.assertEqual(body["cardTemplateId"], "published-template-id")
        self.assertEqual(body["cardData"], {"cardParamMap": fields})
        self.assertIsInstance(body["cardData"]["cardParamMap"]["form_fields"], str)
        self.assertEqual(body["imRobotOpenDeliverModel"], {
            "spaceType": "IM_ROBOT",
            "robotCode": "cifra-app-code",
        })

    def test_builder_rejects_unbound_template_shape_and_invalid_target(self):
        valid_fields = {
            **CARD_TEMPLATE_STATIC_FIELDS,
            CARD_FORM_FIELDS_TEMPLATE_KEY: json.dumps([{
                "type": "SELECT",
                "required": True,
                "name": "dtd_review_1",
                "label": "synthetic item",
                "placeholder": "请选择创建待办或忽略",
                "options": CARD_DECISION_OPTIONS,
            }], ensure_ascii=False, separators=(",", ":")),
        }
        base = {
            "batch_id": BATCH_ID,
            "template_id": "published-template-id",
            "template_fields": ["not", "a", "map"],
            "open_space_id": "dtv1.card//IM_ROBOT.owner-open-id",
            "robot_code": "cifra-app-code",
        }
        with self.assertRaisesRegex(ValueError, "card_template_fields_invalid"):
            build_create_and_deliver_body(**base)
        with self.assertRaisesRegex(ValueError, "card_template_fields_invalid"):
            build_create_and_deliver_body(**{
                **base,
                "template_fields": {**valid_fields, "form_status": "disabled"},
            })
        with self.assertRaisesRegex(ValueError, "card_open_space_id_invalid"):
            build_create_and_deliver_body(
                **{**base, "template_fields": {}, "open_space_id": "dtv1.card//IM_GROUP.group"}
            )

    def test_template_builder_serializes_select_form_fields_without_batch_identity(self):
        items = [
            {
                "action_id": "701",
                "source_date": "2026-11-07",
                "source": {"text": "sample source", "group_name": "sample group",
                           "message_time": "2026-11-07 09:00:00", "text_truncated": False},
            },
            {
                "action_id": "702",
                "source_date": "2026-11-07",
                "source": {"text": "second source", "text_truncated": False},
            },
        ]
        fields = build_review_template_fields(
            batch_id=BATCH_ID,
            review_text=f"复核编号：{BATCH_ID}\n\nreview prompt",
            items=items,
            field_mapping=CARD_TEMPLATE_FIELD_MAPPING,
        )

        self.assertEqual(fields["title"], "待办复核")
        self.assertEqual(fields["form_status"], "normal")
        self.assertEqual(fields["button_text"], "提交复核")
        self.assertEqual(set(fields), {"title", "form_status", "button_text", "form_fields"})
        form_fields = json.loads(fields["form_fields"])
        self.assertEqual(len(form_fields), 2)
        self.assertEqual(form_fields[0], {
            "type": "SELECT",
            "required": True,
            "name": "dtd_review_1",
            "label": "复核第 1 项\n2026-11-07\n来源群：sample group\n消息时间：2026-11-07 09:00:00\n原文：sample source",
            "placeholder": "请选择创建待办或忽略",
            "options": CARD_DECISION_OPTIONS,
        })
        self.assertEqual(form_fields[1]["name"], "dtd_review_2")
        self.assertEqual(form_fields[1]["options"], [
            {"value": 1, "text": {"zh_CN": "创建待办"}},
            {"value": 2, "text": {"zh_CN": "忽略"}},
        ])
        self.assertNotIn(BATCH_ID, json.dumps(fields, ensure_ascii=False))

    def test_template_builder_rejects_unconfigured_template_field_and_incomplete_sources(self):
        item = [{"action_id": "701", "source_date": "2026-11-07",
                 "source": {"text": "sample source", "text_truncated": False}}]
        base = {"batch_id": BATCH_ID,
                "review_text": f"复核编号：{BATCH_ID}\n\nreview prompt", "items": item}
        invalid_cases = (
            ({"form_fields": "form_fields"}, item, "card_template_mapping_invalid"),
            ({**CARD_TEMPLATE_FIELD_MAPPING, "form_fields": "review_text"}, item,
             "card_template_mapping_invalid"),
            ({**CARD_TEMPLATE_FIELD_MAPPING, "extra": "title"}, item,
             "card_template_mapping_invalid"),
            (CARD_TEMPLATE_FIELD_MAPPING, [dict(item[0], source={
                "text": 1, "text_truncated": False,
            })], "card_template_source_invalid"),
        )
        for mapping, selected_items, error in invalid_cases:
            with self.subTest(mapping=mapping):
                with self.assertRaisesRegex(ValueError, error):
                    build_review_template_fields(
                        **{**base, "items": selected_items}, field_mapping=mapping,
                    )

    def test_template_builder_fails_closed_when_serialized_form_exceeds_bound(self):
        items = [{
            "action_id": str(700 + ordinal),
            "source_date": "2026-11-07",
            "source": {"text": "🙂" * 1600, "group_name": "群" * 512,
                       "text_truncated": False},
        } for ordinal in range(1, 6)]
        with self.assertRaisesRegex(ValueError, "card_template_value_too_large"):
            build_review_template_fields(
                batch_id=BATCH_ID,
                review_text=f"复核编号：{BATCH_ID}\n\nreview prompt",
                items=items,
                field_mapping=CARD_TEMPLATE_FIELD_MAPPING,
            )

    def test_parser_normalizes_callback_without_retaining_raw_content(self):
        parsed = parse_stream_card_callback(json.dumps(callback_payload()), message_time=CALLBACK_TIME)

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.out_track_id, BATCH_ID)
        self.assertEqual(parsed.user_id, "platform-user-42")
        self.assertEqual(parsed.user_id_type, 1)
        self.assertEqual(parsed.action_ids, (CARD_SUBMIT_ACTION_ID,))
        self.assertEqual(parsed.decisions, ((1, "create"), (2, "ignore")))
        self.assertEqual(parsed.callback_id, "card-event-1")
        self.assertFalse(hasattr(parsed, "content"))

    def test_parser_rejects_incomplete_or_ambiguous_callback_fields(self):
        missing_selection = callback_form_field(1, 1)
        del missing_selection["default_number"]
        malformed = [
            callback_payload(type="other-event"),
            callback_payload(outTrackId="invalid batch id"),
            callback_payload(userIdType=True),
            callback_payload(content="not-json"),
            callback_payload(action_ids=[]),
            callback_payload(action_ids=[CARD_SUBMIT_ACTION_ID, CARD_SUBMIT_ACTION_ID]),
            callback_payload(action_ids=["unapproved-action"]),
            callback_payload(form_fields=[]),
            callback_payload(form_fields=[missing_selection, callback_form_field(2, 2)]),
            callback_payload(form_fields=[callback_form_field(1, 1, extra="reject")]),
            callback_payload(form_fields=[callback_form_field(1, 1), callback_form_field(1, 2)]),
            callback_payload(form_fields=[callback_form_field(1, 3), callback_form_field(2, 2)]),
            callback_payload(form_fields=[callback_form_field(1, True), callback_form_field(2, 2)]),
            callback_payload(form_fields=[callback_form_field(1, "1"), callback_form_field(2, 2)]),
            callback_payload(form_fields=[callback_form_field(2, 1), callback_form_field(3, 2)]),
            callback_payload(form_fields=[callback_form_field(
                1, 1, options=[
                    {"value": 1, "text": {"zh_CN": "创建待办"}},
                    {"value": 3, "text": {"zh_CN": "澄清"}},
                ],
            ), callback_form_field(2, 2)]),
            callback_payload(content=json.dumps({"cardPrivateData": {
                "actionIds": [CARD_SUBMIT_ACTION_ID],
                "params": {CARD_FORM_FIELDS_PARAM: [callback_form_field(1, 1)],
                           "unexpected": "reject"},
            }})),
            callback_payload(content=(
                '{"cardPrivateData":{"actionIds":["dtd-review-submit"],'
                '"params":{"submit_form_fields":[{"type":"SELECT",'
                '"required":true,"name":"dtd_review_1","label":"synthetic",'
                '"placeholder":"请选择创建待办或忽略","options":['
                '{"value":1,"text":{"zh_CN":"创建待办"}},'
                '{"value":2,"text":{"zh_CN":"忽略"}}],'
                '"default_number":1,"default_number":2}]}}}'
            )),
        ]

        for payload in malformed:
            with self.subTest(payload=payload.get("type")):
                self.assertIsNone(parse_stream_card_callback(payload, message_time=CALLBACK_TIME))
        self.assertIsNone(parse_stream_card_callback(callback_payload(), message_time=None))
        self.assertIsNone(parse_stream_card_callback(callback_payload(), message_time="not-a-time"))

    def test_missing_callback_id_gets_stable_content_independent_dedupe_key(self):
        payload = callback_payload()
        payload.pop("callbackId")

        first = parse_stream_card_callback(payload, message_time=CALLBACK_TIME)
        second = parse_stream_card_callback(payload, message_time=CALLBACK_TIME)

        self.assertIsNotNone(first)
        self.assertEqual(first.callback_id, second.callback_id)
        self.assertTrue(first.callback_id.startswith("card_"))

        changed_decision = parse_stream_card_callback(
            callback_payload(form_fields=[callback_form_field(1, 2), callback_form_field(2, 2)]),
            message_time=CALLBACK_TIME,
        )
        self.assertIsNotNone(changed_decision)
        self.assertNotEqual(first.callback_id, changed_decision.callback_id)

    def test_verified_callback_reuses_existing_sql_state_machine(self):
        callback = parse_stream_card_callback(callback_payload(), message_time=CALLBACK_TIME)
        self.assertIsNotNone(callback)

        result = process_card_callback(
            self.connection,
            callback,
            template_id="published-template-id",
            identity_resolver=lambda user_id, user_id_type: (
                "owner-open-id" if (user_id, user_id_type) == ("platform-user-42", 1) else None
            ),
        )

        self.assertEqual(result, {"status": "recorded", "batch_id": BATCH_ID})
        self.assertEqual(
            self.connection.execute("SELECT state FROM batch WHERE id=?", (BATCH_ID,)).fetchone()[0],
            "answered",
        )
        row = self.connection.execute(
            "SELECT content,decisions FROM reply WHERE batch_id=?", (BATCH_ID,)
        ).fetchone()
        self.assertEqual(row[0], "1，创建；2，忽略")
        self.assertEqual(json.loads(row[1]), {"1": "create", "2": "ignore"})
        self.assertEqual(self.connection.execute(
            "SELECT source FROM reply WHERE batch_id=?", (BATCH_ID,),
        ).fetchone()[0], "card")
        verified = verify_card_reply_evidence(self.connection, BATCH_ID)
        self.assertEqual(verified["source"], "card")
        self.assertEqual(verified["decisions"], {1: "create", 2: "ignore"})
        state_machine_reply = _verified_reply(self.connection, BATCH_ID, history=[])
        self.assertEqual(state_machine_reply["reply_source"], "card")
        self.assertEqual(state_machine_reply["decisions"], {1: "create", 2: "ignore"})

        rows = [
            {
                "id": action_id,
                "conversation_id": f"source-conversation-{action_id}",
                "message_id": f"source-message-{action_id}",
                "status": "REVIEW",
                "effective_external_task_id": None,
                "duplicate_of_action_id": None,
                "updated_at": "2026-11-07",
                "business_key": f"source-{action_id}",
            }
            for action_id in ("701", "702")
        ]

        class Adapter:
            def query(self, request):
                if request["view"] == "actions":
                    return {
                        "view": "actions",
                        "rows": rows,
                        "returned_count": len(rows),
                        "has_more": False,
                        "next_page_token": None,
                    }
                action_id = request["message_id"].removeprefix("source-message-")
                return {
                    "view": "message",
                    "rows": [{
                        "conversation_id": request["conversation_id"],
                        "message_id": request["message_id"],
                        "text": f"synthetic source {action_id}",
                        "group_name": "source group",
                        "sender_name": "source owner",
                        "message_time": "2026-11-06T10:00:00",
                    }],
                    "returned_count": 1,
                    "matched_count": 1,
                    "has_more": False,
                }

        handoff = build_handoff(
            self.connection,
            Adapter(),
            BATCH_ID,
            card_evidence=verified,
        )
        self.assertEqual(handoff["reply_source"], "card")
        self.assertEqual(
            [item["user_decision"] for item in handoff["items"]],
            ["create", "ignore"],
        )
        self.assertEqual(handoff["writes_performed"], 0)

        duplicate = process_card_callback(
            self.connection,
            callback,
            template_id="published-template-id",
            identity_resolver=lambda _user_id, _user_id_type: "owner-open-id",
        )
        self.assertEqual(duplicate["status"], "duplicate")
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM reply").fetchone()[0], 1)
        different_event_same_actions = parse_stream_card_callback(
            callback_payload(callbackId="event-card-2"), message_time=CALLBACK_TIME,
        )
        self.assertIsNotNone(different_event_same_actions)
        self.assertEqual(process_card_callback(
            self.connection,
            different_event_same_actions,
            template_id="published-template-id",
            identity_resolver=lambda _user_id, _user_id_type: "owner-open-id",
        )["status"], "already_answered")
        self.assertEqual(self.connection.execute(
            "SELECT COUNT(*) FROM review_card_evidence",
        ).fetchone()[0], 1)

    def test_callback_rejects_owner_action_and_time_mismatches_before_sql_write(self):
        callback = parse_stream_card_callback(callback_payload(), message_time=CALLBACK_TIME)
        self.assertIsNotNone(callback)
        expired_callback = parse_stream_card_callback(
            callback_payload(), message_time=PROMPT_TIME,
        )
        self.assertIsNotNone(expired_callback)

        cases = [
            (callback, "card_identity_mismatch",
             lambda _user_id, _user_id_type: "other-owner"),
            (expired_callback, "card_callback_not_after_prompt",
             lambda _user_id, _user_id_type: "owner-open-id"),
        ]
        for candidate, reason, resolver in cases:
            with self.subTest(reason=reason), self.assertRaisesRegex(ValueError, reason):
                prepare_card_review_input(
                    self.connection,
                    candidate,
                    identity_resolver=resolver,
                )
        self.assertEqual(self.connection.execute("SELECT state FROM batch").fetchone()[0], "open")
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM reply").fetchone()[0], 0)

    def test_process_rejects_wrong_owner_without_persisting_card_evidence(self):
        callback = parse_stream_card_callback(callback_payload(), message_time=CALLBACK_TIME)
        self.assertIsNotNone(callback)

        with self.assertRaisesRegex(ValueError, "card_identity_mismatch"):
            process_card_callback(
                self.connection,
                callback,
                template_id="published-template-id",
                identity_resolver=lambda _user_id, _user_id_type: "different-owner",
            )

        self.assertEqual(self.connection.execute(
            "SELECT state FROM batch WHERE id=?", (BATCH_ID,),
        ).fetchone()[0], "open")
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM reply").fetchone()[0], 0)
        self.assertEqual(self.connection.execute(
            "SELECT COUNT(*) FROM review_card_evidence",
        ).fetchone()[0], 0)

    def test_state_machine_rejects_card_evidence_that_does_not_match_stored_decisions(self):
        callback = parse_stream_card_callback(callback_payload(), message_time=CALLBACK_TIME)
        self.assertIsNotNone(callback)
        process_card_callback(
            self.connection,
            callback,
            template_id="published-template-id",
            identity_resolver=lambda _user_id, _user_id_type: "owner-open-id",
        )
        self.connection.execute(
            "UPDATE review_card_evidence SET action_ids=? WHERE batch_id=?",
            (json.dumps(["tampered-action"]), BATCH_ID),
        )

        with self.assertRaisesRegex(ValueError, "card_action_evidence_mismatch"):
            verify_card_reply_evidence(self.connection, BATCH_ID)

    def test_process_rejects_unknown_batch_and_parser_rejects_unknown_action(self):
        unknown_batch = parse_stream_card_callback(
            callback_payload(outTrackId="batch_unknown123"), message_time=CALLBACK_TIME,
        )
        unknown_action = parse_stream_card_callback(callback_payload([
            "unapproved-action",
        ]), message_time=CALLBACK_TIME)
        self.assertIsNotNone(unknown_batch)
        self.assertIsNone(unknown_action)

        with self.assertRaisesRegex(ValueError, "card_batch_unknown"):
            process_card_callback(
                self.connection,
                unknown_batch,
                template_id="published-template-id",
                identity_resolver=lambda _user_id, _user_id_type: "owner-open-id",
            )
        self.assertEqual(self.connection.execute(
            "SELECT state FROM batch WHERE id=?", (BATCH_ID,),
        ).fetchone()[0], "open")
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM reply").fetchone()[0], 0)

    def test_callback_uses_out_track_id_and_requires_decisions_for_all_batch_items(self):
        incomplete = parse_stream_card_callback(
            callback_payload(form_fields=[callback_form_field(1, 1)]),
            message_time=CALLBACK_TIME,
        )
        extra_ordinal = parse_stream_card_callback(
            callback_payload(form_fields=[
                callback_form_field(1, 1),
                callback_form_field(2, 2),
                callback_form_field(3, 1),
            ]),
            message_time=CALLBACK_TIME,
        )
        mismatched_out_track_id = parse_stream_card_callback(
            callback_payload(outTrackId="other_batch_123"), message_time=CALLBACK_TIME,
        )
        self.assertIsNotNone(incomplete)
        self.assertIsNotNone(extra_ordinal)
        self.assertIsNotNone(mismatched_out_track_id)

        for callback, reason in (
            (incomplete, "card_action_set_incomplete"),
            (extra_ordinal, "card_action_set_incomplete"),
            (mismatched_out_track_id, "card_batch_not_unique_open"),
        ):
            with self.subTest(reason=reason), self.assertRaisesRegex(ValueError, reason):
                prepare_card_review_input(
                    self.connection,
                    callback,
                    message_time=CALLBACK_TIME,
                    identity_resolver=lambda _user_id, _user_id_type: "owner-open-id",
                )
        self.assertEqual(self.connection.execute("SELECT state FROM batch").fetchone()[0], "open")

    def test_callback_out_track_id_must_resolve_to_the_unique_open_batch(self):
        callback = parse_stream_card_callback(
            callback_payload(outTrackId="batch_other123"), message_time=CALLBACK_TIME,
        )
        self.assertIsNotNone(callback)

        with self.assertRaisesRegex(ValueError, "card_batch_not_unique_open"):
            prepare_card_review_input(
                self.connection,
                callback,
                message_time=CALLBACK_TIME,
                identity_resolver=lambda _user_id, _user_id_type: "owner-open-id",
            )

    def test_callback_does_not_process_a_batch_with_clarification(self):
        callback = parse_stream_card_callback(callback_payload(), message_time=CALLBACK_TIME)
        self.connection.execute(
            "INSERT INTO review_clarification "
            "(batch_id,prompt_text,status,claimed_at,baseline_message_ids,bot_id,prompt_message_id,prompt_time) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (BATCH_ID, "clarification", "delivered", PROMPT_TIME, "[]", "bot", "clarify-1", PROMPT_TIME),
        )

        with self.assertRaisesRegex(ValueError, "card_clarification_not_supported"):
            prepare_card_review_input(
                self.connection,
                callback,
                message_time=CALLBACK_TIME,
                identity_resolver=lambda _user_id, _user_id_type: "owner-open-id",
            )


if __name__ == "__main__":
    unittest.main()
