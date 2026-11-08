"""Scope fallback gate tests using the real processor and typed resolver contracts."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from event_worker import EventProcessor
from jev_v2 import JevV2Resolver
from llm_adapter import SemanticFallbackResolver, TitleFieldResolver
from test_jev_v2 import _runtime


HOLIDAY_NOTICE = (
    "通知：结合国庆中秋假期安排，节前请对各库区开展安全自查，重点核查消防设施、"
    "电气线路、物资堆放和门窗封闭，发现问题及时整改；假期期间严格落实值班要求，"
    "保持通讯畅通，遇到异常及时报告；节后汇总自查和整改情况，反馈未完成事项。"
)
COMPILED_TITLE = "完成节前库区安全自查并落实假期值班要求"


def _message(text: str = HOLIDAY_NOTICE, **extra):
    return {
        "conversation_id": "c1",
        "message_id": "scope-fallback",
        "group": "假期值班通知群",
        "sender": "王萌",
        "sender_id": "OTHER",
        "create_time": "2026-09-30 14:52:00",
        "text": text,
        **extra,
    }


def _typed_jev_transport(calls, *, scope=0.72, followup=0.88, prospective=0.96):
    def transport(request, timeout):
        calls.append(request)
        answers = {}
        for name, question in request["questions"].items():
            if question["type"] == "noul":
                value = {
                    "applies_to_workgroup": scope,
                    "needs_follow_up": followup,
                    "prospective_action": prospective,
                }[name]
                answers[name] = {"type": "noul", "noul": value}
            else:
                options = question["criteria"]
                choice = "none" if "none" in options else "unknown"
                other = [key for key in options if key != choice]
                probabilities = {choice: 0.99}
                probabilities.update({key: 0.01 / len(other) for key in other})
                answers[name] = {
                    "type": "choice",
                    "choice": choice,
                    "probabilities": probabilities,
                    "confidence": 0.99,
                }
        return {"answers": answers}

    return transport


class _FakeNativeAgent:
    def __init__(self, label, response, timeline, inputs=None):
        self.label = label
        self.response = response
        self.timeline = timeline
        self.inputs = inputs if inputs is not None else []

    def run_conversation(self, user_message, **kwargs):
        self.timeline.append(self.label)
        self.inputs.append((user_message, kwargs))
        return {
            "completed": True,
            "final_response": json.dumps(self.response, ensure_ascii=False),
        }


def _fallback(response, timeline, inputs=None):
    return SemanticFallbackResolver(
        agent_factory=lambda **kwargs: _FakeNativeAgent(
            "semantic_fallback", response, timeline, inputs
        )
    )


def _processor(*, fallback, jev_calls, title_resolver=None, scope=0.72, followup=0.88):
    config, self_user = _runtime()
    config["config_group"][0]["group_name"] = "假期值班通知群"
    return EventProcessor(
        config,
        self_user,
        mode="shadow",
        llm_resolver=JevV2Resolver(
            new_policy=True,
            transport=_typed_jev_transport(
                jev_calls, scope=scope, followup=followup, prospective=0.96
            ),
        ),
        semantic_fallback=fallback,
        title_resolver=title_resolver,
        existing_actions=[],
        business_candidate_query_complete=True,
    )


def _accepted_fallback_response():
    return {
        "applicability": "applicable",
        "followup": "required",
        "title_span": "title_0",
        "due_key": "none",
        "same_work": {},
        "evidence_spans": ["节前请对各库区开展安全自查"],
    }


def test_full_scope_uncertainty_reaches_real_fallback_once_then_compiles_title():
    timeline = []
    jev_calls = []
    fallback_inputs = []
    title_inputs = []
    fallback = _fallback(_accepted_fallback_response(), timeline, fallback_inputs)
    title_resolver = TitleFieldResolver(
        agent_factory=lambda **kwargs: _FakeNativeAgent(
            "title_compiler",
            {
                "title": COMPILED_TITLE,
                "evidence_spans": ["开展安全自查", "落实值班要求"],
            },
            timeline,
            title_inputs,
        )
    )
    processor = _processor(
        fallback=fallback, jev_calls=jev_calls, title_resolver=title_resolver
    )

    plan = processor.process_batch([_message()])[0]

    assert len(jev_calls) == 1
    assert jev_calls[0]["questions"]["applies_to_workgroup"]
    assert len(fallback_inputs) == 1
    assert processor.stats["semantic_fallback_calls"] == 1
    assert processor.stats["semantic_fallback_succeeded"] == 1
    assert timeline == ["semantic_fallback", "title_compiler"]
    assert len(title_inputs) == 1
    title_source = json.loads(title_inputs[0][0])["source_text"]
    assert title_source == HOLIDAY_NOTICE
    assert plan["decision"] == "CREATE"
    assert plan["title"] == COMPILED_TITLE
    assert plan["route_owner"] == "李林骁"
    assert plan["due_date"] is None
    assert plan["due_iso"] is None
    assert plan["semantic_evidence"]["applies_to_workgroup"] == 0.72
    assert plan["semantic_evidence"]["needs_follow_up"] == 0.88
    assert plan["semantic_evidence"]["prospective_action"] == 0.96
    assert plan["fallback_semantic_evidence"]["applicability"] == "applicable"


def test_accepted_long_semantics_without_title_compiler_remains_title_review():
    timeline = []
    jev_calls = []
    fallback_inputs = []
    processor = _processor(
        fallback=_fallback(_accepted_fallback_response(), timeline, fallback_inputs),
        jev_calls=jev_calls,
    )

    plan = processor.process_batch([_message(message_id="no-title-compiler")])[0]

    assert len(jev_calls) == 1
    assert len(fallback_inputs) == 1
    assert timeline == ["semantic_fallback"]
    assert plan["decision"] == "REVIEW"
    assert plan["reason"] == "title_requires_context"
    assert plan["missing_fields"] == ["title"]
    assert plan["due_date"] is None


def test_scope_fallback_blocks_missing_source_context_identity_owner_and_other_assignment():
    cases = [
        (
            "referenced-r-column",
            "请查阅具体要求见R列，并完成库区安全自查。",
            {},
            0.72,
            0.88,
            "jev_v2_referenced_context_missing",
        ),
        (
            "file-only",
            "[文件] 国庆安全通知.pdf fileId: HOLIDAY-ATTACH-1",
            {},
            0.72,
            0.88,
            "attachment_context_ambiguous",
        ),
        (
            "unverified-identity",
            HOLIDAY_NOTICE,
            {"sender": "李林骁"},
            0.20,
            0.50,
            "jev_v2_scope_context_unverified",
        ),
        (
            "missing-owner",
            "请对危险废物清单开展安全自查并落实假期值班要求。",
            {},
            0.72,
            0.88,
            "jev_v2_scope_or_followup_uncertain",
        ),
        (
            "explicit-other-assignment",
            "@苗笑天请负责节前库区安全自查并落实假期值班要求。",
            {},
            0.72,
            0.88,
            "direct_other_assignment_outside_self_scope",
        ),
        (
            "missing-message-id",
            HOLIDAY_NOTICE,
            {"message_id": ""},
            0.72,
            0.88,
            None,
        ),
        (
            "truncated-source",
            ("节前对库区进行安全自查并落实值班要求，" * 300),
            {},
            0.72,
            0.88,
            "jev_v2_source_incomplete",
        ),
    ]
    for message_id, text, extra, scope, followup, expected_reason in cases:
        timeline = []
        jev_calls = []
        fallback_inputs = []
        fallback = _fallback(_accepted_fallback_response(), timeline, fallback_inputs)
        processor = _processor(
            fallback=fallback,
            jev_calls=jev_calls,
            scope=scope,
            followup=followup,
        )

        event = {**_message(text, message_id=message_id), **extra}
        plans = processor.process_batch([event])

        assert fallback_inputs == [], message_id
        assert processor.stats["semantic_fallback_calls"] == 0, message_id
        if plans:
            assert plans[0]["decision"] != "CREATE", message_id
            assert plans[0]["reason"] == expected_reason, message_id
            if message_id == "truncated-source":
                assert jev_calls == [], message_id
                assert plans[0]["jev_error_code"] == "jev_v2_source_incomplete", message_id
                assert plans[0]["jev_error_stage"] == "validation", message_id
                assert plans[0]["reason"] == "jev_v2_source_incomplete", message_id
            elif message_id == "file-only":
                assert jev_calls == [], message_id
            elif message_id == "explicit-other-assignment":
                assert plans[0]["decision"] == "IGNORE", message_id
                assert jev_calls == [], message_id
            elif expected_reason:
                assert len(jev_calls) == 1, message_id
        else:
            assert processor.stats["errors"] == 1, message_id


def test_verified_self_authored_notice_stays_hard_ignore_without_fallback():
    timeline = []
    jev_calls = []
    fallback_inputs = []
    processor = _processor(
        fallback=_fallback(_accepted_fallback_response(), timeline, fallback_inputs),
        jev_calls=jev_calls,
    )
    processor.self_user["message_sender_id"] = "VERIFIED-SENDER"

    plan = processor.process_batch(
        [
            _message(
                "@所有人 假期期间请落实值班要求并保持通讯畅通。",
                message_id="self-authored",
                sender="李林骁",
                sender_id="VERIFIED-SENDER",
            )
        ]
    )[0]

    assert plan["decision"] == "IGNORE"
    assert plan["reason"] == "self_authored_notice"
    assert jev_calls == []
    assert fallback_inputs == []
    assert processor.stats["semantic_fallback_calls"] == 0


def test_media_only_message_never_escalates_scope_fallback():
    timeline = []
    jev_calls = []
    fallback_inputs = []
    processor = _processor(
        fallback=_fallback(_accepted_fallback_response(), timeline, fallback_inputs),
        jev_calls=jev_calls,
    )

    plan = processor.process_batch(
        [_message("[图片消息](mediaId=@HOLIDAY-IMG)", message_id="media-only")]
    )[0]

    assert fallback_inputs == []
    assert processor.stats["semantic_fallback_calls"] == 0
    assert plan["decision"] != "CREATE"
