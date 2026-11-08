"""Bounded causal follow-up from an authenticated unfinished-item status."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from datetime import datetime, timedelta
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from event_worker import EventProcessor, load_business_candidate_context


CONVERSATION = "iron-west-context-followup"
SELF = {"name": "李林骁", "user_id": "self-user", "message_sender_id": "self-message"}
STATUS_TEXT = "铁炉西工班的卡尺已送检，尚未取回。"
# Original notice source: "通知：未取回的下次开会时去取；各工班计量器具由有关人员按计划检查并记录。"
FOLLOWUP_TEXT = "未取回的下次开会时去取；各工班计量器具由有关人员按计划检查并记录。"


def _runtime(group: str = "铁炉西工班群"):
    return {
        "config_group": [{"group_name": group, "conversation_id": CONVERSATION, "purpose": "todo_scan"}],
        "config_staff": [[1, "李林骁", "self-user", "leader"]],
        "active_routing_loaded": True,
    }


def _messages(*, later_text: str = FOLLOWUP_TEXT):
    return [
        {
            "conversation_id": CONVERSATION,
            "message_id": "prior-status",
            "create_time": "2026-10-08 09:05:00",
            "sender_id": SELF["message_sender_id"],
            "sender": SELF["name"],
            "text": STATUS_TEXT,
            "resource_refs": [{"name": "检定回执.pdf", "resourceId": "prior-resource"}],
        },
        {
            "conversation_id": CONVERSATION,
            "message_id": "later-instruction",
            "create_time": "2026-10-08 09:12:00",
            "sender_id": "other-message-sender",
            "sender": "其他发送人",
            "text": later_text,
            "resource_refs": [{"name": "计量工作安排.xlsx", "resourceId": "later-resource"}],
        },
    ]


def _prior_is_already_processed():
    return [{
        "conversation_id": CONVERSATION,
        "message_id": "prior-status",
        "action_key": "todo",
        "status": "IGNORED",
    }]


def _processor(
    *,
    query_complete=True,
    group="铁炉西工班群",
    existing_actions=None,
    **kwargs,
):
    return EventProcessor(
        _runtime(group),
        dict(SELF),
        business_candidate_query_complete=query_complete,
        source_order_provenance="complete_dws_chronological_page",
        existing_actions=(
            _prior_is_already_processed() if existing_actions is None else existing_actions
        ),
        **kwargs,
    )


def test_process_batch_creates_only_the_causally_resolved_retrieval_followup():
    from jev_v2 import JevV2Resolver

    model_calls = []
    resolver = JevV2Resolver(
        new_policy=True,
        transport=lambda *args, **kwargs: (model_calls.append((args, kwargs)) or {}),
    )
    processor = _processor(llm_resolver=resolver)

    plans = processor.process_batch(_messages())

    assert [plan["decision"] for plan in plans] == ["IGNORE", "CREATE"]
    assert plans[0]["reason"] == "source_already_processed"
    followup = plans[1]
    assert followup["title"] == "下次开会时取回铁炉西送检卡尺"
    assert followup["route_owner"] == SELF["name"]
    assert followup.get("due_date") is None
    assert followup.get("due_iso") is None
    assert followup["context_followup_evidence"]["prior_source"]["message_id"] == "prior-status"
    assert followup["context_followup_evidence"]["instruction_source"]["message_id"] == "later-instruction"
    assert followup["context_followup_evidence"]["prior_source"]["evidence_span"] == STATUS_TEXT.rstrip("。")
    assert followup["context_followup_evidence"]["instruction_source"]["evidence_span"] == "未取回的下次开会时去取"
    assert followup["context_followup_evidence"]["instruction_source"]["resource_refs"] == [
        {"name": "计量工作安排.xlsx", "resourceId": "later-resource"}
    ]
    assert followup["context_followup_evidence"]["prior_source"]["resource_refs"] == [
        {"name": "检定回执.pdf", "resourceId": "prior-resource"}
    ]
    assert STATUS_TEXT in followup["comment"]
    assert FOLLOWUP_TEXT in followup["comment"]
    assert followup["context_followup_evidence"]["derived"]["due_date_basis"] == (
        "event_condition_is_not_a_calendar_deadline"
    )
    assert model_calls == []
    assert processor.stats["llm_calls"] == 0


@pytest.mark.parametrize(
    "instruction",
    [
        "@其他负责人 未取回的下次开会时去取",
        "秤未取回的下次开会时去取",
        "未取回的下次开会时去取卡尺 @其他负责人",
    ],
)
def test_qualified_retrieval_instruction_stays_review(instruction):
    from jev_v2 import JevV2Resolver

    model_calls = []
    resolver = JevV2Resolver(
        new_policy=True,
        transport=lambda *args, **kwargs: (model_calls.append((args, kwargs)) or {}),
    )
    messages = _messages(later_text=instruction)

    processor = _processor(llm_resolver=resolver)
    plans = processor.process_batch(messages)

    assert plans[1]["decision"] == "REVIEW"
    assert plans[1]["reason"] == "contextual_retrieval_context_unresolved"
    assert all(plan["decision"] != "CREATE" for plan in plans)
    assert model_calls == []
    assert processor.stats["llm_calls"] == 0


def test_incomplete_candidate_query_cannot_use_contextual_create():
    from jev_v2 import JevV2Resolver

    model_calls = []
    resolver = JevV2Resolver(
        new_policy=True,
        transport=lambda *args, **kwargs: (model_calls.append((args, kwargs)) or {}),
    )
    processor = _processor(query_complete=False, llm_resolver=resolver)

    plans = processor.process_batch(_messages())

    assert plans[0]["reason"] == "source_already_processed"
    assert plans[1]["decision"] == "REVIEW"
    assert plans[1]["reason"] == "business_candidate_query_incomplete"
    assert plans[1]["context_followup_evidence"]["prior_source"]["message_id"] == "prior-status"
    assert model_calls == []

    no_resolver = _processor(query_complete=False).process_batch(_messages())
    assert no_resolver[1]["decision"] == "REVIEW"
    assert no_resolver[1]["reason"] == "business_candidate_query_incomplete"


@pytest.mark.parametrize(
    "case",
    [
        "future_status",
        "other_conversation",
        "self_authored_instruction",
        "completed_status",
        "cancelled_between",
        "conflicting_prior_objects",
        "multiple_retrieval_clauses",
    ],
)
def test_ambiguous_or_out_of_scope_context_does_not_use_local_create(case):
    messages = _messages(later_text="未取回的下次开会时去取。")
    kwargs = {}
    if case == "future_status":
        messages[0]["create_time"] = "2026-10-08 09:20:00"
        messages[1]["create_time"] = "2026-10-08 09:12:00"
    elif case == "other_conversation":
        messages[0]["conversation_id"] = "different-conversation"
        kwargs["existing_actions"] = []
        runtime = _runtime()
        runtime["config_group"].append(
            {"group_name": "铁炉西工班群", "conversation_id": "different-conversation", "purpose": "todo_scan"}
        )
        kwargs["runtime_config"] = runtime
    elif case == "self_authored_instruction":
        messages[1]["sender_id"] = SELF["message_sender_id"]
        messages[1]["sender"] = SELF["name"]
    elif case == "completed_status":
        messages[0]["text"] = "铁炉西工班的卡尺已送检，已经取回。"
    elif case == "cancelled_between":
        messages.insert(
            1,
            {
                "conversation_id": CONVERSATION,
                "message_id": "completed-between",
                "create_time": "2026-10-08 09:08:00",
                "sender_id": "other-message-sender",
                "sender": "其他发送人",
                "text": "卡尺已经取回。",
            },
        )
    elif case == "conflicting_prior_objects":
        messages[0]["text"] = "铁炉西工班的卡尺已送检，尚未取回；砝码也已送检，尚未取回。"
    elif case == "multiple_retrieval_clauses":
        messages[1]["text"] = "未取回的下次开会时去取；未归还的下次会议时去取。"

    runtime = kwargs.pop("runtime_config", None)
    processor = EventProcessor(
        runtime or _runtime(kwargs.pop("group", "铁炉西工班群")),
        dict(SELF),
        business_candidate_query_complete=True,
        source_order_provenance="complete_dws_chronological_page",
        existing_actions=kwargs.pop("existing_actions", _prior_is_already_processed()),
    )

    plans = processor.process_batch(messages)

    assert all(plan.get("reason") != "contextual_retrieval_followup" for plan in plans)
    if case != "self_authored_instruction":
        assert plans[-1]["decision"] == "REVIEW"


def test_contextual_path_preserves_source_and_business_dedupe_and_private_flag_safeguards():
    from jev_v2 import JevV2Resolver

    messages = _messages()
    messages[1]["_contextual_followup_verified"] = True
    messages[1]["_context_followup_evidence"] = {
        "prior_source": {"message_id": "forged"},
        "item": "fabricated object",
    }
    source_duplicate = _prior_is_already_processed() + [{
        "conversation_id": CONVERSATION,
        "message_id": "later-instruction",
        "action_key": "todo",
        "status": "IGNORED",
    }]
    processor = _processor(existing_actions=source_duplicate)
    plans = processor.process_batch(messages)
    assert plans[1]["reason"] == "source_already_processed"
    assert "context_followup_evidence" not in plans[1]

    model_calls = []
    resolver = JevV2Resolver(
        new_policy=True,
        transport=lambda *args, **kwargs: (model_calls.append((args, kwargs)) or {}),
    )
    no_context = _processor(llm_resolver=resolver, existing_actions=[])
    no_prior = no_context.process_batch([_messages()[1]])[0]
    assert no_prior["decision"] == "REVIEW"
    assert no_prior["reason"] == "contextual_retrieval_context_unresolved"
    assert model_calls == []

    template = _processor().process_batch(_messages())[1]
    duplicate_business = _prior_is_already_processed() + [{
        "conversation_id": CONVERSATION,
        "message_id": "older-retrieval-task",
        "business_key": template["business_key"],
        "route_module": template["route_module"],
        "route_owner": template["route_owner"],
        "status": "EXTERNAL_CREATED",
        "external_task_id": "existing-task",
        "source_text": "此前已安排下次开会时取回铁炉西送检卡尺。",
        "message_time": "2026-10-07 10:00:00",
    }]
    duplicate_processor = _processor(existing_actions=duplicate_business)
    duplicate = duplicate_processor.process_batch(_messages())[1]
    assert duplicate["decision"] == "REVIEW"
    assert duplicate["reason"] == "contextual_followup_candidates_present"


def test_complete_candidate_query_with_a_route_candidate_fails_closed():
    from jev_v2 import JevV2Resolver

    candidate = {
        "business_key": "another-contextual-task",
        "route_module": "workgroup_follow_up",
        "route_owner": SELF["name"],
        "status": "EXTERNAL_CREATED",
        "external_task_id": "existing-task",
        "source_text": "请在下次会议安排工作事项。",
        "message_time": "2026-10-07 10:00:00",
    }
    resolver_calls = []
    resolver = JevV2Resolver(
        new_policy=True,
        transport=lambda *args, **kwargs: (resolver_calls.append((args, kwargs)) or {}),
    )
    processor = _processor(
        existing_actions=_prior_is_already_processed() + [candidate],
        llm_resolver=resolver,
    )

    plan = processor.process_batch(_messages())[1]

    assert plan["decision"] == "REVIEW"
    assert plan["reason"] == "contextual_followup_candidates_present"
    assert plan["context_followup_evidence"]["prior_source"]["message_id"] == "prior-status"
    assert resolver_calls == []


def test_linked_contextual_retrieval_is_recalled_for_a_later_distinct_source():
    from event_worker import recall_business_candidates

    source_text = "未取回的下次开会时去取。"
    title = "下次开会时取回铁炉西送检卡尺"
    runtime = _runtime()
    action = {
        "conversation_id": CONVERSATION,
        "message_id": "older-retrieval-source",
        "business_key": None,
        "status": "EXTERNAL_CREATED",
        "external_task_id": "native-linked-task",
        "created_at": "2026-10-07 09:01:00",
    }

    class FakeAdapter:
        def query(self, request):
            assert request == {
                "view": "messages", "limit": 100,
                "conversation_id": CONVERSATION, "message_id": "older-retrieval-source",
            }
            return {"rows": [{
                "conversation_id": CONVERSATION,
                "message_id": "older-retrieval-source",
                "group_name": "铁炉西工班群",
                "sender_id": "opaque-message-sender",
                "sender_name": "报告人",
                "message_time": "2026-10-07 09:00:00",
                "text": source_text,
            }], "has_more": False}

    class FakeDws:
        def __init__(self):
            self.calls = []

        def get_todo(self, task_id):
            self.calls.append(task_id)
            return {
                "taskId": "native-linked-task",
                "subject": title,
                "isDone": False,
                "executorInfos": [{"name": SELF["name"], "userId": 333}],
                "executorIds": ["333"],
                "dueTime": 0,
            }

    dws = FakeDws()
    candidates, issue = load_business_candidate_context(
        FakeAdapter(), runtime,
        {"name": SELF["name"], "user_id": "self-user"}, [action], dws=dws,
    )
    assert issue is None
    assert dws.calls == ["native-linked-task"]
    assert len(candidates) == 1
    assert candidates[0]["business_key"] is None
    assert candidates[0]["route_module"] == "workgroup_follow_up"
    assert candidates[0]["route_owner"] == SELF["name"]
    assert candidates[0]["title"] == title

    processor = _processor(existing_actions=_prior_is_already_processed() + candidates)
    repeated = processor.process_batch(
        _messages(later_text="未取回的下次开会时去取。")
    )[1]

    assert repeated["decision"] == "REVIEW"
    assert repeated["reason"] == "contextual_followup_candidates_present"

    recall_plan = {
        "route_module": candidates[0]["route_module"],
        "route_owner": candidates[0]["route_owner"],
        "business_key": "different-semantic-business",
    }
    recalled, overflow = recall_business_candidates(recall_plan, candidates)
    assert overflow is False
    assert recalled == [{
        "key": "business_0",
        "text": candidates[0]["source_text"][:1600],
        "create_time": candidates[0]["message_time"][:64],
    }]
    assert "native-linked-task" not in str(recalled)


def test_linked_business_recall_uses_external_identity_and_keeps_key_guards():
    from event_worker import recall_business_candidates

    plan = {
        "route_module": "workgroup_follow_up",
        "route_owner": SELF["name"],
        "business_key": "different-semantic-business",
    }
    base = {
        "route_module": "workgroup_follow_up",
        "route_owner": SELF["name"],
        "status": "SUCCESS",
        "source_text": "A linked source with useful text.",
        "message_time": "2026-10-07 09:00:00",
    }

    null_key = {**base, "business_key": None, "external_task_id": "external-1"}
    empty_key = {**base, "business_key": "", "external_task_id": "external-2"}
    recalled, overflow = recall_business_candidates(plan, [null_key, empty_key])
    assert overflow is False
    assert len(recalled) == 2
    assert [candidate["key"] for candidate in recalled] == ["business_0", "business_1"]
    assert all("external-" not in str(candidate) for candidate in recalled)

    missing_link = {**base, "business_key": None, "external_task_id": ""}
    assert recall_business_candidates(plan, [missing_link])[0] == []

    unkeyed_simulation = {
        **base,
        "business_key": None,
        "simulated": True,
        "simulation_id": "simulation-1",
        "external_task_id": "",
    }
    assert recall_business_candidates(
        plan, [unkeyed_simulation], allow_simulated=True
    )[0] == []

    duplicate_external_id = [
        {**base, "business_key": None, "external_task_id": "external-duplicate", "message_id": "source-1"},
        {**base, "business_key": None, "external_task_id": "external-duplicate", "message_id": "source-2"},
    ]
    recalled, overflow = recall_business_candidates(plan, duplicate_external_id)
    assert overflow is False
    assert len(recalled) == 1


def test_local_contextual_create_is_claimed_once_linked_read_back_and_suppressed_on_restart(tmp_path):
    from jev_v2 import JevV2Resolver

    class FakeWriter:
        def __init__(self):
            self.requests = []
            self.claimed = set()

        def execute(self, request):
            self.requests.append(dict(request))
            if request["operation"] == "claim_action":
                key = (request["conversation_id"], request["message_id"], request["action_key"])
                if key in self.claimed:
                    return {"created": False, "action_id": 71, "reason": "source_exists"}
                self.claimed.add(key)
                return {"created": True, "action_id": 71}
            return {"ok": True, "committed": True, "action_id": request.get("action_id"),
                    "row": {"status": request.get("status"), "external_task_id": request.get("external_task_id")}}

    class FakeDws:
        def __init__(self):
            self.calls = []
            self.plan = None

        def search_todos(self, query):
            self.calls.append(("search_todos", query))
            return []

        def resolve_exact(self, names):
            self.calls.append(("resolve_exact", list(names)))
            return {name: "live-self" for name in names}

        def create_todo(self, plan, priority=20):
            self.calls.append(("create_todo", plan["title"], priority))
            self.plan = dict(plan)
            return {"task_id": "remote-context-task"}

        def add_source_comment(self, task_id, comment):
            self.calls.append(("add_source_comment", task_id, comment))

        def verify_source_comment(self, task_id, comment):
            self.calls.append(("verify_source_comment", task_id, comment))

        def download_message_resource(self, **kwargs):
            self.calls.append(("download_message_resource", kwargs["resource_id"]))
            download_dir = Path(kwargs["workdir"])
            download_dir.mkdir(parents=True, exist_ok=True)
            attachment_path = download_dir / "synthetic-fixture.xlsx"
            attachment_path.write_bytes(b"synthetic attachment bytes")
            return {"path": str(attachment_path), "file_name": attachment_path.name}

        def add_attachment(self, task_id, path):
            self.calls.append(("add_attachment", task_id, path))

        def verify_attachment(self, task_id, file_name):
            self.calls.append(("verify_attachment", task_id, file_name))

        def get_todo(self, task_id):
            self.calls.append(("get_todo", task_id))
            return {
                "taskId": task_id,
                "subject": self.plan["title"],
                "isDone": False,
                "executorInfos": [{"name": SELF["name"], "userId": SELF["user_id"]}],
                "priority": self.plan["priority"],
            }

    model_calls = []
    resolver = JevV2Resolver(
        new_policy=True,
        transport=lambda *args, **kwargs: (model_calls.append((args, kwargs)) or {}),
    )
    writer, dws = FakeWriter(), FakeDws()
    first = _processor(
        llm_resolver=resolver,
        mode="production",
        write_ledger=True,
        allow_external_writes=True,
        writer=writer,
        dws=dws,
            resource_dir=str(tmp_path / "protected-resources"),
    ).process_batch(_messages())
    second = _processor(
        llm_resolver=resolver,
        mode="production",
        write_ledger=True,
        allow_external_writes=True,
        writer=writer,
        dws=dws,
            resource_dir=str(tmp_path / "protected-resources"),
    ).process_batch(_messages())

    assert first[1]["decision"] == second[1]["decision"] == "CREATE"
    operations = [request["operation"] for request in writer.requests]
    assert operations.count("claim_action") == 2
    assert operations.count("link_external_task") == 1
    assert dws.calls.count(("create_todo", "下次开会时取回铁炉西送检卡尺", 20)) == 1
    assert sum(call[0] == "get_todo" for call in dws.calls) == 1
    assert sum(call[0] == "verify_source_comment" for call in dws.calls) == 1
    assert sum(call[0] == "verify_attachment" for call in dws.calls) == 1
    assert model_calls == []

    import hashlib
    import json
    import stat
    ledger_path = Path(tmp_path / "protected-resources" / "resource-download-ledger.jsonl")
    rows = [json.loads(line) for line in ledger_path.read_text(encoding="utf-8").splitlines()]
    assert [row["status"] for row in rows] == ["download_verified", "attachment_verified"]
    assert rows[0]["sha256"] == hashlib.sha256(b"synthetic attachment bytes").hexdigest()
    assert rows[0]["size_bytes"] == len(b"synthetic attachment bytes")
    assert rows[0]["source_message_id"] == "later-instruction"
    assert rows[0]["source_conversation_id"] == CONVERSATION
    assert stat.S_IMODE(ledger_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(ledger_path.parent.stat().st_mode) == 0o700
    downloaded_path = ledger_path.parent / rows[0]["relative_path"]
    assert stat.S_IMODE(downloaded_path.stat().st_mode) == 0o600


@pytest.mark.parametrize("item", ["卡尺", "砝码"])
def test_mixed_workgroup_notice_uses_explicit_local_prefix_without_self_sender_mapping(item):
    from jev_v2 import JevV2Resolver

    conversation = f"configured-conversation-{uuid4().hex}"
    prior_id = f"source-{uuid4().hex}"
    other_status_id = f"source-{uuid4().hex}"
    another_status_id = f"source-{uuid4().hex}"
    later_id = f"source-{uuid4().hex}"
    report_sender = f"message-contact-{uuid4().hex}"
    other_sender = f"message-contact-{uuid4().hex}"
    notice_sender = f"message-contact-{uuid4().hex}"
    contact_user_id = f"contact-user-{uuid4().hex}"
    base = datetime.now().replace(microsecond=0) - timedelta(minutes=10)
    times = [base + timedelta(minutes=offset) for offset in (0, 2, 3, 4)]
    mixed_group = "综合工作协调群（多工班）"
    notice = (
        "目前系统上显示近期游标卡尺需要送检的2个，无需填表，等开会了直接送计量室就行。"
        "未取回的下次开会时去取。"
        "之前反馈称没送的：@其他人员 填写这个表后，等通知再送检，预计是下周让送，"
        "其他工班的称没送的也可以填写"
    )
    prior_resource = {"name": "检定回执.pdf", "resourceId": f"resource-{uuid4().hex}"}
    notice_resource = {"name": "计量工作安排.xlsx", "resourceId": f"resource-{uuid4().hex}"}
    messages = [
        {
            "conversation_id": conversation,
            "message_id": prior_id,
            "create_time": times[0].strftime("%Y-%m-%d %H:%M:%S"),
            "sender_id": report_sender,
            "sender": "报告人甲",
            "text": f"铁炉西{item}送检后暂未取回",
            "resource_refs": [prior_resource],
        },
        {
            "conversation_id": conversation,
            "message_id": other_status_id,
            "create_time": times[1].strftime("%Y-%m-%d %H:%M:%S"),
            "sender_id": other_sender,
            "sender": "报告人乙",
            "text": "郑东工班压力表已送检，尚未取回。",
        },
        {
            "conversation_id": conversation,
            "message_id": another_status_id,
            "create_time": times[2].strftime("%Y-%m-%d %H:%M:%S"),
            "sender_id": f"message-contact-{uuid4().hex}",
            "sender": "报告人丁",
            "text": "总库工班温度计已送检，尚未取回。",
        },
        {
            "conversation_id": conversation,
            "message_id": later_id,
            "create_time": times[3].strftime("%Y-%m-%d %H:%M:%S"),
            "sender_id": notice_sender,
            "sender": "通知人丙",
            "text": notice,
            "resource_refs": [notice_resource],
        },
    ]
    runtime = _runtime(mixed_group)
    runtime["config_group"][0]["conversation_id"] = conversation
    model_calls = []
    resolver = JevV2Resolver(
        new_policy=True,
        transport=lambda *args, **kwargs: (model_calls.append((args, kwargs)) or {}),
    )
    processor = EventProcessor(
        runtime,
        {"name": SELF["name"], "user_id": contact_user_id},
        existing_actions=[{
            "conversation_id": conversation,
            "message_id": prior_id,
            "action_key": "todo",
            "status": "IGNORED",
        }, {
            "conversation_id": conversation,
            "message_id": other_status_id,
            "action_key": "todo",
            "status": "IGNORED",
        }, {
            "conversation_id": conversation,
            "message_id": another_status_id,
            "action_key": "todo",
            "status": "IGNORED",
        }],
        business_candidate_query_complete=True,
        source_order_provenance="complete_dws_chronological_page",
        llm_resolver=resolver,
    )

    plans = processor.process_batch(messages)

    followup = plans[-1]
    assert followup["decision"] == "CREATE"
    assert followup["reason"] == "contextual_retrieval_followup"
    assert followup["title"] == f"下次开会时取回铁炉西送检{item}"
    assert followup.get("due_date") is None
    assert followup.get("due_iso") is None
    assert followup.get("due_time") is None
    evidence = followup["context_followup_evidence"]
    assert evidence["prior_source"]["message_id"] == prior_id
    assert evidence["prior_source"]["sender_id"] == report_sender
    assert evidence["prior_source"]["text"] == f"铁炉西{item}送检后暂未取回"
    assert evidence["prior_source"]["resource_refs"] == [prior_resource]
    assert evidence["instruction_source"]["message_id"] == later_id
    assert evidence["instruction_source"]["sender_id"] == notice_sender
    assert evidence["instruction_source"]["text"] == notice
    assert evidence["instruction_source"]["resource_refs"] == [notice_resource]
    assert evidence["instruction_source"]["evidence_span"] == "未取回的下次开会时去取"
    assert "填写这个表" not in followup["title"]
    assert "送计量室" not in followup["title"]
    assert sum(plan["decision"] == "CREATE" for plan in plans) == 1
    assert model_calls == []
    assert processor.stats["llm_calls"] == 0


def test_unprefixed_context_requires_verified_sender_in_a_configured_local_group():
    messages = _messages()
    messages[0]["text"] = "卡尺已送检，尚未取回。"
    local_group_plan = EventProcessor(
        _runtime("铁炉西工班群"),
        dict(SELF),
        existing_actions=_prior_is_already_processed(),
        business_candidate_query_complete=True,
        source_order_provenance="complete_dws_chronological_page",
    ).process_batch(messages)[1]
    assert local_group_plan["reason"] == "contextual_retrieval_followup"

    mixed_runtime = _runtime("综合工作协调群（多工班）")
    mixed_group_plan = EventProcessor(
        mixed_runtime,
        dict(SELF),
        existing_actions=_prior_is_already_processed(),
        business_candidate_query_complete=True,
        source_order_provenance="complete_dws_chronological_page",
    ).process_batch(messages)[1]
    assert mixed_group_plan["decision"] == "REVIEW"
    assert mixed_group_plan.get("reason") != "contextual_retrieval_followup"
