"""Fake-only integration coverage for the supervised V2 production bridge."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import event_worker
from dingtalk_reader import DingTalkReader
from dws_adapter import DwsClient
from event_worker import EventProcessor, OneShotHistoryPoller, main
from jev_v2 import JevV2Resolver
from llm_adapter import DateRoleFallbackResolver, SemanticFallbackResolver, TitleFieldResolver


TZ = timezone(timedelta(hours=8))
FLOOR = datetime(2026, 10, 8, 0, 0, tzinfo=TZ)
NOW = datetime(2026, 10, 8, 9, 0, tzinfo=TZ)

LONG_SOURCE = (
    "2026年9月22日至29日处置危废废物\n"
    "郑州市轨道交通2026-2027年度废油处置合同第9批处置，非郑东工班联单编号：20264101142065，"
    "联单时间：2026年9月24日；郑东工班联单编号：20264101143281，联单时间：2026年9月29日。\n"
    "郑州市轨道交通2026年危险废弃物（废空容器、废油滤芯等）处置合同第6批处置，"
    "非郑东工班联单编号：20264101142066，联单时间：2026年9月24日；"
    "郑东工班联单编号：20264101143286，联单时间：2026年9月29日。\n"
    "请各工班及时在系统中录入联单信息并反馈归档完成情况。"
)


def _runtime():
    return {
        "config_calendar": [
            {"dt": "2026-10-08", "day_type": "workday"},
            {"dt": "2026-10-09", "day_type": "workday"},
        ],
        "config_group": [
            {"group_name": "危废、废旧工作群", "conversation_id": "conv-1", "purpose": "todo_scan"}
        ],
        "config_staff": [
            [1, "李林骁", "SELF", "leader"],
            [2, "杨梦卓", "CONFIG-OWNER", "executor"],
        ],
        "active_routing_loaded": True,
        "responsibility_rules": {"hazardous_waste": "杨梦卓"},
    }


def _jev_transport(calls, *, business_value=0.50):
    def transport(request, _timeout):
        calls.append(request)
        answers = {}
        for key, question in request["questions"].items():
            if question["type"] == "noul":
                if key.startswith("same_work_"):
                    value = business_value
                else:
                    value = {
                        "applies_to_workgroup": 0.66,
                        "needs_follow_up": 0.93,
                        "prospective_action": 0.97,
                    }[key]
                answers[key] = {"type": "noul", "noul": value}
                continue
            options = question["criteria"]
            if key.startswith("date_role_"):
                selected, confidence = "unknown", 0.55
            else:
                selected = "none" if "none" in options else next(iter(options))
                confidence = 0.99
            remainder = (1.0 - confidence) / (len(options) - 1)
            answers[key] = {
                "type": "choice",
                "choice": selected,
                "probabilities": {
                    option: confidence if option == selected else remainder for option in options
                },
                "confidence": confidence,
            }
        return {"answers": answers}

    return transport


class _NativeHarness:
    def __init__(self):
        self.calls = []
        self.factory_options = []

    def factory(self, **kwargs):
        self.factory_options.append(kwargs)
        harness = self

        class Agent:
            _persist_disabled = True

            def run_conversation(self, user_message, *, system_message, conversation_history):
                context = json.loads(user_message)
                if "same_work_candidates" in context:
                    kind = "semantic"
                    result = {
                        "applicability": "applicable",
                        "followup": "required",
                        "title_span": "title_0",
                        "due_key": "none",
                        "same_work": {
                            row["key"]: "different" for row in context["same_work_candidates"]
                        },
                        "evidence_spans": ["请各工班及时在系统中录入联单信息"],
                    }
                elif "unknown_date_candidates" in context:
                    kind = "date"
                    result = {
                        "roles": [
                            {
                                "key": row["key"],
                                "role": "historical",
                                "evidence": row["source_span"],
                            }
                            for row in context["unknown_date_candidates"]
                        ]
                    }
                else:
                    kind = "title"
                    result = {
                        "title": "补录并核对危废联单信息",
                        "evidence_spans": ["及时在系统中录入联单信息"],
                    }
                harness.calls.append((kind, context, system_message, conversation_history))
                return {
                    "completed": True,
                    "final_response": json.dumps(result, ensure_ascii=False),
                }

        return Agent()


class _Store:
    def __init__(self, *, failure=None):
        self.failure = failure
        self.messages = {}
        self.actions = []
        self.assignment_audits = {}
        self.tasks = {}
        self.operations = []
        self.next_action_id = 1


class _FakeAdapter:
    def __init__(self, store):
        self.store = store
        self.requests = []

    def query(self, request):
        request = dict(request)
        self.requests.append(request)
        view = request["view"]
        if view == "runtime_config":
            return {"config": _runtime()}
        if view == "scan_state":
            return {
                "rows": [{
                    "conversation_id": "conv-1",
                    "group_name": "stale-display-name",
                    "last_msg_time": "2026-09-30 12:00:00",
                    "last_msg_id": "old-cursor-id",
                }],
                "has_more": False,
            }
        if view == "actions":
            rows = [row for row in self.store.actions if not request.get("conversation_id") or
                    row.get("conversation_id") == request["conversation_id"]]
            return {"rows": rows, "has_more": False}
        if view == "messages":
            rows = [row for row in self.store.messages.values()
                    if all(row.get(key) == request.get(key) for key in ("conversation_id", "message_id")
                           if request.get(key) is not None)]
            return {"rows": rows, "has_more": False}
        raise AssertionError(request)


class _FakeWriter:
    def __init__(self, store):
        self.store = store

    def execute(self, request):
        request = dict(request)
        self.store.operations.append(("ledger", request))
        operation = request["operation"]
        if operation == "record_message":
            key = (request["conversation_id"], request["message_id"])
            self.store.messages[key] = {
                "conversation_id": request["conversation_id"],
                "message_id": request["message_id"],
                "group_name": request["group_name"],
                "sender_id": request.get("sender_id"),
                "sender_name": request.get("sender_name"),
                "message_time": request.get("message_time"),
                "text": request.get("text"),
            }
            return {"recorded": True}
        if operation == "claim_action":
            if self.store.failure == "claim":
                row = {
                    "action_id": f"ACTION-{self.store.next_action_id}",
                    "conversation_id": request["conversation_id"],
                    "message_id": request["message_id"],
                    "business_key": request.get("business_key"),
                    "status": "CLAIMED",
                }
                self.store.next_action_id += 1
                self.store.actions.append(row)
                return {"created": False, "action_id": row["action_id"]}
            existing = next((row for row in self.store.actions
                             if row.get("conversation_id") == request["conversation_id"]
                             and row.get("message_id") == request["message_id"]), None)
            if existing:
                return {"created": False, "action_id": existing["action_id"]}
            row = {
                "action_id": f"ACTION-{self.store.next_action_id}",
                "conversation_id": request["conversation_id"],
                "message_id": request["message_id"],
                "business_key": request.get("business_key"),
                "status": request["status"],
            }
            self.store.next_action_id += 1
            self.store.actions.append(row)
            return {"created": True, "action_id": row["action_id"]}
        if operation == "link_external_task":
            if self.store.failure == "link":
                raise RuntimeError("fake_link_failure")
            row = next(row for row in self.store.actions if row["action_id"] == request["action_id"])
            row["status"] = request["status"]
            row["external_task_id"] = request["external_task_id"]
            return {"ok": True, "committed": True, "action_id": request["action_id"], "row": {"external_task_id": request["external_task_id"]}, "linked": True}
        if operation == "save_action_result":
            row = next(row for row in self.store.actions if row["action_id"] == request["action_id"])
            row["status"] = request["status"]
            row["last_error"] = request.get("last_error")
            return {"ok": True, "committed": True, "action_id": request["action_id"],
                    "row": {"status": row["status"]}, "saved": True}
        if operation == "save_assignment_audit":
            self.store.assignment_audits[request["action_id"]] = request["assignment_audit"]
            return {"saved": True}
        if operation == "advance_scan_state":
            self.store.operations.append(("cursor", request))
            return {"advanced": True}
        raise AssertionError(request)


class _FakeDws(DwsClient):
    def __init__(self, store, *, failure=None):
        super().__init__(confirmed_official_writes=True)
        self.store = store
        self.failure = failure or store.failure
        self.calls = []
        self.task = None

    def get_self(self):
        return {"name": "李林骁", "user_id": "SELF"}

    def resolve_exact(self, names):
        names = list(names)
        self.calls.append(("resolve_exact", names))
        identities = {"苗笑天": "LIVE-MIAO", "李林骁": "SELF"}
        return {name: identities[name] for name in names}

    def run(self, args):
        args = list(args)
        self.calls.append(("run", args))
        self.store.operations.append(("run", args))
        if args[:2] == ["todo", "+search"]:
            # A complete, empty search result lets this pipeline test exercise
            # creation/readback without pretending the local ledger proves a
            # matching remote todo exists.
            return {
                "ok": True,
                "outcome": "success",
                "data": {"complete": True, "count": 0, "todos": []},
            }
        if args[:3] == ["todo", "task", "create"]:
            if self.failure == "create_timeout":
                raise TimeoutError("fake_create_timeout")
            title = args[args.index("--title") + 1]
            executor_ids = args[args.index("--executors") + 1].split(",")
            due_iso = args[args.index("--due") + 1] if "--due" in args else None
            self.task = {
                "task_id": "TASK-1",
                "subject": title,
                "due_iso": due_iso,
                "executor_ids": list(executor_ids),
                "executor_names": [
                    "苗笑天" if value == "LIVE-MIAO" else "李林骁"
                    for value in executor_ids
                ],
            }
            self.store.tasks[self.task["task_id"]] = dict(self.task)
            return {"result": {"data": {"taskId": self.task["task_id"]}}}
        if args[:2] == ["todo", "+comment"]:
            self.store.operations.append(("comment", args))
            content = args[args.index("--content") + 1]
            return {"ok": True, "outcome": "success", "data": {
                "taskId": args[args.index("--task-id") + 1],
                "commentId": "COMMENT-1",
                "content": content,
                "verified": True,
            }}
        if args[:3] == ["todo", "comment", "list"]:
            comments = [
                {"content": values[values.index("--content") + 1]}
                for name, values in self.store.operations if name == "comment"
                for _ in [0]
            ]
            return {"result": {"comments": comments}}
        if args[:3] == ["todo", "task", "get"]:
            task_id = args[args.index("--task-id") + 1]
            task = self.store.tasks.get(task_id)
            if task is None:
                raise AssertionError("read-back without a fake created task")
            due_time = None
            if task["due_iso"]:
                due_time = int(datetime.fromisoformat(task["due_iso"]).timestamp() * 1000)
            subject = "mismatched" if self.failure == "readback" else task["subject"]
            return {"result": {
                "taskId": task_id,
                "subject": subject,
                "isDone": False,
                "priority": 20,
                "dueTime": due_time,
                "executorInfos": [
                    {"name": name, "userId": user_id}
                    for name, user_id in zip(task["executor_names"], task["executor_ids"])
                ],
            }}
        raise AssertionError(args)


class _FakeReader:
    def __init__(self, messages, *, complete=True):
        self.messages = list(messages)
        self.complete = complete
        self.calls = []

    def read_group(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "complete": self.complete,
            "source_order_provenance": "complete_dws_chronological_page",
            "messages": list(self.messages),
        }


def _message(message_id="new-source", *, create_time="2026-10-08 09:00:00", text=LONG_SOURCE):
    return {
        "conversation_id": "conv-1",
        "message_id": message_id,
        "group": "危废、废旧工作群",
        "sender": "来源发送人",
        "sender_id": "EXTERNAL-SENDER",
        "create_time": create_time,
        "text": text,
        "resource_refs": [],
    }


def _seed_business_candidate(store):
    store.messages[("conv-1", "prior-source")] = {
        "conversation_id": "conv-1",
        "message_id": "prior-source",
        "group_name": "危废、废旧工作群",
        "sender_id": "ANOTHER-SENDER",
        "sender_name": "既有来源",
        "message_time": "2026-10-07 09:00:00",
        "text": "核对既有危废联单信息并反馈归档完成情况。",
    }
    store.actions.append({
        "action_id": "PRIOR-ACTION",
        "conversation_id": "conv-1",
        "message_id": "prior-source",
        "business_key": "prior-business-key",
        "external_task_id": "PRIOR-TASK",
        "status": "EXTERNAL_CREATED",
        "created_at": "2026-10-07 09:01:00",
    })


def _poller(store, reader, *, now=NOW, failure=None, cutoff=FLOOR, resolver_calls=None,
            native=None, processor_factory=EventProcessor, business_value=0.50):
    adapter = _FakeAdapter(store)
    writer = _FakeWriter(store)
    dws = _FakeDws(store, failure=failure)
    resolver_calls = resolver_calls if resolver_calls is not None else []
    resolver = JevV2Resolver(
        new_policy=True,
        transport=_jev_transport(resolver_calls, business_value=business_value),
    )
    semantic = SemanticFallbackResolver(agent_factory=native.factory) if native else None
    date_role = DateRoleFallbackResolver(agent_factory=native.factory) if native else None
    title = TitleFieldResolver(agent_factory=native.factory) if native else None
    poller = OneShotHistoryPoller(
        adapter=adapter,
        reader=reader,
        dws=dws,
        writer=writer,
        resolver=resolver,
        semantic_fallback=semantic,
        date_role_fallback_resolver=date_role,
        title_resolver=title,
        mode="production",
        write_ledger=True,
        allow_external_writes=True,
        supervised_rollout=True,
        not_before=cutoff,
        processor_factory=processor_factory,
        now_factory=lambda: now,
    )
    return poller, adapter, writer, dws, resolver_calls


def test_one_shot_supervised_v2_creates_once_and_keeps_validated_native_fields():
    store = _Store()
    _seed_business_candidate(store)
    stale = _message("pre-cutoff-source", create_time="2026-10-07 17:59:00")
    current = _message()
    reader = _FakeReader([stale, current])
    native = _NativeHarness()
    poller, adapter, _writer, dws, jev_calls = _poller(
        store, reader, native=native,
    )

    result = poller.run_once()

    assert result["status"] == "COMPLETED"
    assert result["initial_cutoff"] == "2026-10-08T00:00:00+08:00"
    assert result["messages"] == 1
    assert [plan["message_id"] for plan in result["plans"]] == ["new-source"]
    plan = result["plans"][0]
    assert plan["decision"] == "CREATE"
    assert plan["title"] == "补录并核对危废联单信息"
    assert plan.get("due_date") is None
    assert plan.get("due_iso") is None
    assert plan["date_role_fallback_evidence"]["roles"]
    assert all(row["role"] == "historical" for row in plan["date_role_fallback_evidence"]["roles"])
    assert plan["executor_names"] == ["苗笑天", "李林骁"]
    assert plan["executor_ids"] == ["LIVE-MIAO", "SELF"]
    assert plan["external_task_id"] == "TASK-1"
    assert [kind for kind, *_ in native.calls] == ["semantic", "date", "title"]
    assert all(options.get("enabled_toolsets") == [] for options in native.factory_options)
    assert all(options.get("max_iterations") == 1 for options in native.factory_options)
    assert all(options.get("save_trajectories") is False for options in native.factory_options)
    assert len(jev_calls) == 1
    assert all(call.get("not_before") == FLOOR for call in reader.calls)
    assert not any(
        req.get("message_id") == "pre-cutoff-source"
        for name, req in store.operations if name == "ledger"
    )

    action = next(row for row in store.actions if row.get("message_id") == "new-source")
    assert action["status"] == "EXTERNAL_CREATED"
    assert action["external_task_id"] == "TASK-1"
    audit = store.assignment_audits[action["action_id"]]
    assert audit["rule_version"] == "confirmed-assignment-habits/2026-10-18.1"
    assert audit["primary_owner"] == "苗笑天"
    assert audit["collaborators"] == ["李林骁"]
    assert audit["awareness_only"] is False
    assert audit["manual_assignment_required"] is False
    assert "text" not in audit and "raw_payload" not in audit
    operations = [
        (name, request.get("operation") if isinstance(request, dict) else request[:3])
        for name, request in store.operations
    ]
    claim_index = operations.index(("ledger", "claim_action"))
    create_index = operations.index(("run", ["todo", "task", "create"]))
    link_index = operations.index(("ledger", "link_external_task"))
    comment_index = operations.index(("run", ["todo", "+comment", "--task-id"]))
    comment_readback_index = operations.index(("run", ["todo", "comment", "list"]))
    readback_index = operations.index(("run", ["todo", "task", "get"]))
    assert claim_index < create_index < link_index < comment_index < comment_readback_index < readback_index
    assert not any("send" in str(call).lower() and "comment" not in str(call).lower() for call in dws.calls)

    restarted_reader = _FakeReader([current])
    restarted, _adapter2, _writer2, dws2, jev_calls2 = _poller(
        store,
        restarted_reader,
        now=datetime(2026, 10, 9, 9, 0, tzinfo=TZ),
        native=_NativeHarness(),
        resolver_calls=jev_calls,
    )
    before = len(jev_calls)
    replay = restarted.run_once()
    assert replay["status"] == "COMPLETED"
    assert replay["stats"]["external_todos_created"] == 0
    assert len(jev_calls) == before
    assert sum(1 for name, value in store.operations
               if name == "run" and value[:3] == ["todo", "task", "create"]) == 1
    assert not dws2.calls or all(call[0] != "run" or call[1][:3] != ["todo", "task", "create"] for call in dws2.calls)


def test_self_default_creates_hazard_task_only_for_current_user_and_replay_is_idempotent():
    store = _Store()
    _seed_business_candidate(store)
    source = _message()
    native = _NativeHarness()
    with patch.dict("os.environ", {"DINGTODO_ASSIGN_SELF_ONLY": "1"}):
        poller, _adapter, _writer, dws, _calls = _poller(store, _FakeReader([source]), native=native)
        result = poller.run_once()
        plan = result["plans"][0]
        assert plan["decision"] == "CREATE"
        assert plan["executor_names"] == ["李林骁"]
        assert plan["executor_ids"] == ["SELF"]
        assert plan["external_task_id"] == "TASK-1"
        assert not any(call[0] == "resolve_exact" and call[1] for call in dws.calls)
        action = next(row for row in store.actions if row.get("message_id") == "new-source")
        audit = store.assignment_audits[action["action_id"]]
        assert audit["primary_owner"] == "李林骁"
        assert audit["collaborators"] == []
        restarted, _adapter, _writer, _dws, _calls = _poller(
            store, _FakeReader([source]), now=datetime(2026, 10, 9, 9, 0, tzinfo=TZ), native=_NativeHarness(),
        )
        assert restarted.run_once()["stats"]["external_todos_created"] == 0
        assert sum(1 for name, value in store.operations
                   if name == "run" and value[:3] == ["todo", "task", "create"]) == 1


def test_self_default_holds_same_cross_group_notice_before_second_write():
    runtime = _runtime()
    runtime["config_group"] = [
        {"group_name": "台账更新群", "conversation_id": "conv-1", "purpose": "todo_scan"},
        {"group_name": "工班长交流群", "conversation_id": "conv-2", "purpose": "todo_scan"},
    ]
    calls = []
    underlying = _jev_transport(calls)

    def confident_transport(request, timeout):
        response = underlying(request, timeout)
        for name in ("applies_to_workgroup", "needs_follow_up", "prospective_action"):
            if name in response["answers"]:
                response["answers"][name]["noul"] = 0.99
        return response

    notice = "会议通知：请各工班在一号会议室及时参加安全工作例会。"
    messages = [
        {"conversation_id": "conv-1", "message_id": "one", "sender_id": "sender",
         "sender": "通知人", "create_time": "2026-10-08 09:00:00", "text": notice + "@李林骁(李林骁)"},
        {"conversation_id": "conv-2", "message_id": "two", "sender_id": "sender",
         "sender": "通知人", "create_time": "2026-10-08 09:00:47", "text": notice + "@所有人"},
    ]
    with patch.dict("os.environ", {"DINGTODO_ASSIGN_SELF_ONLY": "1"}):
        processor = EventProcessor(
            runtime, {"name": "李林骁", "user_id": "SELF"}, mode="shadow",
            write_ledger=False, allow_external_writes=False,
            llm_resolver=JevV2Resolver(new_policy=True, transport=confident_transport),
            existing_actions=[], business_candidate_query_complete=True,
            source_order_provenance="complete_dws_chronological_page",
        )
        plans = processor.process_batch(messages)
    assert plans[0]["decision"] == "CREATE"
    assert plans[0]["executor_names"] == ["李林骁"]
    assert plans[1]["decision"] == "REVIEW"
    assert plans[1]["reason"] == "cross_group_source_possible_duplicate"
    assert processor.stats["external_todos_created"] == 0


def test_clear_source_deadline_reaches_task_due_time_readback():
    store = _Store()
    _seed_business_candidate(store)
    message = _message(
        "deadline-source",
        text="请各工班于2026年10月8日前完成危废联单核对并反馈。",
    )
    poller, _adapter, _writer, dws, calls = _poller(
        store,
        _FakeReader([message]),
        business_value=0.01,
    )

    result = poller.run_once()

    plan = result["plans"][0]
    assert plan["decision"] == "CREATE"
    assert plan["due_iso"] == "2026-10-08T18:00:00+08:00"
    create_args = next(
        args for kind, args in dws.calls
        if kind == "run" and args[:3] == ["todo", "task", "create"]
    )
    assert create_args[create_args.index("--due") + 1] == plan["due_iso"]
    assert any(
        kind == "run" and args[:3] == ["todo", "task", "get"]
        for kind, args in dws.calls
    )
    assert result["stats"]["external_todos_created"] == 1
    assert len(calls) == 1


@pytest.mark.parametrize("opaque_reference", [
    "https://oapi.dingtalk.com/card?auth_key=FAKE_TEST_CREDENTIAL",
    "dingtalk://chat/message?conversationId=FAKE_TEST_CONVERSATION&messageId=FAKE_TEST_MESSAGE",
])
def test_one_shot_reviews_opaque_references_without_any_provider_or_external_create(
    opaque_reference,
):
    store = _Store()
    native = _NativeHarness()
    message = _message(
        "opaque-source",
        text=f"请各工班完成危废联单核对并反馈。{opaque_reference}",
    )
    poller, _adapter, _writer, dws, jev_calls = _poller(
        store,
        _FakeReader([message]),
        native=native,
    )

    result = poller.run_once()

    plan = result["plans"][0]
    assert plan["decision"] == "REVIEW"
    assert plan["jev_error_code"] == "jev_redaction_incomplete"
    assert jev_calls == []
    assert native.calls == []
    assert not any(
        call[0] == "run" and call[1][:3] == ["todo", "task", "create"]
        for call in dws.calls
    )


@pytest.mark.parametrize("failure,expected_creates", [
    ("claim", 0),
    ("create_timeout", 1),
    ("link", 1),
    ("readback", 1),
])
def test_uncertain_claim_create_link_and_readback_failures_remain_restart_protected(
    failure, expected_creates,
):
    store = _Store(failure=failure)
    _seed_business_candidate(store)
    native = _NativeHarness()
    reader = _FakeReader([_message()])
    poller, _adapter, _writer, dws, _calls = _poller(
        store, reader, failure=failure, native=native,
    )

    first = poller.run_once()
    second_poller, _adapter2, _writer2, dws2, _calls2 = _poller(
        store, _FakeReader([_message()]), failure=failure, native=_NativeHarness(),
    )
    second = second_poller.run_once()

    creates = sum(1 for name, value in store.operations
                  if name == "run" and value[:3] == ["todo", "task", "create"])
    assert creates == expected_creates
    assert second["stats"]["external_todos_created"] == 0
    assert not any(call[0] == "run" and call[1][:3] == ["todo", "task", "create"] for call in dws2.calls)
    assert first["status"] in {"COMPLETED", "REVIEW"}
    assert all(row.get("external_task_id") == "TASK-1" for row in store.actions
               if row.get("message_id") == "new-source" and failure == "readback")
    assert not any("send" in str(call).lower() for call in dws.calls)


def test_poller_skips_future_cutoff_without_reading_and_preserves_cutoff_in_summary():
    store = _Store()
    reader = _FakeReader([_message()])
    poller, adapter, _writer, _dws, _calls = _poller(
        store,
        reader,
        now=datetime(2026, 10, 7, 9, 0, tzinfo=TZ),
        cutoff=FLOOR,
        native=_NativeHarness(),
    )

    result = poller.run_once()

    assert result["status"] == "SKIPPED"
    assert result["reason"] == "not_before_after_now"
    assert result["initial_cutoff"] == "2026-10-08T00:00:00+08:00"
    assert adapter.requests == []
    assert reader.calls == []


def test_incomplete_history_never_processes_or_advances_cursor():
    store = _Store()
    reader = _FakeReader([_message()], complete=False)
    poller, _adapter, _writer, _dws, jev_calls = _poller(
        store, reader, native=_NativeHarness(),
    )

    result = poller.run_once()

    assert result["status"] == "REVIEW"
    assert not jev_calls
    assert not any(name == "ledger" and request.get("operation") in {
        "record_message", "claim_action", "advance_scan_state"
    } for name, request in store.operations)


def test_reader_uses_max_of_cursor_overlap_and_typed_rollout_floor():
    from dingtalk_reader import parse_not_before

    floor = parse_not_before("2026-10-08T00:00:00+08:00")
    with patch("dingtalk_reader.subprocess.run") as run:
        run.return_value.returncode = 0
        run.return_value.stdout = json.dumps({"complete": True, "hasMore": False, "messages": []})
        result = DingTalkReader().read_group(
            conversation_id="conv-1",
            group_name="群",
            last_msg_time="2026-09-30 12:00:00",
            end=NOW,
            not_before=floor,
        )

    command = run.call_args.args[0]
    start = datetime.fromisoformat(command[command.index("--start") + 1])
    assert start == floor
    assert result["start"] == floor.isoformat()
    assert "2026-09-30" not in result["start"]


@pytest.mark.parametrize("value", [
    "2026-10-08T00:00:00Z",
    "2026-10-08T00:00:00+09:00",
    "2026-10-08 00:00:00+08:00",
    "2026-10-08T00:00:00",
])
def test_not_before_requires_rfc3339_with_explicit_china_offset(value):
    from dingtalk_reader import parse_not_before

    with pytest.raises(ValueError, match="not_before_must_be_rfc3339_plus_08"):
        parse_not_before(value)


def test_cli_help_and_constructor_plumbing_select_v2_and_native_adapters():
    with patch("sys.stdout.write") as output:
        with pytest.raises(SystemExit) as help_exit:
            main(["--once", "--help"])
    assert help_exit.value.code == 0
    help_text = "".join(call.args[0] for call in output.call_args_list)
    for flag in (
        "--jev-v2-policy", "--semantic-fallback", "--title-fields",
        "--date-role-fallback", "--supervised-rollout", "--not-before",
    ):
        assert flag in help_text

    with (
        patch("event_worker.WorkQueryAdapter"),
        patch("event_worker.DingTalkReader") as reader,
        patch("event_worker.DwsClient") as dws,
        patch("event_worker.WorkWriter"),
        patch("event_worker.configure_writer_environment"),
        patch("event_worker.JevV2Resolver", return_value="v2") as v2,
        patch("event_worker.SemanticFallbackResolver", return_value="semantic") as semantic,
        patch("event_worker.TitleFieldResolver", return_value="title") as title,
        patch("event_worker.DateRoleFallbackResolver", return_value="date") as date_role,
        patch("event_worker.OneShotHistoryPoller") as poller,
        patch("builtins.print"),
    ):
        poller.return_value.run_once.return_value = {"status": "COMPLETED"}
        status = main([
            "--once", "--mode", "production", "--write-ledger", "--allow-external-writes",
            "--jev-v2-policy", "--semantic-fallback", "--title-fields", "--date-role-fallback",
            "--supervised-rollout", "--not-before", "2026-10-08T00:00:00+08:00",
        ])

    assert status == 0
    v2.assert_called_once_with(new_policy=True)
    semantic.assert_called_once_with()
    title.assert_called_once_with()
    date_role.assert_called_once_with()
    kwargs = poller.call_args.kwargs
    assert kwargs["resolver"] == "v2"
    assert kwargs["semantic_fallback"] == "semantic"
    assert kwargs["title_resolver"] == "title"
    assert kwargs["date_role_fallback_resolver"] == "date"
    assert kwargs["supervised_rollout"] is True
    assert kwargs["not_before"] == FLOOR
    assert reader.called
    dws.assert_called_once_with(confirmed_official_writes=True)


@pytest.mark.parametrize(("args", "expected_authorization"), [
    (["--once", "--mode", "production", "--write-ledger"], False),
    (["--once", "--mode", "production", "--write-ledger", "--allow-external-writes"], True),
    (["--once", "--mode", "test", "--write-ledger"], False),
])
def test_main_authorizes_shortcut_writes_only_when_all_existing_gates_hold(args, expected_authorization):
    with (
        patch("event_worker.WorkQueryAdapter"),
        patch("event_worker.DingTalkReader"),
        patch("event_worker.DwsClient") as dws,
        patch("event_worker.WorkWriter"),
        patch("event_worker.configure_writer_environment"),
        patch("event_worker.OneShotHistoryPoller") as poller,
        patch("builtins.print"),
    ):
        poller.return_value.run_once.return_value = {"status": "COMPLETED"}
        assert main(args) == 0
    dws.assert_called_once_with(confirmed_official_writes=expected_authorization)


@pytest.mark.parametrize("args", [
    ["--once", "--llm-jev", "--jev-v2-policy"],
    ["--once", "--semantic-fallback"],
    ["--once", "--title-fields"],
    ["--once", "--date-role-fallback"],
    ["--once", "--supervised-rollout"],
    ["--once", "--mode", "production", "--jev-v2-policy", "--title-fields"],
])
def test_cli_rejects_incompatible_resolver_and_rollout_combinations_before_construction(args):
    with (
        patch("event_worker.WorkQueryAdapter") as adapter,
        patch("event_worker.DingTalkReader") as reader,
        patch("event_worker.DwsClient") as dws,
        patch("builtins.print"),
    ):
        with pytest.raises(SystemExit) as error:
            main(args)
    assert error.value.code == 2
    adapter.assert_not_called()
    reader.assert_not_called()
    dws.assert_not_called()


def test_processor_keeps_native_guards_and_shadow_only_human_evidence():
    config = _runtime()
    self_user = {"name": "李林骁", "user_id": "SELF"}
    semantic = SemanticFallbackResolver(agent_factory=lambda **_kwargs: None)
    date_role = DateRoleFallbackResolver(agent_factory=lambda **_kwargs: None)
    title = TitleFieldResolver(agent_factory=lambda **_kwargs: None)
    legacy = event_worker.JevResolver(api_key="fake", transport=lambda *_args: {})

    with pytest.raises(ValueError, match="date_role_fallback_requires_read_only_shadow"):
        EventProcessor(config, self_user, mode="production", llm_resolver=JevV2Resolver(new_policy=True),
                       date_role_fallback_resolver=date_role)
    with pytest.raises(ValueError, match="title_fields_require_read_only_shadow"):
        EventProcessor(config, self_user, mode="production", llm_resolver=JevV2Resolver(new_policy=True),
                       title_resolver=title)
    with pytest.raises(ValueError, match="supervised_native_fallbacks_require_jev_v2_new_policy"):
        EventProcessor(config, self_user, mode="production", llm_resolver=legacy,
                       date_role_fallback_resolver=date_role, supervised_rollout=True)
    with pytest.raises(ValueError, match="deadline_clarifications_require_read_only_shadow"):
        EventProcessor(config, self_user, mode="production", llm_resolver=JevV2Resolver(new_policy=True),
                       deadline_clarifications_path="unused", supervised_rollout=True)
    with pytest.raises(ValueError, match="simulated_candidates_require_read_only_shadow"):
        EventProcessor(config, self_user, mode="production", llm_resolver=JevV2Resolver(new_policy=True),
                       allow_simulated_candidates=True, supervised_rollout=True)

    # The separately gated production injection is accepted only with the actual new-policy resolver.
    processor = EventProcessor(
        config,
        self_user,
        mode="production",
        llm_resolver=JevV2Resolver(new_policy=True, transport=lambda *_args: {}),
        semantic_fallback=semantic,
        date_role_fallback_resolver=date_role,
        title_resolver=title,
        supervised_rollout=True,
    )
    assert processor.date_role_fallback_resolver is date_role
    assert processor.title_resolver is title
