from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path
import sys
import types

import pytest


ROOT = Path(__file__).resolve().parents[1]
WRITER_ROOT = ROOT / "writer_candidate" / "polardb_work_writer"


def _load_candidate_policy():
    spec = importlib.util.spec_from_file_location(
        "phase3_candidate_policy", WRITER_ROOT / "policy.py"
    )
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def _load_candidate_writer():
    """Load the copied writer under isolated names without importing its DB config."""
    package_names = (
        "phase3_workflows",
        "phase3_workflows.polardb_work_writer",
        "workflows",
        "workflows.polardb_work_writer",
    )
    auxiliary_names = (
        "workflows.polardb_work_writer.policy",
        "workflows.polardb_work_writer.db",
        "pymysql",
    )
    previous = {name: sys.modules.get(name) for name in (*package_names, *auxiliary_names)}
    workflows = types.ModuleType("workflows")
    workflows.__path__ = []
    package = types.ModuleType("workflows.polardb_work_writer")
    package.__path__ = []
    sys.modules["workflows"] = workflows
    sys.modules["workflows.polardb_work_writer"] = package
    try:
        for module_name, filename in (
            ("workflows.polardb_work_writer.policy", "policy.py"),
        ):
            spec = importlib.util.spec_from_file_location(
                module_name, WRITER_ROOT / filename
            )
            module = importlib.util.module_from_spec(spec)
            assert spec and spec.loader
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
        db_module = types.ModuleType("workflows.polardb_work_writer.db")
        db_module.connect_writer = lambda: None
        sys.modules["workflows.polardb_work_writer.db"] = db_module
        pymysql = types.ModuleType("pymysql")
        pymysql.IntegrityError = type("IntegrityError", (Exception,), {})
        pymysql.OperationalError = type("OperationalError", (Exception,), {})
        sys.modules["pymysql"] = pymysql
        spec = importlib.util.spec_from_file_location(
            "phase3_candidate_writer", WRITER_ROOT / "writer.py"
        )
        module = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(module)
        return module.WorkWriter
    finally:
        for name in package_names:
            if previous[name] is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous[name]
        for name in auxiliary_names:
            if previous[name] is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous[name]


def test_claim_child_action_policy_accepts_only_stable_scoped_source_keys():
    policy = _load_candidate_policy()
    identity = "synthetic-conversation\0synthetic-message\0asset-cycle\0asset_tags"
    action_key = "child:" + hashlib.sha256(identity.encode()).hexdigest()[:24]
    request = policy.normalize_request({
        "operation": "claim_child_action",
        "contract_version": "child-actions/1.0",
        "conversation_id": "synthetic-conversation",
        "message_id": "synthetic-message",
        "parent_action_id": 12,
        "parent_task_id": "synthetic-parent-task",
        "business_scope": "asset-cycle",
        "scope_key": "asset_tags",
        "action_key": action_key,
        "status": "CLAIMED",
    })

    assert request["action_key"] == action_key
    assert request["parent_action_id"] == 12
    assert request["scope_key"] == "asset_tags"
    with pytest.raises(policy.InvalidWriteRequest, match="child_action_key_mismatch"):
        policy.normalize_request({
            "operation": "claim_child_action",
            "contract_version": "child-actions/1.0",
            "conversation_id": "synthetic-conversation",
            "message_id": "synthetic-message",
            "parent_action_id": 12,
            "parent_task_id": "synthetic-parent-task",
            "business_scope": "asset-cycle",
            "scope_key": "asset_tags",
            "action_key": "child:mutable-title",
            "status": "CLAIMED",
        })


def test_assignment_audit_is_bounded_and_rejects_unlisted_or_raw_fields():
    policy = _load_candidate_policy()
    request = policy.normalize_request({
        "operation": "save_assignment_audit",
        "action_id": 12,
        "assignment_audit": {
            "rule_version": "confirmed-assignment/2026-10-v1",
            "source": "rule-driven-task-routing/references/confirmed-assignment-habits.md",
            "rule_id": "A",
            "primary_owner": "陈红洁",
            "collaborators": ["李林骁"],
            "awareness_only": False,
            "awareness_names": [],
            "manual_assignment_required": False,
            "assignment_basis": "confirmed_static_rule",
            "handover_evidence_ids": [],
            "handover_source": None,
        },
    })

    assert request["assignment_audit"]["primary_owner"] == "陈红洁"
    with pytest.raises(policy.InvalidWriteRequest, match="unknown_assignment_audit_fields"):
        policy.normalize_request({
            "operation": "save_assignment_audit",
            "action_id": 12,
            "assignment_audit": {
                "rule_version": "v1",
                "source": "synthetic",
                "rule_id": "A",
                "primary_owner": "陈红洁",
                "collaborators": [],
                "awareness_only": False,
                "awareness_names": [],
                "manual_assignment_required": False,
                "assignment_basis": "confirmed_static_rule",
                "handover_evidence_ids": [],
                "handover_source": None,
                "raw_message": "synthetic message must never be persisted",
            },
        })


def test_writer_candidate_exposes_a_schema_checked_contract_probe():
    writer_class = _load_candidate_writer()

    assert writer_class.CONTRACT_VERSION == "polardb-work-writer/0.2.0"
    assert writer_class.is_durable_work_writer is True
    assert callable(getattr(writer_class, "contract_info", None))


def test_writer_candidate_mcp_schema_lists_only_the_new_bounded_operations():
    source = (WRITER_ROOT / "mcp_server.py").read_text(encoding="utf-8")
    assert '"claim_child_action"' in source
    assert '"save_assignment_audit"' in source
    assert "parent_task_id" in source
    assert "assignment_audit" in source


def test_query_adapter_exposes_parent_audit_and_canonical_compatibility_fields():
    source = (ROOT / "writer_candidate" / "polardb_work_adapter" / "query.py").read_text(encoding="utf-8")
    for field in (
        "id AS action_id", "parent_action_id", "rule_audit", "effective_external_task_id", "canonical_action_id",
    ):
        assert field in source
