from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dws_skill_context import DwsSkillContextError, load_dws_skill_context
from llm_adapter import (
    DateRoleFallbackResolver,
    HermesCodexLlmResolver,
    LlmError,
    SemanticFallbackResolver,
    TitleFieldResolver,
)


def _write_skill(root: Path, name: str, contract: str, *, suffix: str = "") -> None:
    directory = root / name
    directory.mkdir(parents=True, exist_ok=True)
    if name == "dingtalk-todo":
        body = f"## 执行契约\n{contract}\n\n<!-- VISIBLE_SHORTCUTS_START -->\nshortcuts"
    else:
        body = (
            "<!-- DWS_RUNTIME_CONTRACT_START -->\n"
            + contract
            + "\n<!-- DWS_RUNTIME_CONTRACT_END -->"
        )
    (directory / "SKILL.md").write_text(
        f"---\nname: {name}\n---\n{body}\n{suffix}", encoding="utf-8"
    )


@pytest.fixture
def official_skill_root(tmp_path: Path) -> Path:
    _write_skill(tmp_path, "dingtalk-shared", "SHARED_OFFICIAL_CONTRACT")
    _write_skill(tmp_path, "dingtalk-chat", "CHAT_OFFICIAL_CONTRACT")
    _write_skill(
        tmp_path,
        "dingtalk-todo",
        "TODO_OFFICIAL_CONTRACT",
        suffix="OUTSIDE_ALLOWED_TODO_SECTION",
    )
    return tmp_path


def test_loader_includes_only_the_three_allowlisted_skill_contracts(official_skill_root):
    context = load_dws_skill_context(official_skill_root)

    assert "SHARED_OFFICIAL_CONTRACT" in context
    assert "CHAT_OFFICIAL_CONTRACT" in context
    assert "TODO_OFFICIAL_CONTRACT" in context
    assert "OUTSIDE_ALLOWED_TODO_SECTION" not in context
    assert "shortcuts" not in context
    assert "no DWS tools" in context


def test_loader_fails_closed_when_an_official_skill_is_missing(tmp_path: Path):
    _write_skill(tmp_path, "dingtalk-shared", "SHARED_OFFICIAL_CONTRACT")

    with pytest.raises(DwsSkillContextError, match="skill_unavailable"):
        load_dws_skill_context(tmp_path)


def test_loader_rejects_skill_with_malformed_contract(tmp_path: Path):
    _write_skill(tmp_path, "dingtalk-shared", "SHARED_OFFICIAL_CONTRACT")
    _write_skill(tmp_path, "dingtalk-chat", "CHAT_OFFICIAL_CONTRACT")
    _write_skill(tmp_path, "dingtalk-todo", "TODO_OFFICIAL_CONTRACT")
    (tmp_path / "dingtalk-chat" / "SKILL.md").write_text(
        "---\nname: dingtalk-chat\n---\nmissing contract markers", encoding="utf-8"
    )

    with pytest.raises(DwsSkillContextError, match="skill_contract_invalid"):
        load_dws_skill_context(tmp_path)


@pytest.mark.parametrize("resolver_kind", ["generic", "semantic", "title", "date_role"])
def test_codex_resolvers_receive_official_skill_context_without_message_leakage(
    resolver_kind, official_skill_root
):
    calls = []
    source = "PRIVATE_MESSAGE_SENTINEL 请核对台账并在2026年10月8日前反馈。"

    class Agent:
        def run_conversation(self, user_message, *, system_message, conversation_history):
            calls.append((user_message, system_message, conversation_history))
            if resolver_kind == "generic":
                response = {"semantic_intent": "follow_up"}
            elif resolver_kind == "semantic":
                response = {
                    "applicability": "applicable",
                    "followup": "required",
                    "title_span": None,
                    "due_key": "unknown",
                    "same_work": {},
                    "evidence_spans": ["核对台账"],
                }
            elif resolver_kind == "title":
                response = {"title": "核对台账", "evidence_spans": ["核对台账"]}
            else:
                response = {
                    "roles": [{
                        "key": "date_0",
                        "role": "deadline",
                        "evidence": "2026年10月8日前",
                    }]
                }
            return {"completed": True, "final_response": json.dumps(response, ensure_ascii=False)}

    resolver_types = {
        "generic": HermesCodexLlmResolver,
        "semantic": SemanticFallbackResolver,
        "title": TitleFieldResolver,
        "date_role": DateRoleFallbackResolver,
    }
    resolver = resolver_types[resolver_kind](
        agent_factory=lambda **_kwargs: Agent(), dws_skills_root=official_skill_root
    )

    if resolver_kind == "generic":
        result = resolver.resolve({"text": source}, {})
        assert result == {"semantic_intent": "follow_up"}
    elif resolver_kind == "semantic":
        result = resolver.resolve_context({"text": source}, {"decision": "REVIEW"})
        assert result["applicability"] == "applicable"
    elif resolver_kind == "title":
        result = resolver.resolve({"text": source}, {})
        assert result["title"] == "核对台账"
    else:
        span = "2026年10月8日前"
        start = source.index(span)
        result = resolver.resolve_roles(
            {"text": source},
            date_candidates=[{
                "key": "date_0",
                "role": "unknown",
                "source_span": span,
                "source_span_start": start,
                "source_span_end": start + len(span),
            }],
        )
        assert result["roles"][0]["role"] == "deadline"

    assert len(calls) == 1
    user_message, system_message, history = calls[0]
    assert "SHARED_OFFICIAL_CONTRACT" in system_message
    assert "CHAT_OFFICIAL_CONTRACT" in system_message
    assert "TODO_OFFICIAL_CONTRACT" in system_message
    assert "OUTSIDE_ALLOWED_TODO_SECTION" not in system_message
    assert "PRIVATE_MESSAGE_SENTINEL" not in system_message
    assert "no DWS tools" in system_message
    assert history == []
    assert source in user_message or "核对台账" in user_message


def test_codex_resolver_does_not_call_model_if_skill_contract_is_unavailable(tmp_path: Path):
    agent_calls = []
    resolver = HermesCodexLlmResolver(
        agent_factory=lambda **_kwargs: agent_calls.append("initialized"),
        dws_skills_root=tmp_path,
    )

    with pytest.raises(LlmError, match="dws_skill_context_unavailable:skill_unavailable"):
        resolver.resolve({"text": "模拟消息"}, {})

    assert agent_calls == []
