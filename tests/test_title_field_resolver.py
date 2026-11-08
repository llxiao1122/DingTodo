import json
import pytest

from llm_adapter import LlmError, TitleFieldResolver


def test_title_resolver_uses_one_locked_native_call_with_redacted_source():
    calls = []
    factory_options = []

    class Agent:
        def run_conversation(self, user_message, **kwargs):
            calls.append((json.loads(user_message), kwargs))
            return {"completed": True, "final_response": json.dumps({
                "title": "巡库时间与巡查要求意见跟进",
                "evidence_spans": ["巡库时间", "加强巡查"],
            }, ensure_ascii=False)}

    resolver = TitleFieldResolver(agent_factory=lambda **kwargs: (factory_options.append(kwargs) or Agent()))
    result = resolver.resolve({"text": "@张三 巡库时间需调整，加强巡查", "title_context": "resource_id=secret-id @李四"}, {"title": None})
    assert result["evidence_spans"] == ["巡库时间", "加强巡查"]
    assert len(calls) == 1
    assert "张三" not in json.dumps(calls[0][0], ensure_ascii=False)
    assert calls[0][0]["available_context"] is None
    assert "secret-id" not in json.dumps(calls[0][0], ensure_ascii=False)
    assert calls[0][1]["conversation_history"] == []
    assert calls[0][1]["system_message"]
    assert factory_options[0]["enabled_toolsets"] == []
    assert factory_options[0]["save_trajectories"] is False
    assert factory_options[0]["max_iterations"] == 1


def test_oversized_source_fails_before_agent_call():
    calls = []
    resolver = TitleFieldResolver(max_source_chars=3, agent_factory=lambda **kwargs: calls.append(kwargs))
    with pytest.raises(LlmError, match="title_source_incomplete"):
        resolver.resolve({"text": "超过上限"}, {})
    assert calls == []


@pytest.mark.parametrize("title", ["关联 resource_id=secret", "处理@张三事项", "引用[RESOURCE_ID]"])
def test_unredacted_or_placeholder_title_rejected(title):
    class Agent:
        def run_conversation(self, user_message, **kwargs):
            return {"completed": True, "final_response": json.dumps({"title": title, "evidence_spans": ["处理事项"]})}
    resolver = TitleFieldResolver(agent_factory=lambda **kwargs: Agent())
    with pytest.raises(ValueError):
        resolver.resolve({"text": "处理事项 resource_id=secret @张三"}, {})


def test_evidence_may_reference_redacted_text_but_title_may_not_use_marker():
    class Agent:
        def run_conversation(self, user_message, **kwargs):
            context = json.loads(user_message)
            assert context["source_text"] == "核对文件 resource_id=[RESOURCE_ID]"
            return {"completed": True, "final_response": json.dumps({"title": "核对文件", "evidence_spans": ["resource_id=[RESOURCE_ID]"]})}
    result = TitleFieldResolver(agent_factory=lambda **kwargs: Agent()).resolve({"text": "核对文件 resource_id=abc"}, {})
    assert result["evidence_spans"] == ["resource_id=[RESOURCE_ID]"]


def test_final_serialized_request_fails_closed_on_unsupported_encoded_identifier():
    calls = []
    class Agent:
        def run_conversation(self, user_message, **kwargs):
            calls.append(user_message)
            return {"completed": True, "final_response": "{}"}
    # Exercise final guard through a value that survives the standard redactor.
    resolver = TitleFieldResolver(agent_factory=lambda **kwargs: Agent())
    with pytest.raises(LlmError, match="title_redaction_incomplete"):
        resolver.resolve({"text": "处理事项 task_id%253Dsecret"}, {})
    assert calls == []
