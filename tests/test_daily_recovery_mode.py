from datetime import datetime
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from event_worker import OneShotHistoryPoller, summary_result  # noqa: E402


class FakeAdapter:
    def __init__(self, day_type="workday"):
        self.day_type = day_type
        self.queries = []

    def query(self, request):
        self.queries.append(dict(request))
        if request["view"] == "runtime_config":
            return {"config": {
                "config_calendar": [{"dt": "2026-10-09", "day_type": self.day_type}],
                "config_group": [{"group_name": "configured", "conversation_id": "conv-1", "purpose": "todo_scan"}],
                "config_staff": [],
                "active_routing_loaded": True,
            }}
        return {"rows": [], "has_more": False}


class FakeReader:
    def __init__(self):
        self.calls = []

    def read_group(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "complete": True,
            "source_order_provenance": "complete_dws_chronological_page",
            "messages": [],
        }


class FakeDws:
    def get_self(self):
        return {"name": "self", "user_id": "self-id"}


def make_poller(day_type="workday", daily_recovery=False):
    adapter = FakeAdapter(day_type)
    reader = FakeReader()
    poller = OneShotHistoryPoller(
        adapter=adapter,
        reader=reader,
        dws=FakeDws(),
        now_factory=lambda: datetime.fromisoformat("2026-10-09T17:30:00+08:00"),
        daily_recovery=daily_recovery,
    )
    return poller, adapter, reader


def test_daily_recovery_allows_history_catchup_exactly_at_1730():
    poller, _adapter, reader = make_poller(daily_recovery=True)
    result = poller.run_once()
    assert result["status"] == "COMPLETED"
    assert len(reader.calls) == 1
    assert result["groups_complete"] == 1


def test_daily_recovery_keeps_authoritative_off_day_gate_visible():
    poller, _adapter, reader = make_poller("weekend", daily_recovery=True)
    result = poller.run_once()
    assert result["status"] == "SKIPPED"
    assert result["reason"] == "calendar_gate_closed"
    assert result["calendar"]["allowed"] is False
    assert reader.calls == []


def test_off_day_gate_remains_visible_even_when_daily_recovery_floor_is_future():
    poller, adapter, reader = make_poller("weekend", daily_recovery=True)
    poller.not_before = datetime.fromisoformat("2026-10-10T00:00:00+08:00")
    result = poller.run_once()
    assert result["status"] == "SKIPPED"
    assert result["reason"] == "calendar_gate_closed"
    assert len([item for item in adapter.queries if item["view"] == "runtime_config"]) == 1
    assert reader.calls == []


def test_daily_recovery_allows_a_bounded_delay_after_the_1730_timer_boundary():
    poller, _adapter, reader = make_poller(daily_recovery=True)
    poller.now_factory = lambda: datetime.fromisoformat("2026-10-09T17:31:00+08:00")
    result = poller.run_once()
    assert result["status"] == "COMPLETED"
    assert len(reader.calls) == 1


def test_daily_recovery_window_ends_before_general_evening_processing():
    poller, _adapter, reader = make_poller(daily_recovery=True)
    poller.now_factory = lambda: datetime.fromisoformat("2026-10-09T17:59:00+08:00")
    result = poller.run_once()
    assert result["status"] == "COMPLETED"
    assert len(reader.calls) == 1

    after_window, _adapter, after_reader = make_poller(daily_recovery=True)
    after_window.now_factory = lambda: datetime.fromisoformat("2026-10-09T18:00:00+08:00")
    skipped = after_window.run_once()
    assert skipped["status"] == "SKIPPED"
    assert skipped["reason"] == "outside_execution_window"
    assert after_reader.calls == []


def test_summary_only_worker_result_omits_source_plans_and_error_payloads():
    result = summary_result({
        "status": "COMPLETED",
        "messages": 1,
        "plans": [{"text": "private source body"}],
        "errors": [{"message_id": "private-id", "error": "private detail"}],
        "stats": {"llm_calls": 1, "unmatched_conversations": ["private-conversation"]},
    })
    assert result == {"status": "COMPLETED", "messages": 1, "stats": {"llm_calls": 1}}
    assert "private" not in repr(result)
