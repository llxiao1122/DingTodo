import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from live_runner import calendar_gate, max_message_cursor, build_planner_config, group_specs  # noqa: E402


class LiveRunnerTests(unittest.TestCase):
    def test_calendar_gate_allows_workday_and_shift(self):
        config = {"config_calendar": [{"dt": "2026-09-21", "day_type": "workday"}]}
        self.assertTrue(calendar_gate(config, date(2026, 9, 21))["allowed"])
        config["config_calendar"][0]["day_type"] = "weekend"
        self.assertFalse(calendar_gate(config, date(2026, 9, 21))["allowed"])

    def test_calendar_gate_stops_when_missing(self):
        self.assertFalse(calendar_gate({"config_calendar": []}, date(2026, 9, 21))["allowed"])

    def test_planner_config_does_not_claim_active_matrix_without_one(self):
        result = build_planner_config({"config_staff": [[1, "李林骁", "STALE", "leader"]]})
        self.assertFalse(result["active_routing_loaded"])
        self.assertEqual(result["staff"]["李林骁"]["user_id"], "STALE")

    def test_planner_config_loads_reviewed_current_area_matrix_and_cross_routes(self):
        result = build_planner_config(
            {
                "config_staff": [[1, "李林骁", "STALE", "leader"]],
                "config_area_duty": [
                    {"id": 5, "area_name": "杂品库", "staff_name": "杨梦卓", "cycle_start": "2026-09-01", "cycle_end": "2026-10-31", "reviewed": 1},
                    {"id": 6, "area_name": "大件库区", "staff_name": "苗笑天", "cycle_start": "2026-09-01", "cycle_end": "2026-10-31", "reviewed": 1},
                    {"id": 7, "area_name": "材料棚及办公用房", "staff_name": "谭继衡", "cycle_start": "2026-09-01", "cycle_end": "2026-10-31", "reviewed": 1},
                    {"id": 8, "area_name": "中小件区", "staff_name": "张志斌", "cycle_start": "2026-09-01", "cycle_end": "2026-10-31", "reviewed": 1},
                ],
            },
            run_date=date(2026, 9, 22),
        )
        self.assertTrue(result["active_routing_loaded"])
        self.assertEqual(result["responsibility_rules"]["hazardous_waste"], "杨梦卓")
        self.assertEqual(result["responsibility_rules"]["scrap"], "谭继衡")

    def test_max_cursor_is_time_then_message_id(self):
        messages = [
            {"create_time": "2026-09-21T09:00:00+08:00", "message_id": "m1"},
            {"create_time": "2026-09-21T09:00:00+08:00", "message_id": "m2"},
            {"create_time": "2026-09-21T10:00:00+08:00", "message_id": "m0"},
        ]
        self.assertEqual(max_message_cursor(messages), ("2026-09-21 10:00:00", "m0"))

    def test_group_specs_includes_iron_west_workgroup_when_authoritatively_enabled(self):
        config = {
            "config_group": [
                {
                    "group_name": "铁炉西工班群",
                    "conversation_id": "cid8QKDfYpg+85AWaskH8XHjQ==",
                    "purpose": "todo_scan",
                },
                {
                    "group_name": "台账更新群",
                    "conversation_id": "allowed-conversation",
                    "purpose": "todo_scan",
                },
            ]
        }
        specs = group_specs(config, [])
        self.assertEqual(
            [item["conversation_id"] for item in specs],
            ["cid8QKDfYpg+85AWaskH8XHjQ==", "allowed-conversation"],
        )


if __name__ == "__main__":
    unittest.main()
