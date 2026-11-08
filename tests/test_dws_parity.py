from __future__ import annotations

import os
import stat
import tempfile
import unittest
from pathlib import Path

from dws_parity import DwsParityLedger
from listener_engine import AtomicPendingState, WakeupRouter


class DwsParityTests(unittest.TestCase):
    def test_live_wakeup_records_identity_and_keeps_existing_pending_scan_trigger(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            clock = [1_800_000_000.0]
            ledger = DwsParityLedger(root / "parity", now=lambda: clock[0])
            state = AtomicPendingState(root / "listener-state")
            router = WakeupRouter(
                "configured-group", ["configured-group"], state, parity_ledger=ledger,
            )
            clock[0] += 10

            self.assertTrue(router.handle({
                "data": {
                    "conversationId": "configured-group",
                    "messageId": "live-message-id",
                    "createTime": clock[0],
                    "text": "content is not persisted",
                },
            }))

            self.assertEqual(state.snapshot(), {"configured-group": 1})
            self.assertEqual(ledger.summary()["event_messages"], 1)

    def test_event_and_complete_history_scan_match_by_private_message_identity_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            clock = [1_800_000_000.0]
            ledger = DwsParityLedger(root / "parity", now=lambda: clock[0])

            clock[0] += 10
            self.assertTrue(ledger.record_event({
                "data": {
                    "conversationId": "group-private-id",
                    "messageId": "message-private-id",
                    "createTime": clock[0],
                    "text": "private message content",
                },
            }, expected_group_id="group-private-id"))

            clock[0] += 30
            self.assertEqual(ledger.record_history("group-private-id", [{
                "conversation_id": "group-private-id",
                "message_id": "message-private-id",
                "create_time": clock[0] - 30,
                "text": "private message content",
            }], scan_start=clock[0] - 35, scan_end=clock[0]), 1)

            report = ledger.summary(now=clock[0])
            self.assertEqual(report["event_messages"], 1)
            self.assertEqual(report["history_messages"], 1)
            self.assertEqual(report["matched_messages"], 1)
            self.assertEqual(report["event_missing_after_complete_scan"], 0)
            self.assertEqual(report["history_without_event"], 0)
            self.assertEqual(report["event_coverage_percent"], 100.0)

            saved = (root / "parity" / "event-history-parity.json").read_text()
            self.assertNotIn("private message content", saved)
            self.assertNotIn("message-private-id", saved)
            self.assertNotIn("group-private-id", saved)
            self.assertEqual(stat.S_IMODE(os.stat(root / "parity" / "event-history-parity.json").st_mode), 0o600)
            self.assertEqual(stat.S_IMODE(os.stat(root / "parity").st_mode), 0o700)


    def test_uncovered_event_stays_pending_and_history_waits_for_stream_delivery(self):
        with tempfile.TemporaryDirectory() as directory:
            clock = [1_800_000_000.0]
            ledger = DwsParityLedger(Path(directory) / "parity", now=lambda: clock[0])
            ledger.record_group_ready("group")
            clock[0] += 1
            ledger.record_event({"messageId": "message-pending", "createTime": clock[0]}, expected_group_id="group")
            ledger.record_event({"messageId": "message-missing", "createTime": clock[0]}, expected_group_id="group")
            ledger.record_history("group", [{
                "conversation_id": "group", "message_id": "history-only", "create_time": clock[0] - 0.5,
            }], scan_start=clock[0] - 1, scan_end=clock[0] - 0.1)

            pending = ledger.summary(now=clock[0] + 30, grace_seconds=60)
            self.assertEqual(pending["event_pending_history"], 2)
            self.assertEqual(pending["event_missing_after_complete_scan"], 0)
            self.assertEqual(pending["history_pending_event"], 1)
            self.assertEqual(pending["history_without_event"], 0)

            aged = ledger.summary(now=clock[0] + 90, grace_seconds=60)
            self.assertEqual(aged["event_pending_history"], 2)
            self.assertEqual(aged["event_missing_after_complete_scan"], 0)
            self.assertEqual(aged["history_pending_event"], 0)
            self.assertEqual(aged["history_without_event"], 1)
            self.assertIsNone(aged["event_coverage_percent"])

    def test_event_is_marked_missing_only_after_a_complete_scan_covers_it(self):
        with tempfile.TemporaryDirectory() as directory:
            clock = [1_800_000_000.0]
            ledger = DwsParityLedger(Path(directory) / "parity", now=lambda: clock[0])
            clock[0] += 10
            event_time = clock[0]
            ledger.record_event({
                "conversationId": "group", "messageId": "message-not-in-history", "createTime": event_time,
            }, expected_group_id="group")
            clock[0] += 5
            ledger.record_history(
                "group", [], scan_start=event_time - 1, scan_end=event_time + 1,
            )

            report = ledger.summary()
            self.assertEqual(report["event_pending_history"], 0)
            self.assertEqual(report["event_missing_after_complete_scan"], 1)
            self.assertEqual(report["event_coverage_percent"], 0.0)


    def test_event_identity_mismatch_is_not_counted_as_a_delivery(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = DwsParityLedger(Path(directory) / "parity", now=lambda: 1_800_000_000.0)

            self.assertFalse(ledger.record_event({
                "conversationId": "wrong-group", "messageId": "message-1",
            }, expected_group_id="configured-group"))
            self.assertFalse(ledger.record_event({
                "conversationId": "configured-group",
            }, expected_group_id="configured-group"))

            report = ledger.summary()
            self.assertEqual(report["event_messages"], 0)
            self.assertEqual(report["events_missing_identity"], 1)


    def test_empty_complete_scan_is_counted_without_creating_message_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = DwsParityLedger(Path(directory) / "parity", now=lambda: 1_800_000_000.0)

            self.assertEqual(
                ledger.record_history("group", [], scan_start=1_799_999_990, scan_end=1_800_000_010),
                0,
            )
            report = ledger.summary()
            self.assertEqual(report["complete_history_scans"], 1)
            self.assertEqual(report["history_messages"], 0)
