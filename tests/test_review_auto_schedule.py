from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import review_auto_schedule as scheduler
from review_inbox import connect, prepare_dispatch, register_batch


TODAY = date(2026, 11, 5)
ACTIVATED_AT = datetime(2026, 11, 4, 9, 0, 0)


def action(action_id: str, *, updated_at: str = '2026-11-05 10:00:00') -> dict:
    return {
        'action_id': action_id,
        'status': 'REVIEW',
        'updated_at': updated_at,
        'conversation_id': f'source-conversation-{action_id}',
        'message_id': f'source-message-{action_id}',
    }


def source(action_id: str) -> dict:
    return {
        'group_name': '测试群',
        'sender_name': '测试发送人',
        'message_time': '2026-11-05 09:00:00',
        'text': f'请检查项目 {action_id}',
        'text_truncated': False,
    }


def full_history(messages: list[dict]) -> dict:
    return {'complete': True, 'partial': False, 'hasMore': False,
            'failures': [], 'messages': messages}


class FakeGateway:
    def __init__(self, by_day: dict[date, list[dict]] | None = None):
        self.by_day = by_day or {}
        self.list_calls: list[date] = []
        self.detail_calls: list[tuple[str, date]] = []
        self.invalid_detail: set[str] = set()

    def list_reviews(self, requested_day: date) -> dict:
        self.list_calls.append(requested_day)
        return {'ok': True, 'complete': True, 'actions': self.by_day.get(requested_day, [])}

    def review_detail(self, action_id: str, requested_day: date) -> dict:
        self.detail_calls.append((action_id, requested_day))
        item = next(row for row in self.by_day[requested_day] if row['action_id'] == action_id)
        if action_id in self.invalid_detail:
            return {'ok': False, 'complete': False, 'found': True,
                    'action': item, 'source': None}
        return {'ok': True, 'complete': True, 'found': True,
                'action': item, 'source': source(action_id)}


class ReviewAutoScheduleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.conn = connect(str(Path(self.tmp.name) / 'inbox.sqlite3'))
        scheduler.enable(self.conn, ACTIVATED_AT)

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def tick(self, gateway: FakeGateway, **options) -> dict:
        return scheduler.run_tick(
            self.conn, gateway, today=options.pop('today', TODAY),
            conversation_id='owner-conversation', owner_id='owner', bot_open_id='bot',
            batch_id_factory=options.pop('batch_id_factory', lambda: 'auto_batch_0001'),
            **options,
        )

    def test_queues_only_new_complete_reviews_and_freezes_the_bound_source(self):
        old = action('1', updated_at='2026-11-04 08:59:59')
        fresh = action('2')
        gateway = FakeGateway({date(2026, 11, 4): [old], TODAY: [fresh]})

        result = self.tick(gateway)

        self.assertEqual(result, {'status': 'queued', 'batch': 'auto_batch_0001'})
        self.assertEqual(gateway.list_calls, [date(2026, 11, 4), TODAY])
        self.assertEqual(gateway.detail_calls, [('2', TODAY)])
        self.assertEqual(self.conn.execute('SELECT status FROM dispatch').fetchone(), ('prepared',))
        items = json.loads(self.conn.execute('SELECT items FROM dispatch').fetchone()[0])
        self.assertEqual(items[0]['action_id'], '2')
        self.assertEqual(items[0]['source'], source('2'))

    def test_incomplete_listing_never_queues(self):
        gateway = FakeGateway({date(2026, 11, 4): [action('1', updated_at='2026-11-04 10:00:00')]})
        gateway.list_reviews = lambda _day: {'ok': False, 'complete': False, 'actions': []}

        self.assertEqual(self.tick(gateway)['status'], 'review_discovery_incomplete')
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM dispatch').fetchone()[0], 0)

    def test_transient_source_failures_are_persisted_without_blocking_later_dates(self):
        previous_day = date(2026, 11, 4)
        gateway = FakeGateway({previous_day: [
            action('1', updated_at='2026-11-04 10:00:00'),
            action('2', updated_at='2026-11-04 11:00:00'),
        ], TODAY: [action('3')]})
        gateway.invalid_detail.add('1')
        read_detail = gateway.review_detail
        raised = False

        def fail_once(action_id: str, requested_day: date) -> dict:
            nonlocal raised
            if action_id == '2' and not raised:
                raised = True
                gateway.detail_calls.append((action_id, requested_day))
                raise TimeoutError('temporary source read failure')
            return read_detail(action_id, requested_day)

        gateway.review_detail = fail_once

        first = self.tick(gateway)

        self.assertEqual(first, {'status': 'queued', 'batch': 'auto_batch_0001',
                                 'retry_action_ids': ['1', '2']})
        self.assertEqual(self.conn.execute('SELECT scan_day FROM review_auto_schedule').fetchone()[0],
                         TODAY.isoformat())
        self.assertEqual(self.conn.execute(
            'SELECT COUNT(*) FROM review_auto_quarantined_action').fetchone()[0], 0)
        first_items = json.loads(self.conn.execute('SELECT items FROM dispatch').fetchone()[0])
        self.assertEqual([item['action_id'] for item in first_items], ['3'])
        self.assertEqual(self.conn.execute(
            "SELECT action_id,status FROM review_auto_retry_action ORDER BY action_id").fetchall(),
            [('1', 'pending'), ('2', 'pending'), ('3', 'queued')])

        self.conn.close()
        self.conn = connect(str(Path(self.tmp.name) / 'inbox.sqlite3'))

        gateway.invalid_detail.remove('1')
        gateway.by_day[TODAY].append(action('4'))
        self.conn.execute("UPDATE dispatch SET status='delivered'")
        second = self.tick(gateway, batch_id_factory=lambda: 'auto_batch_0002')

        self.assertEqual(second, {'status': 'queued', 'batch': 'auto_batch_0002'})
        self.assertEqual(gateway.list_calls,
                         [previous_day, TODAY, previous_day, TODAY])
        self.assertEqual(gateway.detail_calls, [('1', previous_day), ('2', previous_day),
                                                ('3', TODAY), ('1', previous_day),
                                                ('2', previous_day), ('4', TODAY)])
        self.assertEqual(self.conn.execute('SELECT scan_day FROM review_auto_schedule').fetchone()[0],
                         TODAY.isoformat())
        queued = json.loads(self.conn.execute(
            "SELECT items FROM dispatch WHERE id='auto_batch_0002'").fetchone()[0])
        self.assertEqual([row['action_id'] for row in queued], ['4', '1', '2'])
        self.assertEqual([row['source_date'] for row in queued],
                         [TODAY.isoformat(), previous_day.isoformat(), previous_day.isoformat()])
        self.assertEqual(self.conn.execute(
            'SELECT COUNT(*) FROM review_auto_quarantined_action').fetchone()[0], 0)

    def test_missing_retry_version_is_retained_as_stale_audit_while_scan_continues(self):
        previous_day = date(2026, 11, 4)
        gateway = FakeGateway({previous_day: [action('8', updated_at='2026-11-04 10:00:00')]})
        gateway.invalid_detail.add('8')

        first = self.tick(gateway)

        self.assertEqual(first['status'], 'review_items_retrying')
        self.assertEqual(self.conn.execute(
            "SELECT status FROM review_auto_retry_action WHERE action_id='8'").fetchone(),
            ('pending',))
        gateway.by_day[previous_day] = []
        gateway.by_day[TODAY] = [action('9')]

        second = self.tick(gateway, batch_id_factory=lambda: 'auto_batch_0002')

        self.assertEqual(second, {'status': 'queued', 'batch': 'auto_batch_0002'})
        self.assertEqual(self.conn.execute(
            "SELECT status,last_error FROM review_auto_retry_action WHERE action_id='8'").fetchone(),
            ('stale', 'review_action_not_in_source_day'))
        items = json.loads(self.conn.execute(
            "SELECT items FROM dispatch WHERE id='auto_batch_0002'").fetchone()[0])
        self.assertEqual([item['action_id'] for item in items], ['9'])

    def test_dispatch_transaction_rechecks_seen_action_ids_before_insert(self):
        candidate = action('12')
        queued_at = '2026-11-05 10:00:00.000000'
        scheduler._enqueue_retry_actions(self.conn, [candidate], TODAY, queued_at)
        frozen = {'action_id': '12', 'source_date': TODAY.isoformat(), 'source': source('12')}
        retry_key = [('12', candidate['updated_at'])]

        scheduler._queue(self.conn, 'auto_batch_0001', 'owner-conversation', 'owner',
                          [frozen], queued_at, retry_key)

        with self.assertRaisesRegex(ValueError, 'duplicate_action_race_lost'):
            scheduler._queue(self.conn, 'auto_batch_0002', 'owner-conversation', 'owner',
                             [frozen], queued_at, retry_key)

        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM dispatch').fetchone()[0], 1)
        self.assertEqual(self.conn.execute(
            "SELECT status,dispatch_id FROM review_auto_retry_action WHERE action_id='12'").fetchone(),
            ('queued', 'auto_batch_0001'))

    def test_permanent_quarantine_is_scoped_to_action_version(self):
        previous_day = date(2026, 11, 4)
        stale = action('3', updated_at='2026-11-04 10:00:00')
        updated = action('3', updated_at='2026-11-05 10:00:00')
        gateway = FakeGateway({previous_day: [stale], TODAY: [updated]})
        read_detail = gateway.review_detail

        def too_long_for_old_version(action_id: str, requested_day: date) -> dict:
            result = read_detail(action_id, requested_day)
            if requested_day == previous_day:
                result['source']['text'] = 'x' * 1601
            return result

        gateway.review_detail = too_long_for_old_version

        first = self.tick(gateway)

        self.assertEqual(first, {
            'status': 'queued', 'batch': 'auto_batch_0001',
            'rejected_action_ids': ['3'],
        })
        self.assertEqual(self.conn.execute(
            'SELECT action_id,updated_at,reason FROM review_auto_quarantined_action').fetchone(),
            ('3', stale['updated_at'], 'review_source_too_long'))
        self.assertEqual(self.conn.execute('SELECT scan_day FROM review_auto_schedule').fetchone()[0],
                         TODAY.isoformat())
        self.assertEqual(gateway.detail_calls, [('3', previous_day), ('3', TODAY)])
        self.assertEqual(self.conn.execute(
            'SELECT action_id,updated_at FROM review_auto_quarantined_action').fetchone(),
            ('3', stale['updated_at']))
        queued = json.loads(self.conn.execute('SELECT items FROM dispatch').fetchone()[0])
        self.assertEqual(queued[0]['action_id'], '3')
        self.assertEqual(queued[0]['source_date'], TODAY.isoformat())
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM dispatch').fetchone()[0], 1)

    def test_current_day_is_rechecked_after_an_idle_tick(self):
        gateway = FakeGateway({TODAY: []})

        self.assertEqual(self.tick(gateway)['status'], 'idle')
        self.assertEqual(self.conn.execute('SELECT scan_day FROM review_auto_schedule').fetchone()[0],
                         TODAY.isoformat())
        gateway.by_day[TODAY] = [action('7')]

        self.assertEqual(self.tick(gateway)['status'], 'queued')
        self.assertEqual(gateway.list_calls[-1], TODAY)

    def test_multiple_batches_on_one_day_do_not_skip_items_after_five(self):
        rows = [action(str(number)) for number in range(1, 7)]
        gateway = FakeGateway({TODAY: rows})

        first = self.tick(gateway)
        self.assertEqual(first['status'], 'queued')
        queued = json.loads(self.conn.execute('SELECT items FROM dispatch').fetchone()[0])
        self.assertEqual([row['action_id'] for row in queued], ['1', '2', '3', '4', '5'])
        self.conn.execute("UPDATE dispatch SET status='delivered'")
        register_batch(self.conn, 'auto_batch_0001', 'owner-conversation', 'owner',
                       'prompt-1', '2026-11-05 10:01:00', {i: [str(i)] for i in range(1, 6)},
                       enable_automation=True)
        self.conn.execute("UPDATE batch SET state='answered' WHERE id='auto_batch_0001'")
        self.conn.execute('CREATE TABLE review_auto_terminal '
                          '(batch_id TEXT PRIMARY KEY,status TEXT NOT NULL)')
        self.conn.execute("INSERT INTO review_auto_terminal VALUES ('auto_batch_0001','all_ignored')")

        second = self.tick(gateway, batch_id_factory=lambda: 'auto_batch_0002')

        self.assertEqual(second, {'status': 'queued', 'batch': 'auto_batch_0002'})
        queued_again = json.loads(self.conn.execute(
            "SELECT items FROM dispatch WHERE id='auto_batch_0002'").fetchone()[0])
        self.assertEqual([row['action_id'] for row in queued_again], ['6'])

    def test_auto_send_is_claimed_once_and_delivery_is_proven_from_fresh_history(self):
        gateway = FakeGateway({date(2026, 11, 4): [action('1', updated_at='2026-11-04 10:00:00')]})
        sent: list[tuple[str, str]] = []

        def preflight(batch_id: str) -> dict:
            return {'batch_id': batch_id, 'bot_open_id': 'bot', 'owner_id': 'owner'}

        def send(batch_id: str, context: dict, text: str) -> None:
            self.assertEqual(context['batch_id'], batch_id)
            sent.append((batch_id, text))

        def read_history(batch_id: str, sending_at: str) -> dict:
            text = self.conn.execute('SELECT text FROM dispatch WHERE id=?', (batch_id,)).fetchone()[0]
            return full_history([{'messageId': 'prompt-1', 'conversationId': 'owner-conversation',
                                  'senderId': 'bot', 'text': text, 'createTime': sending_at}])

        result = self.tick(gateway, preflight=preflight, sender=send, history_reader=read_history)

        self.assertEqual(result, {'status': 'dispatch_delivered', 'batch': 'auto_batch_0001'})
        self.assertEqual(len(sent), 1)
        self.assertEqual(self.conn.execute('SELECT status FROM dispatch').fetchone()[0], 'delivered')
        self.assertEqual(self.conn.execute('SELECT state FROM batch').fetchone()[0], 'open')

        blocked = self.tick(gateway, preflight=preflight, sender=send, history_reader=read_history,
                            batch_id_factory=lambda: 'auto_batch_0002')
        self.assertEqual(blocked['status'], 'open_batch_pending')
        self.assertEqual(len(sent), 1)

    def test_card_supplement_runs_only_after_text_anchor_is_persisted(self):
        gateway = FakeGateway({date(2026, 11, 4): [action('1', updated_at='2026-11-04 10:00:00')]})
        card_calls: list[str] = []

        def read_history(batch_id: str, sending_at: str) -> dict:
            text = self.conn.execute('SELECT text FROM dispatch WHERE id=?', (batch_id,)).fetchone()[0]
            return full_history([{'messageId': 'prompt-1', 'conversationId': 'owner-conversation',
                                  'senderId': 'bot', 'text': text, 'createTime': sending_at}])

        def send_card(batch_id: str) -> dict:
            card_calls.append(batch_id)
            self.assertEqual(self.conn.execute(
                'SELECT status FROM dispatch WHERE id=?', (batch_id,),
            ).fetchone(), ('delivered',))
            self.assertEqual(self.conn.execute(
                'SELECT state FROM batch WHERE id=?', (batch_id,),
            ).fetchone(), ('open',))
            return {'status': 'delivery_unknown', 'reason': 'delivery_exception'}

        result = self.tick(
            gateway,
            preflight=lambda _batch_id: {'ready': True},
            sender=lambda *_args: None,
            history_reader=read_history,
            card_sender=send_card,
        )

        self.assertEqual(result, {
            'status': 'dispatch_delivered',
            'batch': 'auto_batch_0001',
            'card_delivery': {'status': 'delivery_unknown', 'reason': 'delivery_exception'},
        })
        self.assertEqual(card_calls, ['auto_batch_0001'])

    def test_card_supplement_is_not_attempted_when_text_delivery_is_unproven(self):
        gateway = FakeGateway({date(2026, 11, 4): [action('1', updated_at='2026-11-04 10:00:00')]})
        card_calls: list[str] = []

        result = self.tick(
            gateway,
            preflight=lambda _batch_id: {'ready': True},
            sender=lambda *_args: None,
            history_reader=lambda _batch, _time: full_history([]),
            card_sender=lambda batch: card_calls.append(batch) or {'status': 'delivered'},
        )

        self.assertEqual(result['status'], 'dispatch_delivery_unknown')
        self.assertNotIn('card_delivery', result)
        self.assertEqual(card_calls, [])

    def test_open_auto_batch_can_resume_card_supplement_after_text_confirmation(self):
        gateway = FakeGateway({date(2026, 11, 4): [action('1', updated_at='2026-11-04 10:00:00')]})

        def read_history(batch_id: str, sending_at: str) -> dict:
            text = self.conn.execute('SELECT text FROM dispatch WHERE id=?', (batch_id,)).fetchone()[0]
            return full_history([{'messageId': 'prompt-1', 'conversationId': 'owner-conversation',
                                  'senderId': 'bot', 'text': text, 'createTime': sending_at}])

        self.tick(gateway, preflight=lambda _batch: {}, sender=lambda *_args: None,
                  history_reader=read_history)
        calls: list[str] = []

        result = self.tick(
            gateway,
            card_sender=lambda batch: calls.append(batch) or {'status': 'delivered'},
        )

        self.assertEqual(result, {
            'status': 'open_batch_pending',
            'batch': 'auto_batch_0001',
            'card_delivery': {'status': 'delivered'},
        })
        self.assertEqual(calls, ['auto_batch_0001'])

    def test_uncertain_delivery_reconciles_without_blind_retry_or_historical_replay(self):
        gateway = FakeGateway({date(2026, 11, 4): [action('1', updated_at='2026-11-04 10:00:00')]})
        sends: list[str] = []

        def preflight(_batch_id: str) -> None:
            return None

        def send(batch_id: str, _context: None, _text: str) -> None:
            sends.append(batch_id)
            raise RuntimeError('transport outcome unknown')

        def history_with_old_duplicate(batch_id: str, sending_at: str) -> dict:
            row = self.conn.execute('SELECT text FROM dispatch WHERE id=?', (batch_id,)).fetchone()
            old_time = (datetime.strptime(sending_at, '%Y-%m-%d %H:%M:%S')
                        - timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')
            return full_history([{'messageId': 'old-prompt', 'conversationId': 'owner-conversation',
                                  'senderId': 'bot', 'text': row[0], 'createTime': old_time}])

        first = self.tick(gateway, preflight=preflight, sender=send,
                          history_reader=history_with_old_duplicate)
        self.assertEqual(first['status'], 'dispatch_delivery_unknown')
        self.assertEqual(self.conn.execute('SELECT status FROM dispatch').fetchone()[0], 'unknown')

        def sender_must_not_retry(*_args) -> None:
            raise AssertionError('unknown dispatch must only reconcile')

        second = self.tick(gateway, preflight=preflight, sender=sender_must_not_retry,
                           history_reader=lambda _batch, _start: full_history([]))

        self.assertEqual(second['status'], 'dispatch_delivery_unknown')
        self.assertEqual(sends, ['auto_batch_0001'])
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM batch').fetchone()[0], 0)

    def test_open_or_unfinished_enabled_batch_blocks_dispatch_but_old_manual_answer_does_not(self):
        gateway = FakeGateway({date(2026, 11, 4): [action('1', updated_at='2026-11-04 10:00:00')]})
        register_batch(self.conn, 'manual_batch_1', 'owner-conversation', 'owner', 'old-prompt',
                       '2026-11-04 09:01:00', {1: ['99']})
        self.conn.execute("UPDATE batch SET state='answered' WHERE id='manual_batch_1'")

        self.assertEqual(self.tick(gateway)['status'], 'queued')

        self.conn.execute("UPDATE dispatch SET status='delivered'")
        register_batch(self.conn, 'auto_batch_0001', 'owner-conversation', 'owner', 'prompt-1',
                       '2026-11-05 10:01:00', {1: ['1']}, enable_automation=True)
        self.conn.execute("UPDATE batch SET state='answered' WHERE id='auto_batch_0001'")
        before = len(gateway.list_calls)

        result = self.tick(gateway, batch_id_factory=lambda: 'auto_batch_0002')

        self.assertEqual(result['status'], 'answered_batch_pending')
        self.assertEqual(len(gateway.list_calls), before)

    def test_unmarked_manual_prepared_dispatch_is_not_auto_sent(self):
        prepare_dispatch(self.conn, 'manual_batch_1', 'owner-conversation', 'owner', [
            {'action_id': '1', 'source_date': '2026-11-05', 'source': source('1')},
        ])
        called: list[str] = []
        result = self.tick(FakeGateway({TODAY: [action('2')]}),
                           preflight=lambda batch: called.append(batch),
                           sender=lambda *_args: called.append('send'),
                           history_reader=lambda *_args: full_history([]))

        self.assertEqual(result['status'], 'manual_dispatch_pending')
        self.assertEqual(called, [])
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM dispatch').fetchone()[0], 1)

