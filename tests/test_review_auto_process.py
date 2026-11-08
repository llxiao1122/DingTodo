import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import review_auto_process as automation
import review_inbox_worker
from review_inbox import connect, poll_batch, register_batch


def owner_reply(text='忽略', message_id='answer'):
    return {'messageId': message_id, 'conversationId': 'conversation', 'senderId': 'owner',
            'createTime': '2026-11-04 10:01:00', 'text': text}


def full_history(messages):
    return {'complete': True, 'partial': False, 'hasMore': False,
            'failures': [], 'messages': messages}


def handoff(batch_id, decision, state, action_id='701'):
    return {'batch': batch_id, 'reply_message_id': 'answer', 'writes_performed': 0,
            'items': [{'ordinal': 1, 'user_decision': decision, 'handoff_state': state,
                       'sources': [{'action_id': action_id, 'conversation_id': 'source-chat',
                                    'message_id': 'source-message'}],
                       'canonical_sources': []}]}


class ReviewAutomationTests(unittest.TestCase):
    def test_bot_identity_must_be_proven_before_sending(self):
        conversation = {'success': True, 'result': {'conversationInfo': {
            'singleChat': True, 'openConversationId': 'conversation'}}}
        robot = {'ok': True, 'data': {'robotStatus': 'ONLINE', 'robotCode': 'robot'}}
        with patch.object(automation, 'validate_owner', return_value='owner-user'), \
             patch.object(automation, 'read_dws', side_effect=[conversation, robot]):
            with self.assertRaisesRegex(ValueError, 'cifra_robot_identity_mismatch'):
                automation._verified_send_context('profile', 'bot', 'app', 'conversation', 'owner')

    def test_historical_binding_requires_digest_and_exact_sender_readback(self):
        import sqlite3
        conn = sqlite3.connect(':memory:')
        conn.execute('CREATE TABLE dispatch (id TEXT, prompt_message_id TEXT, prompt_time TEXT, '
                     'status TEXT, conversation_id TEXT, owner_id TEXT, preview_hash TEXT)')
        conn.execute('INSERT INTO dispatch VALUES (?,?,?,?,?,?,?)',
                     ('batch', 'message', 'time', 'delivered', 'conversation', 'owner', 'hash'))
        context = {'digest': 'hash', 'preview_hash': 'hash', 'text': 'prompt'}
        message = {'messageId': 'message', 'conversationId': 'conversation',
                   'senderId': 'bot', 'text': 'prompt'}
        with patch('review_dispatch_send.verify_context', return_value=context), \
             patch.object(automation, '_read_history', return_value=[message]):
            self.assertTrue(automation._historical_bot_binding(conn, 'profile', 'bot', 'app',
                                                               'conversation', 'owner'))
        with patch('review_dispatch_send.verify_context', return_value=context), \
             patch.object(automation, '_read_history', return_value=[{**message, 'senderId': 'other'}]):
            self.assertFalse(automation._historical_bot_binding(conn, 'profile', 'bot', 'app',
                                                                'conversation', 'owner'))
        conn.close()

    def test_source_title_is_exact_and_refuses_ambiguous_deadline(self):
        item = {'ordinal': 1, 'user_decision': 'create', 'sources': [
            {'action_id': '701', 'source': {'text': '请各工班领取计量器具'}}]}
        plan = automation.source_grounded_plan(item, 'batch', 'reply')
        self.assertEqual(plan['title'], '请各工班领取计量器具')
        item['sources'][0]['source']['text'] = '请各工班领取计量器具 @工班长'
        self.assertEqual(automation.source_grounded_plan(item, 'batch', 'reply')['title'],
                         '请各工班领取计量器具')
        item['sources'][0]['source']['text'] = '请领取计量器具 @工班长 后续须登记'
        self.assertIsNone(automation.source_grounded_plan(item, 'batch', 'reply'))
        item['sources'][0]['source']['text'] = '请明天下班前领取计量器具'
        self.assertIsNone(automation.source_grounded_plan(item, 'batch', 'reply'))
        item['sources'][0]['source']['text'] = '请周五领取计量器具'
        self.assertIsNone(automation.source_grounded_plan(item, 'batch', 'reply'))

    def test_source_plan_binds_explicit_deadline_to_message_date(self):
        item = {'ordinal': 1, 'user_decision': 'create', 'sources': [
            {'action_id': '701', 'source': {'text': '请明天15:00前更新台账',
                                             'message_time': '2026-11-05 09:00:00'}}]}
        plan = automation.source_grounded_plan(item, 'batch', 'reply')
        self.assertEqual(plan['due_iso'], '2026-11-06T15:00:00+08:00')
        item['sources'][0]['source']['message_time'] = ''
        self.assertIsNone(automation.source_grounded_plan(item, 'batch', 'reply'))

    def test_forged_deadline_plan_never_calls_resolver(self):
        item = {'ordinal': 1, 'user_decision': 'create',
                'handoff_state': 'ready_for_existing_worker_review', 'sources': [
                    {'action_id': '701', 'source': {'text': '请明天15:00前更新台账',
                                                     'message_time': '2026-11-05 09:00:00'}}]}
        plan = automation.source_grounded_plan(item, 'batch_12345678', 'answer')
        plan['due_iso'] = '2026-11-07T15:00:00+08:00'
        with patch('review_resolve_create.resolve') as resolver:
            result = automation._apply_create(self.conn, 'batch_12345678', item, 'profile',
                                              'bot', 'lock', lambda _item: plan, 'answer')
        self.assertEqual(result['reason'], 'create_deadline_not_source_bound')
        resolver.assert_not_called()

    def test_explicit_deadline_reaches_create_resolver(self):
        item = {'ordinal': 1, 'user_decision': 'create',
                'handoff_state': 'ready_for_existing_worker_review', 'sources': [
                    {'action_id': '701', 'source': {'text': '请明天15:00前更新台账',
                                                     'message_time': '2026-11-05 09:00:00'}}]}
        plan = automation.source_grounded_plan(item, 'batch_12345678', 'answer')
        with patch('review_resolve_create.resolve',
                   return_value={'status': 'applied', 'external_task_id': 'task'}) as resolver:
            result = automation._apply_create(self.conn, 'batch_12345678', item, 'profile',
                                              'bot', 'lock', lambda _item: plan, 'answer')
        self.assertEqual(result['status'], 'applied')
        self.assertEqual(resolver.call_args.kwargs['due_iso'], '2026-11-06T15:00:00+08:00')

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = str(Path(self.temp_dir.name) / 'inbox.sqlite3')
        self.conn = connect(self.database)
        register_batch(self.conn, 'batch_12345678', 'conversation', 'owner', 'prompt',
                       '2026-11-04 10:00:00', {1: ['701']})
        poll_batch(self.conn, 'batch_12345678', full_history([owner_reply()]))
        automation.ensure_schema(self.conn)
        self.conn.execute("INSERT INTO review_auto_enabled VALUES (?, ?)",
                          ('batch_12345678', '2026-11-04 10:00:00'))
        self.history = [owner_reply()]

    def tearDown(self):
        self.conn.close()
        self.temp_dir.cleanup()

    def test_historical_batch_is_not_automatically_processed(self):
        self.conn.execute('DELETE FROM review_auto_enabled')
        with patch.object(automation, '_send_once') as send:
            result = automation.process_answered_batch(
                self.conn, 'batch_12345678', 'profile', 'bot', 'app', 'lock')
        self.assertEqual(result['reason'], 'batch_automation_not_enabled')
        send.assert_not_called()

    def test_handoff_verification_failure_is_reported_after_ack(self):
        sent = []
        def send_once(_conn, _batch, kind, text, *_args):
            sent.append((kind, text))
            return 'delivered'
        common = self._common_mocks(send_once)
        with common[0], common[1], common[2]:
            result = automation.process_answered_batch(
                self.conn, 'batch_12345678', 'profile', 'bot', 'app', 'lock',
                handoff_reader=lambda *_args: (_ for _ in ()).throw(ValueError('source_changed')))
        self.assertEqual(result['reason'], 'handoff_verification_failed')
        self.assertEqual(sent, [('ack', '收到'), ('partial', automation.PENDING_TEXT)])

    def test_failure_of_one_answered_batch_does_not_block_another(self):
        register_batch(self.conn, 'batch_new123', 'conversation', 'owner', 'prompt-new',
                       '2026-11-04 11:00:00', {1: ['702']})
        self.conn.execute("UPDATE batch SET state='answered' WHERE id='batch_new123'")
        with patch.object(review_inbox_worker, 'process_answered_batch',
                          side_effect=[ValueError('old bad evidence'), {'status': 'complete'}]) as process:
            result = review_inbox_worker.run(self.conn, 'profile', 'bot', 'app', 'lock')
        self.assertEqual(process.call_count, 2)
        self.assertEqual(result['status'], 'pending')
        self.assertEqual(result['batches'][0]['reason'], 'batch_processing_failed')
        self.assertEqual(result['batches'][1]['status'], 'complete')

    def test_notice_readback_accepts_surrounding_formatting_whitespace(self):
        self.conn.execute("INSERT INTO review_auto_notice "
                          "(batch_id,kind,text,status,claimed_at,baseline_message_ids) "
                          "VALUES (?,?,?,?,?,?)",
                          ('batch_12345678', 'ack', '收到', 'sending',
                           '2026-11-04 10:02:00', '[]'))
        message = {'conversationId': 'conversation', 'senderId': 'bot',
                   'messageId': 'notice', 'text': '\n收到\n',
                   'createTime': '2026-11-04 10:02:01'}
        self.assertEqual(automation._reconcile_notice(self.conn, 'batch_12345678',
                                                     'ack', 'bot', [message]), 'delivered')

    def test_multiple_matching_notices_remain_unknown(self):
        self.conn.execute("INSERT INTO review_auto_notice "
                          "(batch_id,kind,text,status,claimed_at,baseline_message_ids) "
                          "VALUES (?,?,?,?,?,?)",
                          ('batch_12345678', 'ack', '收到', 'sending',
                           '2026-11-04 10:02:00', '[]'))
        message = {'conversationId': 'conversation', 'senderId': 'bot',
                   'messageId': 'notice-one', 'text': '收到',
                   'createTime': '2026-11-04 10:02:01'}
        self.assertEqual(automation._reconcile_notice(
            self.conn, 'batch_12345678', 'ack', 'bot',
            [message, {**message, 'messageId': 'notice-two'}]), 'unknown')
        self.assertEqual(self.conn.execute('SELECT status FROM review_auto_notice').fetchone()[0], 'unknown')

    def test_new_opted_in_batch_is_registered_with_automation(self):
        conn = connect(str(Path(self.temp_dir.name) / 'new.sqlite3'))
        try:
            register_batch(conn, 'batch_new123', 'conversation', 'owner', 'prompt',
                           '2026-11-04 10:00:00', {1: ['702']}, enable_automation=True)
            self.assertEqual(conn.execute('SELECT batch_id FROM review_auto_enabled').fetchone(),
                             ('batch_new123',))
        finally:
            conn.close()

    def _common_mocks(self, send_once):
        return (
            patch.object(automation, '_verified_send_context', return_value=('owner-user', 'cifra-robot')),
            patch.object(automation, '_read_history', return_value=self.history),
            patch.object(automation, '_send_once', side_effect=send_once),
        )

    def test_explicit_ignore_is_applied_after_ack_and_sends_no_completion_notice(self):
        sent = []
        calls = 0

        def read_handoff(*_args):
            nonlocal calls
            calls += 1
            state = 'ready_for_existing_worker_review' if calls == 1 else 'resolved_ignore'
            return handoff('batch_12345678', 'ignore', state)

        def fake_ignore(conn, batch, ordinal, *_args, **_kwargs):
            conn.execute('INSERT INTO resolution '
                         '(batch_id,ordinal,action_id,decision,reply_message_id,request_json,status) '
                         'VALUES (?,?,?,?,?,?,?)',
                         (batch, ordinal, '701', 'ignore', 'answer', '{}', 'applied'))
            return {'status': 'applied'}

        def send_once(_conn, _batch, kind, text, *_args):
            sent.append((kind, text))
            return 'delivered'

        common = self._common_mocks(send_once)
        with common[0], common[1], common[2], \
             patch.object(automation, 'validate_owner', return_value='owner-user'), \
             patch('review_resolve_ignore.resolve', side_effect=fake_ignore):
            result = automation.process_answered_batch(
                self.conn, 'batch_12345678', 'profile', 'bot', 'app',
                str(Path(self.temp_dir.name) / 'lock'), handoff_reader=read_handoff)

        self.assertEqual(result['status'], 'all_ignored')
        self.assertEqual(sent, [('ack', '收到')])
        self.assertEqual(result['pending_ordinals'], [])
        with patch.object(automation, '_read_history', side_effect=AssertionError('must not reread')):
            replay = automation.process_answered_batch(
                self.conn, 'batch_12345678', 'profile', 'bot', 'app', 'lock')
        self.assertEqual(replay['status'], 'all_ignored')

    def test_create_without_confirmed_title_stays_pending_and_reports_no_completion(self):
        self.conn.execute('DELETE FROM reply')
        self.conn.execute('DELETE FROM reply_candidate')
        self.conn.execute("UPDATE batch SET state='open' WHERE id='batch_12345678'")
        self.assertEqual(poll_batch(self.conn, 'batch_12345678',
                                    full_history([owner_reply('创建待办提醒我，标注测试用')])), 'recorded')
        self.history = [owner_reply('创建待办提醒我，标注测试用')]
        sent = []

        def send_once(_conn, _batch, kind, text, *_args):
            sent.append((kind, text))
            return 'delivered'

        item = handoff('batch_12345678', 'create', 'ready_for_existing_worker_review')
        with patch.object(automation, '_verified_send_context', return_value=('owner-user', 'cifra-robot')), \
             patch.object(automation, '_read_history', return_value=self.history), \
             patch.object(automation, '_send_once', side_effect=send_once), \
             patch.object(automation, 'validate_owner', return_value='owner-user'), \
             patch('review_resolve_create.resolve') as create_resolver:
            result = automation.process_answered_batch(
                self.conn, 'batch_12345678', 'profile', 'bot', 'app',
                str(Path(self.temp_dir.name) / 'lock'), handoff_reader=lambda *_args: item)

        self.assertEqual(result['status'], 'pending')
        self.assertEqual(result['pending_ordinals'], [1])
        self.assertEqual(result['items'][0]['reason'],
                         'create_requires_owner_confirmed_title_and_duplicate_query')
        self.assertEqual(sent, [('ack', '收到'), ('partial', automation.PENDING_TEXT)])
        create_resolver.assert_not_called()

    def test_applying_create_with_task_id_reconciles_without_requesting_a_new_plan(self):
        self.conn.execute('DELETE FROM reply')
        self.conn.execute('DELETE FROM reply_candidate')
        self.conn.execute("UPDATE batch SET state='open' WHERE id='batch_12345678'")
        reply = owner_reply('创建待办提醒我，标注测试用')
        poll_batch(self.conn, 'batch_12345678', full_history([reply]))
        self.history = [reply]
        audit = {
            'claim': {'action_id': 701, 'conversation_id': 'source-chat',
                      'message_id': 'source-message'},
            'title': '提醒我检查通知', 'executor_user_id': 'owner-user',
            'source_message_id': 'source-message', 'reply_message_id': 'answer',
            'contact_identity': {'name': 'Owner'},
        }
        self.conn.execute(
            'INSERT INTO resolution '
            '(batch_id,ordinal,action_id,decision,reply_message_id,request_json,status,external_task_id) '
            'VALUES (?,?,?,?,?,?,?,?)',
            ('batch_12345678', 1, '701', 'create', 'answer',
             json.dumps(audit), 'applying', 'task-701'),
        )
        source = {'action_id': '701', 'conversation_id': 'source-chat',
                  'message_id': 'source-message', 'external_task_id': 'task-701'}
        calls = 0
        sent = []

        def read_handoff(*_args):
            nonlocal calls
            calls += 1
            state = 'action_state_requires_reconciliation' if calls == 1 else 'resolved_task'
            return {'batch': 'batch_12345678', 'reply_message_id': 'answer',
                    'items': [{'ordinal': 1, 'user_decision': 'create',
                               'handoff_state': state, 'sources': [source],
                               'canonical_sources': []}]}

        def fake_reconcile(conn, batch_id, ordinal, profile, bot_id, lock_path,
                           title, query, apply=False, reconcile=False):
            self.assertEqual((batch_id, ordinal, title, query),
                             ('batch_12345678', 1, '提醒我检查通知', '提醒我检查通知'))
            self.assertTrue(apply)
            self.assertTrue(reconcile)
            conn.execute("UPDATE resolution SET status='applied' "
                         'WHERE batch_id=? AND ordinal=?', (batch_id, ordinal))
            return {'status': 'applied', 'external_task_id': 'task-701'}

        def send_once(_conn, _batch, kind, text, *_args):
            sent.append((kind, text))
            return 'delivered'

        common = self._common_mocks(send_once)
        with common[0], common[1], common[2], \
             patch.object(automation, '_verify_created_task') as task_readback, \
             patch('review_resolve_create.resolve', side_effect=fake_reconcile) as resolver:
            result = automation.process_answered_batch(
                self.conn, 'batch_12345678', 'profile', 'bot', 'app',
                str(Path(self.temp_dir.name) / 'lock'),
                create_plan_provider=lambda _item: self.fail('reconciliation must reuse the saved plan'),
                handoff_reader=read_handoff)

        self.assertEqual(result['status'], 'complete')
        self.assertEqual(sent, [('ack', '收到'), ('complete', '处理已完成')])
        resolver.assert_called_once()
        self.assertEqual(resolver.call_args.kwargs, {'apply': True, 'reconcile': True})
        task_readback.assert_called_once_with(self.conn, 'batch_12345678', 1, 'profile', 'task-701')

    def test_applying_create_without_task_id_does_not_enter_resolver(self):
        audit = {
            'claim': {'action_id': 701, 'conversation_id': 'source-chat',
                      'message_id': 'source-message'},
            'title': '提醒我检查通知', 'source_message_id': 'source-message',
            'reply_message_id': 'answer',
        }
        self.conn.execute(
            'INSERT INTO resolution '
            '(batch_id,ordinal,action_id,decision,reply_message_id,request_json,status,external_task_id) '
            'VALUES (?,?,?,?,?,?,?,NULL)',
            ('batch_12345678', 1, '701', 'create', 'answer',
             json.dumps(audit), 'applying'),
        )
        item = handoff('batch_12345678', 'create', 'action_state_requires_reconciliation')
        with patch('review_resolve_create.resolve') as resolver:
            result = automation._apply_create(
                self.conn, 'batch_12345678', item['items'][0], 'profile', 'bot', 'lock',
                plan_provider=lambda _item: self.fail('a task ID is required before reconciliation'),
                reply_message_id='answer')

        self.assertEqual(result['status'], 'pending')
        self.assertEqual(result['reason'], 'prior_resolution_needs_reconciliation')
        resolver.assert_not_called()

    def test_applying_create_with_mismatched_source_binding_is_not_reconciled(self):
        audit = {
            'claim': {'action_id': 701, 'conversation_id': 'source-chat',
                      'message_id': 'source-message'},
            'title': '提醒我检查通知', 'source_message_id': 'source-message',
            'reply_message_id': 'answer',
        }
        self.conn.execute(
            'INSERT INTO resolution '
            '(batch_id,ordinal,action_id,decision,reply_message_id,request_json,status,external_task_id) '
            'VALUES (?,?,?,?,?,?,?,?)',
            ('batch_12345678', 1, '701', 'create', 'answer',
             json.dumps(audit), 'applying', 'task-701'),
        )
        item = handoff('batch_12345678', 'create', 'action_state_requires_reconciliation')
        item['items'][0]['sources'][0]['external_task_id'] = 'task-701'
        item['items'][0]['sources'][0]['message_id'] = 'different-source-message'
        with patch('review_resolve_create.resolve') as resolver:
            result = automation._apply_create(
                self.conn, 'batch_12345678', item['items'][0], 'profile', 'bot', 'lock',
                plan_provider=lambda _item: self.fail('a bound applying resolution is required'),
                reply_message_id='answer')

        self.assertEqual(result['status'], 'pending')
        self.assertEqual(result['reason'], 'prior_resolution_source_binding_mismatch')
        resolver.assert_not_called()

    def test_human_confirmed_create_plan_is_bound_to_exact_item_and_requires_readback(self):
        self.conn.execute('DELETE FROM reply')
        self.conn.execute('DELETE FROM reply_candidate')
        self.conn.execute("UPDATE batch SET state='open' WHERE id='batch_12345678'")
        reply = owner_reply('创建待办提醒我，标注测试用')
        poll_batch(self.conn, 'batch_12345678', full_history([reply]))
        self.history = [reply]
        calls = 0
        sent = []

        def read_handoff(*_args):
            nonlocal calls
            calls += 1
            state = 'ready_for_existing_worker_review' if calls == 1 else 'resolved_task'
            return handoff('batch_12345678', 'create', state)

        def confirmed_plan(_item):
            return {'owner_confirmed': True, 'batch_id': 'batch_12345678', 'ordinal': 1,
                    'action_id': '701', 'reply_message_id': 'answer', 'title': '提醒我检查通知',
                    'duplicate_query': '检查通知'}

        def fake_create(conn, batch, ordinal, _profile, _bot, _lock, title, query, apply=False):
            self.assertEqual((title, query, apply), ('提醒我检查通知', '检查通知', True))
            conn.execute('INSERT INTO resolution '
                         '(batch_id,ordinal,action_id,decision,reply_message_id,request_json,status,external_task_id) '
                         'VALUES (?,?,?,?,?,?,?,?)',
                         (batch, ordinal, '701', 'create', 'answer',
                          '{"title":"提醒我检查通知","executor_user_id":"owner-user",'
                          '"contact_identity":{"name":"Owner"}}', 'applied', 'task-701'))
            return {'status': 'applied', 'external_task_id': 'task-701'}

        def send_once(_conn, _batch, kind, text, *_args):
            sent.append((kind, text))
            return 'delivered'

        with patch.object(automation, '_verified_send_context', return_value=('owner-user', 'cifra-robot')), \
             patch.object(automation, '_read_history', return_value=self.history), \
             patch.object(automation, '_send_once', side_effect=send_once), \
             patch.object(automation, 'validate_owner', return_value='owner-user'), \
             patch.object(automation, '_verify_created_task') as task_readback, \
             patch('review_resolve_create.resolve', side_effect=fake_create):
            result = automation.process_answered_batch(
                self.conn, 'batch_12345678', 'profile', 'bot', 'app',
                str(Path(self.temp_dir.name) / 'lock'), create_plan_provider=confirmed_plan,
                handoff_reader=read_handoff)

        self.assertEqual(result['status'], 'complete')
        self.assertEqual(sent, [('ack', '收到'), ('complete', '处理已完成')])
        self.assertEqual(self.conn.execute('SELECT status FROM review_auto_terminal').fetchone()[0],
                         'complete')
        task_readback.assert_called_once_with(self.conn, 'batch_12345678', 1, 'profile', 'task-701')

    def test_create_plan_must_match_ordinal_action_and_reply(self):
        plan = {'owner_confirmed': True, 'batch_id': 'batch_12345678', 'ordinal': 2,
                'action_id': '701', 'reply_message_id': 'answer', 'title': 'title',
                'duplicate_query': 'title'}
        self.assertFalse(automation._create_plan_is_bound(plan, 'batch_12345678', 1, '701', 'answer'))
        self.assertFalse(automation._create_plan_is_bound(
            {**plan, 'ordinal': 1, 'action_id': '702'}, 'batch_12345678', 1, '701', 'answer'))
        self.assertFalse(automation._create_plan_is_bound(
            {**plan, 'ordinal': 1, 'owner_confirmed': False}, 'batch_12345678', 1, '701', 'answer'))
        self.assertTrue(automation._create_plan_is_bound(
            {**plan, 'ordinal': 1}, 'batch_12345678', 1, '701', 'answer'))

    def test_unknown_send_reconciles_without_blind_retry(self):
        history = []
        with patch.object(automation, '_read_history', side_effect=lambda *_args: history), \
             patch.object(automation, 'read_dws', side_effect=RuntimeError('uncertain')) as sender:
            first = automation._send_once(
                self.conn, 'batch_12345678', 'ack', automation.ACK_TEXT, 'profile', 'bot',
                'app', 'owner-user', 'cifra-robot', '2026-11-04 10:00:00')
            self.assertEqual(first, 'unknown')
            claim_time = self.conn.execute(
                "SELECT claimed_at FROM review_auto_notice WHERE batch_id=? AND kind='ack'",
                ('batch_12345678',)).fetchone()[0]
            history[:] = [{'messageId': 'ack-message', 'conversationId': 'conversation',
                           'senderId': 'bot', 'createTime': claim_time, 'text': '收到'}]
            second = automation._send_once(
                self.conn, 'batch_12345678', 'ack', automation.ACK_TEXT, 'profile', 'bot',
                'app', 'owner-user', 'cifra-robot', '2026-11-04 10:00:00')
            third = automation._send_once(
                self.conn, 'batch_12345678', 'ack', automation.ACK_TEXT, 'profile', 'bot',
                'app', 'owner-user', 'cifra-robot', '2026-11-04 10:00:00')

        self.assertEqual((second, third), ('delivered', 'delivered'))
        sender.assert_called_once()

    def test_multiple_owner_replies_are_not_acknowledged(self):
        messages = [owner_reply(), owner_reply('忽略', message_id='other')]
        with self.assertRaisesRegex(ValueError, 'multiple_replies_need_review'):
            automation._verified_reply(self.conn, 'batch_12345678', messages)

    def test_missing_app_identity_keeps_reply_pending_without_network_calls(self):
        with patch.object(automation, '_verified_send_context') as context:
            result = automation.process_answered_batch(
                self.conn, 'batch_12345678', 'profile', 'bot', None,
                str(Path(self.temp_dir.name) / 'lock'))
        self.assertEqual(result['status'], 'pending')
        self.assertEqual(result['reason'], 'cifra_app_id_not_configured')
        context.assert_not_called()


class ClarificationTests(unittest.TestCase):
    batch_id = 'batch_12345678'
    prompt_time = '2026-11-04 10:00:00'

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.conn = connect(str(Path(self.temp_dir.name) / 'clarification.sqlite3'))
        register_batch(self.conn, self.batch_id, 'conversation', 'owner', 'prompt',
                       self.prompt_time, {1: ['701']}, enable_automation=True)
        self.bot_prompt = {'messageId': 'prompt', 'conversationId': 'conversation',
                           'senderId': 'bot', 'createTime': self.prompt_time,
                           'text': 'review prompt'}
        self.ambiguous = owner_reply('maybe', 'uncertain')

    def tearDown(self):
        self.conn.close()
        self.temp_dir.cleanup()

    def _payload(self, messages):
        return full_history(messages)

    def _record_clarification(self, prompt_time='2026-11-04 10:02:00'):
        text = automation._clarification_text(self.batch_id, 1)
        self.conn.execute(
            'INSERT INTO review_clarification '
            '(batch_id,prompt_text,status,claimed_at,baseline_message_ids,bot_id,prompt_message_id,prompt_time) '
            'VALUES (?,?,?,?,?,?,?,?)',
            (self.batch_id, text, 'delivered', '2026-11-04 10:01:30', '["prompt","uncertain"]',
             'bot', 'clarification', prompt_time),
        )
        return {'messageId': 'clarification', 'conversationId': 'conversation',
                'senderId': 'bot', 'createTime': prompt_time, 'text': text}

    def test_ambiguous_reply_is_acknowledged_once_then_clarified(self):
        history = self._payload([self.bot_prompt, self.ambiguous])
        self.assertEqual(poll_batch(self.conn, self.batch_id, history), 'reply_needs_review')
        calls = []

        def send_ack(_conn, _batch, kind, text, *_args):
            calls.append(('ack', kind, text))
            return 'delivered'

        def send_clarification(_conn, _batch, text, *_args):
            calls.append(('clarification', text))
            return 'delivered'

        with patch.object(automation, '_verified_send_context', return_value=('owner-user', 'robot')), \
             patch.object(automation, '_send_once', side_effect=send_ack), \
             patch.object(automation, '_send_clarification_once', side_effect=send_clarification):
            result = automation.process_needs_review_batch(
                self.conn, self.batch_id, 'profile', 'bot', 'app', history)

        self.assertEqual(result['status'], 'clarification_sent')
        self.assertEqual(calls[0], ('ack', 'ack', automation.ACK_TEXT))
        self.assertEqual(calls[1][0], 'clarification')
        self.assertIn(self.batch_id, calls[1][1])
        self.assertEqual(self.conn.execute(
            'SELECT content FROM reply_candidate WHERE message_id=?', ('uncertain',)
        ).fetchone()[0], 'maybe')

    def test_clarification_accepts_only_one_new_owner_reply_and_keeps_old_original(self):
        self.assertEqual(poll_batch(self.conn, self.batch_id,
                                    self._payload([self.bot_prompt, self.ambiguous])), 'reply_needs_review')
        clarification = self._record_clarification()
        followup = owner_reply('1，忽略', 'clarified')
        followup['createTime'] = '2026-11-04 10:03:00'
        history = [self.bot_prompt, self.ambiguous, clarification, followup]

        self.assertEqual(poll_batch(self.conn, self.batch_id, self._payload(history)), 'recorded')
        self.assertEqual(self.conn.execute('SELECT content FROM reply WHERE batch_id=?',
                                           (self.batch_id,)).fetchone()[0], '1，忽略')
        self.assertEqual(self.conn.execute(
            'SELECT content FROM reply_candidate WHERE message_id=?', ('uncertain',)
        ).fetchone()[0], 'maybe')
        verified = automation._verified_reply(self.conn, self.batch_id, history)
        self.assertEqual(verified['reply_message_id'], 'clarified')
        self.assertEqual(verified['decisions'], {1: 'ignore'})

    def test_wrong_batch_marker_same_second_and_multiple_followups_stay_unanswered(self):
        cases = [
            ('wrong marker', '复核编号：another_batch；1，忽略', '2026-11-04 10:03:00', 'reply_needs_review'),
            ('same second', '1，忽略', '2026-11-04 10:02:00', 'waiting_for_clarification'),
        ]
        for name, text, created, expected in cases:
            with self.subTest(name=name):
                self.conn.execute('DELETE FROM review_clarification')
                self.conn.execute('DELETE FROM reply_candidate')
                self.conn.execute('DELETE FROM reply')
                self.conn.execute("UPDATE batch SET state='open' WHERE id=?", (self.batch_id,))
                self.assertEqual(poll_batch(self.conn, self.batch_id,
                                            self._payload([self.bot_prompt, self.ambiguous])), 'reply_needs_review')
                clarification = self._record_clarification()
                followup = owner_reply(text, 'followup-' + name.replace(' ', '-'))
                followup['createTime'] = created
                status = poll_batch(self.conn, self.batch_id,
                                    self._payload([self.bot_prompt, self.ambiguous, clarification, followup]))
                self.assertEqual(status, expected)
                self.assertEqual(self.conn.execute('SELECT state FROM batch WHERE id=?',
                                                   (self.batch_id,)).fetchone()[0], 'open')
                self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM reply').fetchone()[0], 0)

    def test_multiple_followups_after_clarification_are_never_selected(self):
        self.assertEqual(poll_batch(self.conn, self.batch_id,
                                    self._payload([self.bot_prompt, self.ambiguous])), 'reply_needs_review')
        clarification = self._record_clarification()
        first = owner_reply('1，忽略', 'first-followup')
        second = owner_reply('1，创建', 'second-followup')
        first['createTime'] = '2026-11-04 10:03:00'
        second['createTime'] = '2026-11-04 10:04:00'

        self.assertEqual(poll_batch(
            self.conn, self.batch_id, self._payload([self.bot_prompt, self.ambiguous, clarification, first, second])
        ), 'multiple_replies_need_review')
        self.assertEqual(self.conn.execute('SELECT state FROM batch WHERE id=?',
                                           (self.batch_id,)).fetchone()[0], 'open')
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM reply').fetchone()[0], 0)

    def test_execution_verification_rejects_a_late_extra_reply(self):
        self.assertEqual(poll_batch(self.conn, self.batch_id,
                                    self._payload([self.bot_prompt, self.ambiguous])), 'reply_needs_review')
        clarification = self._record_clarification()
        followup = owner_reply('1，忽略', 'clarified')
        followup['createTime'] = '2026-11-04 10:03:00'
        self.assertEqual(poll_batch(self.conn, self.batch_id,
                                    self._payload([self.bot_prompt, self.ambiguous, clarification, followup])),
                         'recorded')
        extra = owner_reply('1，创建', 'late-extra')
        extra['createTime'] = '2026-11-04 10:04:00'
        with self.assertRaisesRegex(ValueError, 'reply_evidence_mismatch|multiple_replies_need_review'):
            automation._verified_reply(
                self.conn, self.batch_id, [self.bot_prompt, self.ambiguous, clarification, followup, extra])


if __name__ == '__main__':
    unittest.main()
