import sqlite3
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from review_inbox import (claim_dispatch, connect, ingest_reply, mark_dispatch_unknown, parse_reply,
                          poll_batch, prepare_from_reviews, reconcile_dispatch, register_batch)
import review_dispatch_send
import review_inbox_worker
import review_identity
import review_resolve_ignore
from review_handoff import build_handoff


def message(**updates):
    data = {'messageId': 'answer', 'conversationId': 'conversation', 'senderId': 'owner',
            'createTime': '2026-10-31 17:27:15',
            'text': '1，忽略，2，这条创建待办，执行人是我，3，创建，执行人是我'}
    return {**data, **updates}


def payload(messages):
    return {'complete': True, 'partial': False, 'hasMore': False, 'failures': [], 'messages': messages}


class InboxTests(unittest.TestCase):
    def test_single_explicit_create_without_executor_is_a_decision(self):
        self.assertEqual(parse_reply('创建待办', 1), {1: 'create'})
        with self.assertRaises(ValueError):
            parse_reply('可能需要创建待办', 1)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.conn = connect(str(Path(self.tmp.name) / 'inbox.db'))
        register_batch(self.conn, 'batch', 'conversation', 'owner', 'prompt',
                       '2026-10-31 17:26:22', {1: ['751'], 2: ['762'], 3: ['742', '743', '744']})

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def test_exact_reply_and_duplicate(self):
        self.assertEqual(poll_batch(self.conn, 'batch', payload([message()])), 'recorded')
        self.assertEqual(ingest_reply(self.conn, 'batch', message()), 'duplicate')
        self.assertEqual(poll_batch(self.conn, 'batch', payload([])), 'already_answered')
        with self.assertRaisesRegex(ValueError, 'reply_identity_conflict'):
            ingest_reply(self.conn, 'batch', message(text='different'))

    def test_wrong_sender_and_incomplete_read(self):
        self.assertEqual(poll_batch(self.conn, 'batch', payload([message(senderId='other')])), 'waiting')
        with self.assertRaisesRegex(ValueError, 'incomplete_message_read'):
            poll_batch(self.conn, 'batch', {**payload([message()]), 'complete': False})
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM reply').fetchone()[0], 0)

    def test_ambiguous_and_multiple_replies(self):
        self.assertEqual(poll_batch(self.conn, 'batch', payload([message(messageId='uncertain', text='第二条先不要创建')])), 'reply_needs_review')
        self.assertEqual(poll_batch(self.conn, 'batch', payload([message(), message(messageId='second')])), 'multiple_replies_need_review')
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM reply').fetchone()[0], 0)
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM reply_candidate').fetchone()[0], 3)

    def test_wrong_conversation_and_second_open_batch(self):
        with self.assertRaisesRegex(ValueError, 'unauthorized_reply'):
            ingest_reply(self.conn, 'batch', message(conversationId='other'))
        with self.assertRaises(sqlite3.IntegrityError):
            register_batch(self.conn, 'other', 'conversation', 'owner', 'prompt2',
                           '2026-10-31 17:26:22', {1: ['123']})

    def test_ambiguous_language(self):
        for text in ('1，忽略，2，创建', '1，暂不忽略，2，创建，3，创建', '1，忽略，2，不创建也创建，3，创建'):
            with self.subTest(text=text), self.assertRaises(ValueError):
                parse_reply(text, 3)
        self.assertEqual(parse_reply('创建待办提醒我，标注测试用', 1), {1: 'create'})
        self.assertEqual(parse_reply('无需创建待办', 1), {1: 'ignore'})
        with self.assertRaises(ValueError):
            parse_reply('创建待办提醒我，标注测试用', 2)

    def test_worker_uses_bound_conversation_and_completeness(self):
        info = {'success': True, 'result': {'conversationInfo': {
            'singleChat': True, 'openConversationId': 'conversation'}}}
        with patch.object(review_inbox_worker, 'validate_owner'), \
             patch.object(review_inbox_worker, 'process_answered_batch',
                          return_value={'status': 'pending', 'reason': 'mock'}), \
             patch.object(review_inbox_worker, 'read_dws', side_effect=[info, payload([message()])]):
            result = review_inbox_worker.run(self.conn, 'profile', 'bot', 'app')
            self.assertEqual(result['poll_status'], 'recorded')
            self.assertEqual(result['status'], 'pending')
        with patch.object(review_inbox_worker, 'read_dws') as read:
            with patch.object(review_inbox_worker, 'process_answered_batch',
                              return_value={'status': 'pending', 'reason': 'mock'}):
                self.assertEqual(review_inbox_worker.run(self.conn, 'profile', 'bot', 'app')['status'], 'pending')
            read.assert_not_called()

    def test_worker_rejects_wrong_bot_conversation(self):
        info = {'success': True, 'result': {'conversationInfo': {
            'singleChat': True, 'openConversationId': 'different'}}}
        with patch.object(review_inbox_worker, 'validate_owner'), \
             patch.object(review_inbox_worker, 'read_dws', return_value=info):
            with self.assertRaisesRegex(ValueError, 'conversation_identity_mismatch'):
                review_inbox_worker.run(self.conn, 'profile', 'bot')

    def test_same_second_or_invalid_time_does_not_decide(self):
        same_second = message(createTime='2026-10-31 17:26:22')
        self.assertEqual(poll_batch(self.conn, 'batch', payload([same_second])), 'reply_needs_review')
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM reply_candidate').fetchone()[0], 1)
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM reply').fetchone()[0], 0)
        with self.assertRaisesRegex(ValueError, 'invalid_message_time_or_row'):
            poll_batch(self.conn, 'batch', payload([message(createTime='bad')]))

    def test_handoff_preserves_duplicate_conflict_and_reply_identity(self):
        ingest_reply(self.conn, 'batch', message())
        ids = ['751', '762', '742', '743', '744', '345']
        rows = [{'id': int(x), 'conversation_id': 'c'+x, 'message_id': 'm'+x,
                 'status': 'REVIEW', 'effective_external_task_id': None,
                 'duplicate_of_action_id': 345 if x in ('742','743','744') else None} for x in ids]
        class Adapter:
            def query(self, request):
                if request['view'] == 'actions':
                    return {'view': 'actions', 'rows': rows, 'returned_count': len(rows),
                            'has_more': False, 'next_page_token': None}
                source = {'conversation_id': request['conversation_id'], 'message_id': request['message_id'],
                          'text': 'source', 'group_name': 'group'}
                return {'view': 'message', 'rows': [source], 'returned_count': 1,
                        'matched_count': 1, 'has_more': False}
        result = build_handoff(self.conn, Adapter(), 'batch', message())
        self.assertEqual([x['handoff_state'] for x in result['items']],
                         ['ready_for_existing_worker_review', 'ready_for_existing_worker_review',
                          'duplicate_link_requires_review'])
        self.assertEqual(result['items'][2]['canonical_sources'][0]['action_id'], '345')
        self.assertEqual(result['writes_performed'], 0)
        with self.assertRaisesRegex(ValueError, 'reply_evidence_mismatch'):
            build_handoff(self.conn, Adapter(), 'batch', message(senderId='other'))

    def test_resolved_duplicate_family_and_completed_decisions_are_reported(self):
        ingest_reply(self.conn, 'batch', message())
        sources = {
            '751': {'conversation_id': 'c751', 'message_id': 'm751', 'text': '通知1',
                    'group_name': '群1', 'sender_name': '张三', 'message_time': '2026-10-29T08:00:00'},
            '762': {'conversation_id': 'c762', 'message_id': 'm762', 'text': '通知2',
                    'group_name': '群2', 'sender_name': '张三', 'message_time': '2026-10-29T09:00:00'},
        }
        base = '通知：为规范打卡管理，科室全员每日上下班打卡须露出全脸。各工班长对班组人员传达到位@所有人 @工班长'
        notice_rows = [
            {'id': 742, 'conversation_id': 'c742', 'message_id': 'm742', 'text': base,
             'group_name': '群3', 'sender_name': '张三', 'message_time': '2026-10-28T14:36:36'},
            {'id': 743, 'conversation_id': 'c743', 'message_id': 'm743', 'text': base,
             'group_name': '群4', 'sender_name': '张三', 'message_time': '2026-10-28T14:37:16'},
            {'id': 744, 'conversation_id': 'c744', 'message_id': 'm744', 'text': '[DING]' + base.replace(' @工班长', ''),
             'group_name': '群5', 'sender_name': '李林骁', 'message_time': '2026-10-28T14:44:10'},
        ]
        sources.update({str(row['id']): row for row in notice_rows})
        rows = [
            {'id': 751, 'conversation_id': 'c751', 'message_id': 'm751', 'status': 'IGNORED',
             'effective_external_task_id': None, 'duplicate_of_action_id': None},
            {'id': 762, 'conversation_id': 'c762', 'message_id': 'm762', 'status': 'SUCCESS',
             'effective_external_task_id': 'task-762', 'duplicate_of_action_id': None},
            {'id': 742, 'conversation_id': 'c742', 'message_id': 'm742', 'status': 'SUCCESS',
             'effective_external_task_id': 'task-742', 'duplicate_of_action_id': None},
            {'id': 743, 'conversation_id': 'c743', 'message_id': 'm743', 'status': 'REVIEW',
             'effective_external_task_id': 'task-742', 'duplicate_of_action_id': 742},
            {'id': 744, 'conversation_id': 'c744', 'message_id': 'm744', 'status': 'REVIEW',
             'effective_external_task_id': 'task-742', 'duplicate_of_action_id': 742},
        ]

        class Adapter:
            def query(self, request):
                if request['view'] == 'actions':
                    return {'view': 'actions', 'rows': rows, 'returned_count': len(rows),
                            'has_more': False, 'next_page_token': None}
                result = next((row for row in sources.values()
                               if row['conversation_id'] == request['conversation_id']
                               and row['message_id'] == request['message_id']), None)
                return {'view': 'message', 'rows': [result] if result else [],
                        'returned_count': 1 if result else 0, 'matched_count': 1 if result else 0,
                        'has_more': False}

        result = build_handoff(self.conn, Adapter(), 'batch', message())
        self.assertEqual([row['handoff_state'] for row in result['items']],
                         ['resolved_ignore', 'resolved_task', 'resolved_duplicate_family'])


class DispatchTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.conn = connect(str(Path(self.tmp.name) / 'dispatch.db'))

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def test_verified_source_and_uncertain_delivery_reconciliation(self):
        class Gateway:
            def review_detail(self, action_id, day):
                return {'ok': True, 'complete': True, 'found': True,
                        'action': {'action_id': action_id, 'status': 'REVIEW'},
                        'source': {'text': '请确认事项', 'text_truncated': False,
                                   'group_name': '测试群', 'message_time': '2026-10-29T10:00:00'}}
        batch, rendered = prepare_from_reviews(self.conn, Gateway(), 'conversation', 'owner',
                                               [('2026-10-29', '762')])
        self.assertIn('复核编号：' + batch, rendered)
        self.assertEqual(claim_dispatch(self.conn, batch), rendered)
        mark_dispatch_unknown(self.conn, batch)
        with self.assertRaisesRegex(ValueError, 'dispatch_not_prepared'):
            claim_dispatch(self.conn, batch)
        self.assertEqual(reconcile_dispatch(self.conn, batch, 'bot', payload([])), 'delivery_not_proven')
        bot_message = {'conversationId': 'conversation', 'senderId': 'bot', 'text': rendered,
                       'messageId': 'prompt', 'createTime': '2026-10-31 17:26:22'}
        incomplete_prompt = {**bot_message, 'text': '复核编号：' + batch}
        self.assertEqual(reconcile_dispatch(self.conn, batch, 'bot', payload([incomplete_prompt])),
                         'delivery_not_proven')
        with self.assertRaisesRegex(ValueError, 'incomplete_message_read'):
            reconcile_dispatch(self.conn, batch, 'bot', {**payload([bot_message]), 'complete': False})
        self.assertEqual(reconcile_dispatch(self.conn, batch, 'bot', payload([bot_message])), 'delivered')
        self.assertEqual(self.conn.execute('SELECT state FROM batch WHERE id=?', (batch,)).fetchone()[0], 'open')
        self.assertEqual(self.conn.execute('SELECT action_ids FROM item WHERE batch_id=?', (batch,)).fetchone()[0], '["762"]')

    def test_incomplete_review_source_rejected(self):
        class Gateway:
            def review_detail(self, action_id, day):
                return {'ok': False, 'complete': False, 'found': True}
        with self.assertRaisesRegex(ValueError, 'review_source_not_verified'):
            prepare_from_reviews(self.conn, Gateway(), 'conversation', 'owner', [('2026-10-29', '762')])
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM dispatch').fetchone()[0], 0)

    def test_send_timeout_never_retries(self):
        class Gateway:
            def review_detail(self, action_id, day):
                return {'ok': True, 'complete': True, 'found': True,
                        'action': {'action_id': action_id, 'status': 'REVIEW'},
                        'source': {'text': '事项', 'text_truncated': False}}
        batch, text = prepare_from_reviews(self.conn, Gateway(), 'conversation', 'owner',
                                           [('2026-10-29', '762')])
        args = SimpleNamespace(batch=batch, mode='preview', confirm_batch=None, profile='p')
        def context(conn, args):
            state, preview = conn.execute('SELECT status,preview_hash FROM dispatch WHERE id=?',
                                          (args.batch,)).fetchone()
            return {'text': text, 'status': state, 'preview_hash': preview, 'digest': 'digest',
                    'robot_code': 'robot', 'owner_user': 'user', 'items': []}
        def transport(arguments):
            if '--dry-run' in arguments:
                return {'dry_run': True, 'executed': False, 'actions': [{'identity': 'bot',
                         'arguments': {'robotCode': 'robot', 'userIds': ['user'], 'markdown': text}}]}
            raise RuntimeError('timeout')
        with patch.object(review_dispatch_send, 'verify_context', side_effect=context), \
             patch.object(review_dispatch_send, 'verify_sources'), \
             patch.object(review_dispatch_send, 'dws', side_effect=transport):
            self.assertEqual(review_dispatch_send.run(self.conn, args)['status'], 'preview')
            args.mode, args.confirm_batch = 'send', batch
            with self.assertRaisesRegex(RuntimeError, 'timeout'):
                review_dispatch_send.run(self.conn, args)
            self.assertEqual(self.conn.execute('SELECT status FROM dispatch').fetchone()[0], 'unknown')
            with self.assertRaisesRegex(ValueError, 'dispatch_not_prepared'):
                review_dispatch_send.run(self.conn, args)


class IdentityTests(unittest.TestCase):
    def test_owner_must_match_both_user_id_and_open_id(self):
        profile = {'profiles': [{'isOrgCurrent': True, 'profile': 'p', 'userId': 'u', 'userName': 'n'}]}
        contact = {'success': True, 'result': [{'userId': 'u', 'openDingTalkId': 'owner'}]}
        with patch.object(review_identity, 'read_dws', side_effect=[profile, contact]):
            self.assertEqual(review_identity.validate_owner('p', 'owner'), 'u')
        with patch.object(review_identity, 'read_dws', side_effect=[profile, contact]):
            with self.assertRaisesRegex(ValueError, 'owner_identity_mismatch'):
                review_identity.validate_owner('p', 'someone-else')


class IgnoreHandoffTests(unittest.TestCase):
    def test_preview_commit_and_repeat_use_single_writer(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = connect(str(Path(tmp) / 'db'))
            register_batch(conn, 'batch', 'conversation', 'owner', 'prompt',
                           '2026-10-31 17:26:22', {1: ['751']})
            item = {'ordinal': 1, 'user_decision': 'ignore', 'handoff_state': 'ready_for_existing_worker_review',
                    'sources': [{'action_id': '751', 'conversation_id': 'c', 'message_id': 'm',
                                 'updated_at': '2026-10-29T01:01:38', 'business_key': 'key'}]}
            handoff = {'items': [item], 'reply_message_id': 'reply'}
            class Writer:
                def execute(self, req):
                    return {'ok': True, 'committed': not req.get('dry_run', False), 'row': {'status': 'IGNORED'}}
            class Reader:
                def query(self, req):
                    return {'has_more': False, 'rows': [{'id': 751, 'status': 'IGNORED',
                            'external_task_id': None, 'duplicate_of_action_id': None}]}
            with patch.object(review_resolve_ignore, 'verified_handoff', return_value=handoff), \
                 patch.object(review_resolve_ignore, 'WorkWriter', Writer), \
                 patch.object(review_resolve_ignore, '_load_work_query_adapter', return_value=Reader()):
                args = (conn, 'batch', 1, 'profile', 'bot', str(Path(tmp)/'work.lock'))
                self.assertEqual(review_resolve_ignore.resolve(*args)['status'], 'preview')
                self.assertEqual(review_resolve_ignore.resolve(*args, apply=True)['status'], 'applied')
                self.assertEqual(review_resolve_ignore.resolve(*args, apply=True)['status'], 'already_recorded_needs_reconciliation')
            conn.close()


if __name__ == '__main__':
    unittest.main()
