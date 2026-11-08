import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import review_resolve_create
from dws_adapter import _epoch_ms
from review_inbox import connect, ingest_reply, register_batch


class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.conn = connect(str(Path(self.tmp.name) / 'review.db'))
        register_batch(self.conn, 'batch', 'conversation', 'owner', 'prompt',
                       '2026-10-31 17:26:22', {1: ['751'], 2: ['762'], 3: ['742', '743', '744']})
        ingest_reply(self.conn, 'batch', {
            'messageId': 'reply', 'conversationId': 'conversation', 'senderId': 'owner',
            'createTime': '2026-10-31 17:27:15',
            'text': '1，忽略，2，这条创建待办，3，创建，执行人是我',
        })
        audit = {'claim': {'action_id': 762}, 'title': '已确认整改事项',
                 'due_iso': '2026-10-31T17:00:00+08:00',
                 'executor_user_id': 'user-id', 'source_message_id': 'source-2',
                 'reply_message_id': 'reply'}
        self.conn.execute(
            "INSERT INTO resolution (batch_id,ordinal,action_id,decision,reply_message_id,request_json,status,external_task_id) "
            "VALUES (?,?,?,?,?,?,?,?)",
            ('batch', 2, '762', 'create', 'reply', json.dumps(audit), 'applying', 'task-123'),
        )
        self.item = {'ordinal': 2, 'user_decision': 'create', 'handoff_state': 'action_state_requires_reconciliation',
                     'sources': [{'action_id': '762', 'status': 'EXTERNAL_CREATED',
                                  'conversation_id': 'conversation', 'message_id': 'source-2',
                                  'external_task_id': 'task-123', 'source': {
                                      'group_name': '整改群', 'sender_name': '班组长',
                                      'message_time': '2026-10-29 16:31:57',
                                      'text': '请于10月31日17:00前更新整改内容并附照片'}}]}
        self.handoff = {'items': [self.item], 'reply_message_id': 'reply'}

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def _preview_due(self, preview_due, due_iso='2026-10-31T17:00:00+08:00'):
        self.conn.execute('DELETE FROM resolution')
        source_item = {'ordinal': 2, 'user_decision': 'create',
                       'handoff_state': 'ready_for_existing_worker_review',
                       'sources': [{'action_id': '762', 'status': 'REVIEW',
                                    'conversation_id': 'conversation', 'message_id': 'source-2',
                                    'updated_at': '2026-10-29T16:31:57', 'business_key': 'business-key',
                                    'source': {'group_name': '整改群', 'sender_name': '班组长',
                                               'message_time': '2026-10-29 16:31:57',
                                               'text': '请于10月31日17:00前更新整改事项并附照片'}}]}
        handoff = {'items': [source_item], 'reply_message_id': 'reply'}

        class Writer:
            def execute(self, request):
                return {'ok': True, 'committed': False, 'row': {'status': 'CLAIMED'}}

        class Dws:
            def __init__(self, **_kwargs):
                self.create_args = None
            def search_todos(self, _query):
                return []
            def run(self, arguments):
                self.create_args = arguments
                return {'dry_run': True, 'executed': False, 'arguments': {
                    'PersonalTodoCreateVO': {'subject': '整改事项', 'executorIds': ['user-id'],
                                              'priority': 20, 'dueTime': preview_due}}}

        dws = Dws()
        with patch.object(review_resolve_create, 'verified_handoff', return_value=handoff), \
             patch.object(review_resolve_create, 'owner_identity', return_value=('user-id', '李林骁', {})), \
             patch.object(review_resolve_create, 'WorkWriter', return_value=Writer()), \
             patch.object(review_resolve_create, 'DwsClient', return_value=dws), \
             patch.dict('os.environ', {'DINGTODO_ASSIGN_SELF_ONLY': '1'}):
            result = review_resolve_create.resolve(
                self.conn, 'batch', 2, 'profile', 'bot', str(Path(self.tmp.name) / 'lock'),
                '整改事项', '整改事项', apply=False, due_iso=due_iso)
        return result, dws.create_args

    def test_single_item_natural_reply_is_self_assigned_only_when_explicit(self):
        self.assertTrue(review_resolve_create.self_assigned('创建待办提醒我，标注测试用', 1))
        self.assertFalse(review_resolve_create.self_assigned('创建待办，标注测试用', 1))
        self.assertFalse(review_resolve_create.self_assigned('创建待办提醒我，标注测试用', 2))

    def test_reconcile_existing_task_without_creating_again(self):
        class Dws:
            def __init__(self, **kwargs):
                self.create_calls = 0
            def get_todo(self, task_id):
                return {'taskId': task_id, 'subject': '已确认整改事项', 'isDone': False,
                        'executorInfos': [{'name': '李林骁', 'userId': 'numeric-user-id'}],
                        'priority': 20, 'dueTime': _epoch_ms('2026-10-31T17:00:00+08:00')}
            def verify_source_comment(self, task_id, comment):
                self.comment = comment
            def create_todo(self, *args, **kwargs):
                self.create_calls += 1
                raise AssertionError('reconciliation must never create another task')
        class Writer:
            calls = []
            def execute(self, request):
                self.calls.append(request)
                return {'ok': True, 'committed': True}
        class Reader:
            calls = 0
            def query(self, request):
                self.calls += 1
                status = 'EXTERNAL_CREATED' if self.calls == 1 else 'SUCCESS'
                return {'has_more': False, 'rows': [{'id': 762, 'status': status,
                        'external_task_id': 'task-123'}]}

        dws = Dws()
        writer = Writer()
        reader = Reader()
        profiles = {'profiles': [{'profile': 'profile', 'isOrgCurrent': True,
                                  'userId': 'user-id', 'userName': '李林骁'}]}
        contacts = {'success': True, 'result': [{'name': '李林骁', 'userId': 'user-id',
                                                 'openDingTalkId': 'owner'}]}
        with patch.object(review_resolve_create, 'verified_handoff', return_value=self.handoff), \
             patch.object(review_resolve_create, 'validate_owner', return_value='user-id'), \
             patch.object(review_resolve_create, 'read_dws', side_effect=[profiles, contacts]), \
             patch.object(review_resolve_create, 'DwsClient', return_value=dws), \
             patch.object(review_resolve_create, 'WorkWriter', return_value=writer), \
             patch.object(review_resolve_create, '_load_work_query_adapter', return_value=reader), \
             patch.dict('os.environ', {'DINGTODO_ASSIGN_SELF_ONLY': '1'}):
            result = review_resolve_create.reconcile_existing(
                self.conn, 'batch', 2, 'profile', 'bot', 'task-123',
                self.conn.execute('SELECT request_json FROM resolution').fetchone()[0])
        self.assertEqual(result['status'], 'applied')
        self.assertEqual(len(writer.calls), 1)
        self.assertEqual(writer.calls[0]['status'], 'SUCCESS')
        stored = self.conn.execute('SELECT status,request_json FROM resolution').fetchone()
        self.assertEqual(stored[0], 'applied')
        self.assertTrue(json.loads(stored[1])['verification']['executor_id_namespace_normalized'])

    def test_reconcile_refuses_due_change_before_remote_read(self):
        profiles = {'profiles': [{'profile': 'profile', 'isOrgCurrent': True,
                                  'userId': 'user-id', 'userName': '李林骁'}]}
        contacts = {'success': True, 'result': [{'name': '李林骁', 'userId': 'user-id',
                                                 'openDingTalkId': 'owner'}]}
        with patch.object(review_resolve_create, 'verified_handoff', return_value=self.handoff), \
             patch.object(review_resolve_create, 'validate_owner', return_value='user-id'), \
             patch.object(review_resolve_create, 'read_dws', side_effect=[profiles, contacts]), \
             patch.object(review_resolve_create, 'DwsClient') as dws, \
             patch.dict('os.environ', {'DINGTODO_ASSIGN_SELF_ONLY': '1'}):
            with self.assertRaisesRegex(ValueError, 'reconciliation_due_binding_mismatch'):
                review_resolve_create.reconcile_existing(
                    self.conn, 'batch', 2, 'profile', 'bot', 'task-123',
                    self.conn.execute('SELECT request_json FROM resolution').fetchone()[0],
                    expected_due_iso='2026-11-02T17:00:00+08:00')
            dws.assert_not_called()

    def test_due_iso_requires_valid_timezone_aware_rfc3339(self):
        self.assertEqual(review_resolve_create._validated_due_iso(
            '2026-11-02T17:00:00+08:00'), '2026-11-02T17:00:00+08:00')
        for invalid in ('2026-11-02', '2026-11-02T17:00:00', '2026-02-30T17:00:00+08:00', ''):
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(ValueError, 'due_iso_invalid'):
                    review_resolve_create._validated_due_iso(invalid)

    def test_source_due_is_recomputed_from_frozen_text_and_message_time(self):
        self.assertEqual(review_resolve_create._source_due_iso(self.item['sources'][0]),
                         '2026-10-31T17:00:00+08:00')
        changed = {**self.item['sources'][0],
                   'source': {**self.item['sources'][0]['source'],
                              'text': '请于11月1日17:00前更新整改内容并附照片'}}
        self.assertEqual(review_resolve_create._source_due_iso(changed),
                         '2026-11-01T17:00:00+08:00')

        for text in ('请周五前更新整改事项', '请于10月31日下班前更新整改事项',
                     '请于10月31日17:00前更新整改事项，11月1日18:00前复核'):
            source = {**self.item['sources'][0],
                      'source': {**self.item['sources'][0]['source'], 'text': text}}
            with self.subTest(text=text), self.assertRaises(ValueError):
                review_resolve_create._source_due_iso(source)

    def test_due_date_is_in_create_preview_and_must_match(self):
        due_iso = '2026-10-31T17:00:00+08:00'
        result, args = self._preview_due(_epoch_ms(due_iso))
        self.assertEqual(result['status'], 'preview')
        self.assertIn('--due', args)
        self.assertIn(due_iso, args)

        with self.assertRaisesRegex(ValueError, 'todo_preview_mismatch'):
            self._preview_due(_epoch_ms('2026-11-01T17:00:00+08:00'))

    def test_due_date_supplied_by_caller_must_match_source_text(self):
        with self.assertRaisesRegex(ValueError, 'due_iso_source_mismatch'):
            self._preview_due(_epoch_ms('2026-10-31T17:00:00+08:00'),
                              due_iso='2026-11-01T17:00:00+08:00')

    def test_non_unique_owner_contact_blocks_reconciliation(self):
        profiles = {'profiles': [{'profile': 'profile', 'isOrgCurrent': True,
                                  'userId': 'user-id', 'userName': '李林骁'}]}
        contacts = {'success': True, 'result': [
            {'name': '李林骁', 'userId': 'user-id', 'openDingTalkId': 'owner'},
            {'name': '李林骁', 'userId': 'other-user', 'openDingTalkId': 'other-owner'},
        ]}
        with patch.object(review_resolve_create, 'verified_handoff', return_value=self.handoff), \
             patch.object(review_resolve_create, 'validate_owner', return_value='user-id'), \
             patch.object(review_resolve_create, 'read_dws', side_effect=[profiles, contacts]), \
             patch.object(review_resolve_create, 'DwsClient') as dws:
            with self.assertRaisesRegex(ValueError, 'owner_contact_not_unique'):
                review_resolve_create.reconcile_existing(
                    self.conn, 'batch', 2, 'profile', 'bot', 'task-123',
                    self.conn.execute('SELECT request_json FROM resolution').fetchone()[0])
            dws.assert_not_called()


if __name__ == '__main__':
    unittest.main()
