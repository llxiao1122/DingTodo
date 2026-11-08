from unittest.mock import Mock, patch

import pytest

from manual_backfill import backfill


def test_manual_backfill_checks_source_and_stops_before_writing():
    writer, dws, check = Mock(), Mock(), Mock()
    action = {'id': 1, 'status': 'REVIEW', 'conversation_id': 'chat', 'message_id': 'one'}
    source = {'conversation_id': 'chat', 'message_id': 'other', 'text': 'source'}
    with pytest.raises(ValueError, match='manual_source_mismatch'):
        backfill(action=action, source=source, plan={}, writer=writer, dws=dws, check_duplicates=check, record_remote_task=lambda *_: None)
    writer.execute.assert_not_called()
    dws.create_todo.assert_not_called()


def test_manual_backfill_claim_failure_never_creates():
    writer, dws = Mock(), Mock()
    writer.execute.return_value = {'ok': False}
    action = {'id': 1, 'status': 'REVIEW', 'conversation_id': 'chat', 'message_id': 'one',
              'updated_at': '2026-11-04T10:00:00'}
    source = {'conversation_id': 'chat', 'message_id': 'one', 'text': 'source'}
    plan = {'decision': 'CREATE', 'title': 'task', 'executor_ids': ['u'], 'executor_names': ['name']}
    with pytest.raises(RuntimeError, match='manual_claim_unconfirmed'):
        backfill(action=action, source=source, plan=plan, writer=writer, dws=dws, check_duplicates=lambda: None, record_remote_task=lambda *_: None)
    dws.create_todo.assert_not_called()


def test_manual_backfill_links_before_comment_and_verifies_source():
    events = []
    writer, dws = Mock(), Mock()
    def execute(request):
        events.append(request['operation'])
        row = {'status': 'CLAIMED' if request['operation'] == 'claim_review_create' else request.get('status'),
               'external_task_id': request.get('external_task_id')}
        return {'ok': True, 'committed': True, 'action_id': 1, 'row': row}
    writer.execute.side_effect = execute
    dws.create_todo.return_value = {'task_id': 'task-1'}
    dws.add_source_comment.side_effect = lambda *_: events.append('comment')
    dws.verify_source_comment.side_effect = lambda *_: events.append('comment_verified')
    dws.get_todo.return_value = {'taskId': 'task-1'}
    action = {'id': 1, 'status': 'REVIEW', 'conversation_id': 'chat', 'message_id': 'one',
              'updated_at': '2026-11-04T10:00:00'}
    source = {'conversation_id': 'chat', 'message_id': 'one', 'text': 'original'}
    plan = {'decision': 'CREATE', 'title': 'task', 'executor_ids': ['u'], 'executor_names': ['name']}
    with patch('manual_backfill.verify_todo', return_value=[]):
        assert backfill(action=action, source=source, plan=plan, writer=writer, dws=dws,
                        check_duplicates=lambda: None, record_remote_task=lambda *_: None) == 'task-1'
    assert events.index('link_external_task') < events.index('comment') < events.index('comment_verified')
    assert 'original' in dws.add_source_comment.call_args.args[1]


def test_unrecorded_remote_id_stops_before_link_and_comment():
    from todo_execution import TodoExecutionError
    writer, dws = Mock(), Mock()
    def execute(request):
        status = 'CLAIMED' if request['operation'] == 'claim_review_create' else request.get('status')
        return {'ok': True, 'committed': True, 'action_id': 1, 'row': {'status': status}}
    writer.execute.side_effect = execute
    dws.create_todo.return_value = {'task_id': 'task-1'}
    action = {'id': 1, 'status': 'REVIEW', 'conversation_id': 'chat', 'message_id': 'one',
              'updated_at': '2026-11-04T10:00:00'}
    source = {'conversation_id': 'chat', 'message_id': 'one', 'text': 'original'}
    plan = {'decision': 'CREATE', 'title': 'task', 'executor_ids': ['u'], 'executor_names': ['name']}
    def failed_receipt(*_):
        raise RuntimeError('journal unavailable')
    with pytest.raises(TodoExecutionError) as error:
        backfill(action=action, source=source, plan=plan, writer=writer, dws=dws,
                 check_duplicates=lambda: None, record_remote_task=failed_receipt)
    assert error.value.task_id == 'task-1'
    assert all(call.args[0]['operation'] != 'link_external_task' for call in writer.execute.call_args_list)
    dws.add_source_comment.assert_not_called()


def test_link_failure_leaves_durable_receipt_for_reconciliation(tmp_path):
    from manual_receipts import ManualReceiptJournal
    from todo_execution import TodoExecutionError
    journal_path = tmp_path / 'receipts.sqlite3'
    journal = ManualReceiptJournal(str(journal_path))
    writer, dws = Mock(), Mock()
    def execute(request):
        if request['operation'] == 'link_external_task':
            raise RuntimeError('ledger unavailable')
        status = 'CLAIMED' if request['operation'] == 'claim_review_create' else request.get('status')
        return {'ok': True, 'committed': True, 'action_id': 1, 'row': {'status': status}}
    writer.execute.side_effect = execute
    dws.create_todo.return_value = {'task_id': 'task-1'}
    action = {'id': 1, 'status': 'REVIEW', 'conversation_id': 'chat', 'message_id': 'one',
              'updated_at': '2026-11-04T10:00:00'}
    source = {'conversation_id': 'chat', 'message_id': 'one', 'text': 'original'}
    plan = {'decision': 'CREATE', 'title': 'task', 'executor_ids': ['u'], 'executor_names': ['name']}
    with pytest.raises(TodoExecutionError):
        backfill(action=action, source=source, plan=plan, writer=writer, dws=dws,
                 check_duplicates=lambda: None, record_remote_task=journal.record)
    journal.close()
    reopened = ManualReceiptJournal(str(journal_path))
    try:
        assert reopened.lookup(1) == 'task-1'
    finally:
        reopened.close()
    dws.add_source_comment.assert_not_called()


def test_authenticated_apply_refuses_changed_preview(tmp_path):
    from manual_backfill import execute_prepared
    journal, writer, dws = Mock(), Mock(), Mock()
    snapshot = ({'id': 1}, {'message_id': 'one'}, {}, 'new-digest')
    with patch('review_resolve_create.owner_identity', return_value=('user', 'name', {})), \
         patch('manual_backfill.prepare', return_value=snapshot):
        with pytest.raises(ValueError, match='manual_preview_changed'):
            execute_prepared(gateway=Mock(), adapter=Mock(), day=None, action_id='1', title='t',
                             query='t', profile='profile', owner_open_id='owner', writer=writer,
                             dws=dws, journal=journal, lock_path=str(tmp_path / 'lock'),
                             expected_digest='old-digest')
    writer.execute.assert_not_called()
    dws.create_todo.assert_not_called()


def test_command_apply_requires_preview_digest_before_network(capsys):
    from manual_backfill import main
    with patch('review_resolve_create.owner_identity') as identity:
        code = main(['--day', '2026-11-04', '--action-id', '1', '--title', 'task',
                     '--query', 'task', '--profile', 'profile', '--owner-open-id', 'owner', '--apply'])
    assert code == 2
    assert 'manual_preview_digest_required' in capsys.readouterr().err
    identity.assert_not_called()


def test_prepare_rejects_remote_preview_with_wrong_executor():
    from manual_backfill import prepare
    gateway, adapter, dws = Mock(), Mock(), Mock()
    gateway.review_detail.return_value = {
        'ok': True, 'complete': True,
        'action': {'conversation_id': 'chat', 'message_id': 'one'},
        'source': {'text': '请领取器具', 'group_name': 'group', 'sender_name': 'sender',
                   'message_time': '2026-11-04 10:00:00', 'text_truncated': False},
    }
    adapter.query.return_value = {'has_more': False, 'rows': [
        {'id': 1, 'status': 'REVIEW', 'conversation_id': 'chat', 'message_id': 'one'}]}
    dws.run.return_value = {'dry_run': True, 'executed': False,
                            'arguments': {'PersonalTodoCreateVO': {
                                'subject': '请领取器具', 'executorIds': ['wrong'], 'priority': 20}}}
    with patch('review_resolve_create.check_candidates'):
        with pytest.raises(ValueError, match='manual_todo_preview_mismatch'):
            prepare(gateway=gateway, adapter=adapter, day=None, action_id='1', title='请领取器具',
                    query='领取器具', identity=('owner', 'name'), dws=dws)
    dws.create_todo.assert_not_called()
