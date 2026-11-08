from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import review_scheduler_runner as runner


def test_runner_connects_verified_conversation_and_callbacks():
    info = {'success': True, 'result': {'conversationInfo': {
        'singleChat': True, 'openConversationId': 'dm'}}}
    now = datetime(2026, 11, 5, tzinfo=timezone.utc)
    with patch.dict(runner.os.environ, {}, clear=True), \
         patch.object(runner, 'validate_owner') as owner, \
         patch.object(runner, 'read_dws', return_value=info), \
         patch.object(runner, 'enable') as enable, \
         patch.object(runner, '_load_work_query_adapter', return_value=object()), \
         patch.object(runner, 'run_tick', return_value={'status': 'idle'}) as tick:
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (1,)
        result = runner.run(conn, profile='p', bot_open_id='bot', owner_open_id='owner',
                            app_id='app', lock_path='lock', now=now)
        assert result == {'status': 'idle'}
        owner.assert_called_once_with('p', 'owner')
        enable.assert_not_called()
        assert tick.call_args.kwargs['conversation_id'] == 'dm'
        assert all(callable(tick.call_args.kwargs[key]) for key in
                   ('preflight', 'sender', 'history_reader', 'advance_batch'))
        assert tick.call_args.kwargs['card_sender'] is None


def test_runner_passes_card_sender_only_after_explicit_feature_enable():
    info = {'success': True, 'result': {'conversationInfo': {
        'singleChat': True, 'openConversationId': 'dm'}}}
    now = datetime(2026, 11, 5, tzinfo=timezone.utc)
    environment = {
        'DWS_CARD_REVIEW_ENABLED': 'true',
        'DINGTODO_REVIEW_CARD_TEMPLATE_ID': 'template',
        'DINGTODO_REVIEW_CARD_TEMPLATE_VERSION': 'v1',
        'DINGTODO_REVIEW_CARD_FIELD_MAP': '{"summary":"review_text"}',
        'DINGTODO_REVIEW_CARD_IDENTITY_MAP_FILE': '/private/map.json',
    }
    with patch.dict(runner.os.environ, environment, clear=True), \
         patch.object(runner, 'validate_owner'), \
         patch.object(runner, 'read_dws', return_value=info), \
         patch.object(runner, '_load_work_query_adapter', return_value=object()), \
         patch.object(runner, 'run_tick', return_value={'status': 'idle'}) as tick:
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (1,)
        runner.run(conn, profile='p', bot_open_id='bot', owner_open_id='owner',
                   app_id='app', lock_path='lock', now=now)

    assert callable(tick.call_args.kwargs['card_sender'])


def test_disabled_scheduler_does_not_require_network_or_gateway():
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = None
    with patch.object(runner, 'read_dws') as read, patch.object(runner, '_load_work_query_adapter') as load:
        assert runner.run(conn, profile='p', bot_open_id='bot', owner_open_id='owner',
                          app_id='app', lock_path='lock') == {'status': 'scheduler_not_enabled'}
        read.assert_not_called()
        load.assert_not_called()


def test_runner_refuses_unverified_dm_before_activation():
    with patch.object(runner, 'validate_owner'), \
         patch.object(runner, 'read_dws', return_value={'success': True, 'result': {
             'conversationInfo': {'singleChat': False, 'openConversationId': 'dm'}}}), \
         patch.object(runner, 'enable') as enable:
        with pytest.raises(ValueError, match='conversation_identity_mismatch'):
            runner.run(object(), profile='p', bot_open_id='bot', owner_open_id='owner',
                       app_id='app', lock_path='lock', activate=True,
                       now=datetime(2026, 11, 5, tzinfo=timezone.utc))
        enable.assert_not_called()
