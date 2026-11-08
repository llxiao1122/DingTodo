from unittest.mock import patch

import pytest

from review_schedule_dws import ReviewScheduleDws


def context(status='prepared', digest='digest'):
    return {'conversation': 'conversation', 'owner_user': 'owner-user',
            'robot_code': 'robot', 'text': 'source', 'status': status,
            'digest': digest, 'preview_hash': digest, 'sending_at': 'time'}


def adapter():
    return ReviewScheduleDws(object(), profile='profile', bot_open_id='bot',
                             owner_open_id='owner', app_id='app')


def test_preflight_checks_binding_and_actual_preview():
    instance = adapter()
    with patch('review_schedule_dws.review_dispatch_send.verify_context', return_value=context()), \
         patch('review_schedule_dws.review_auto_process._verified_send_context',
               return_value=('owner-user', 'robot')), \
         patch('review_schedule_dws.review_dispatch_send.run',
               return_value={'status': 'preview', 'batch': 'batch'}) as preview:
        assert instance.preflight('batch') == {'digest': 'digest'}
        preview.assert_called_once()


def test_send_checks_claim_plan_source_and_bot_before_write():
    instance = adapter()
    with patch('review_schedule_dws.review_dispatch_send.verify_context', return_value=context('sending')), \
         patch('review_schedule_dws.review_auto_process._verified_send_context',
               return_value=('owner-user', 'robot')), \
         patch('review_schedule_dws.review_dispatch_send.verify_sources') as source, \
         patch('review_schedule_dws.review_dispatch_send.dws',
               return_value={'ok': True, 'result': {'success': True}}) as send:
        instance.send('batch', {'digest': 'digest'}, 'source')
        source.assert_called_once()
        assert send.call_args.args[0][-1] == '--yes'


def test_changed_plan_or_wrong_bot_never_sends():
    instance = adapter()
    with patch('review_schedule_dws.review_dispatch_send.verify_context', return_value=context('sending')), \
         patch('review_schedule_dws.review_auto_process._verified_send_context',
               return_value=('owner-user', 'different-robot')), \
         patch('review_schedule_dws.review_dispatch_send.dws') as send:
        with pytest.raises(ValueError, match='plan_changed'):
            instance.send('batch', {'digest': 'old'}, 'source')
        with pytest.raises(ValueError, match='identity_mismatch'):
            instance.send('batch', {'digest': 'digest'}, 'source')
        send.assert_not_called()


def test_history_requires_same_claim_and_identity():
    instance = adapter()
    with patch('review_schedule_dws.review_dispatch_send.verify_context', return_value=context('unknown')), \
         patch('review_schedule_dws.review_auto_process._verified_send_context',
               return_value=('owner-user', 'robot')), \
         patch('review_schedule_dws.read_dws', return_value={'complete': True}) as read:
        with pytest.raises(ValueError, match='context_changed'):
            instance.history('batch', 'wrong-time')
        read.assert_not_called()
        assert instance.history('batch', 'time') == {'complete': True}
