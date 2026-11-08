import json

import pytest

from review_gateway import _safe_source
from review_handoff import build_handoff
from review_inbox import connect, ingest_reply, register_batch


def test_review_reply_cannot_apply_to_changed_source(tmp_path):
    conn = connect(str(tmp_path / 'inbox.sqlite3'))
    register_batch(conn, 'review_batch', 'dm', 'owner', 'prompt',
                   '2026-11-05 10:00:00', {1: ['71']})
    reply = {'messageId': 'reply', 'conversationId': 'dm', 'senderId': 'owner',
             'createTime': '2026-11-05 10:01:00', 'text': '创建待办'}
    ingest_reply(conn, 'review_batch', reply)
    original = {'conversation_id': 'group', 'message_id': 'message', 'text': '请检查设备',
                'group_name': '测试群', 'sender_name': '测试人', 'message_time': '2026-11-05 09:00:00'}
    frozen, _ = _safe_source(original)
    conn.execute('INSERT INTO dispatch (id,conversation_id,owner_id,text,items,status) VALUES (?,?,?,?,?,?)',
                 ('review_batch', 'dm', 'owner', 'prompt', json.dumps([
                     {'action_id': '71', 'source_date': '2026-11-05', 'source': frozen}]), 'delivered'))

    class Adapter:
        def __init__(self):
            self.source = original

        def query(self, request):
            if request['view'] == 'actions':
                rows = [{'id': 71, 'status': 'REVIEW', 'conversation_id': 'group', 'message_id': 'message'}]
            else:
                rows = [self.source]
            return {'view': request['view'], 'rows': rows, 'has_more': False,
                    'returned_count': len(rows), 'matched_count': len(rows), 'next_page_token': None}

    adapter = Adapter()
    assert build_handoff(conn, adapter, 'review_batch', reply)['items'][0]['handoff_state'] == 'ready_for_existing_worker_review'
    adapter.source = {**original, 'text': '请拆除设备'}
    with pytest.raises(ValueError, match='dispatch_source_changed'):
        build_handoff(conn, adapter, 'review_batch', reply)
    conn.close()
