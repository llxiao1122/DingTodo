"""Build a read-only, evidence-bound handoff from a Cifra review reply."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import unicodedata

from review_gateway import _load_work_query_adapter, _safe_source
from review_identity import validate_owner
from review_inbox import connect, parse_reply
from review_inbox_poll import read_dws


def read_all_actions(adapter):
    rows, seen = [], set()
    token = None
    for _ in range(100):
        request = {'view': 'actions', 'limit': 100}
        if token:
            request['page_token'] = token
        page = adapter.query(request)
        if (not isinstance(page, dict) or page.get('view') != 'actions'
                or not isinstance(page.get('rows'), list) or not isinstance(page.get('has_more'), bool)
                or page.get('returned_count') != len(page['rows'])):
            raise ValueError('incomplete_action_read')
        rows.extend(page['rows'])
        if not page['has_more']:
            if page.get('next_page_token') is not None:
                raise ValueError('unexpected_action_cursor')
            return rows
        token = page.get('next_page_token')
        if not isinstance(token, str) or not token or token in seen:
            raise ValueError('invalid_action_cursor')
        seen.add(token)
    raise ValueError('action_read_limit')


def source_for(adapter, row):
    response = adapter.query({'view': 'message', 'conversation_id': row['conversation_id'],
                              'message_id': row['message_id'], 'limit': 2})
    rows = response.get('rows') if isinstance(response, dict) else None
    if (not isinstance(response, dict) or response.get('view') != 'message' or not isinstance(rows, list) or len(rows) != 1
            or response.get('has_more') is not False or response.get('matched_count') != 1
            or response.get('returned_count') != 1):
        raise ValueError('source_missing_or_ambiguous')
    source = rows[0]
    if (source.get('conversation_id') != row['conversation_id']
            or source.get('message_id') != row['message_id'] or not isinstance(source.get('text'), str)):
        raise ValueError('source_identity_mismatch')
    return source


def _normalized_notice(text):
    value = unicodedata.normalize('NFKC', str(text or '')).strip()
    value = re.sub(r'^\[DING\]\s*', '', value)
    value = re.sub(r'@(?:所有人|工班长)', '', value)
    return re.sub(r'\s+', '', value)


def build_handoff(conn, adapter, batch_id, live_reply=None, *, card_evidence=None):
    batch = conn.execute('SELECT conversation_id,owner_id,prompt_time,state FROM batch WHERE id=?',
                         (batch_id,)).fetchone()
    reply = conn.execute('SELECT message_id,message_time,content_sha256,content,decisions,source FROM reply WHERE batch_id=?',
                         (batch_id,)).fetchall()
    if not batch or batch[3] != 'answered' or len(reply) != 1:
        raise ValueError('batch_or_reply_not_ready')
    conversation, owner, prompt_time, _ = batch
    message_id, message_time, digest, content, saved, reply_source = reply[0]
    if (not isinstance(content, str)
            or hashlib.sha256(content.encode('utf-8')).hexdigest() != digest
            or message_time <= prompt_time):
        raise ValueError('reply_evidence_mismatch')
    if reply_source == 'card':
        from review_card_adapter import verify_card_reply_evidence
        verified_card = verify_card_reply_evidence(conn, batch_id)
        if (card_evidence != verified_card or verified_card.get('conversation_id') != conversation
                or verified_card.get('owner_id') != owner
                or verified_card.get('reply_message_id') != message_id
                or verified_card.get('reply_time') != message_time
                or verified_card.get('reply_text') != content):
            raise ValueError('card_reply_evidence_mismatch')
    elif (reply_source == 'chat' and card_evidence is None
            and isinstance(live_reply, dict)
            and live_reply.get('conversationId') == conversation
            and live_reply.get('senderId') == owner
            and live_reply.get('messageId') == message_id
            and live_reply.get('createTime') == message_time
            and live_reply.get('text') == content):
        pass
    else:
        raise ValueError('reply_evidence_mismatch')
    items = conn.execute('SELECT ordinal,action_ids FROM item WHERE batch_id=? ORDER BY ordinal',
                         (batch_id,)).fetchall()
    clarification = conn.execute('SELECT 1 FROM review_clarification WHERE batch_id=?', (batch_id,)).fetchone()
    decisions = parse_reply(content, len(items), batch_id=batch_id) if clarification else parse_reply(content, len(items))
    if json.loads(saved) != {str(k): v for k, v in decisions.items()}:
        raise ValueError('reply_decision_mismatch')
    dispatch = conn.execute('SELECT items,status FROM dispatch WHERE id=?', (batch_id,)).fetchone()
    frozen_sources = None
    if dispatch:
        if dispatch[1] != 'delivered':
            raise ValueError('dispatch_delivery_not_verified')
        frozen = json.loads(dispatch[0])
        if not isinstance(frozen, list) or any(not isinstance(item, dict) for item in frozen):
            raise ValueError('dispatch_sources_invalid')
        frozen_sources = {}
        for item in frozen:
            aid, source = item.get('action_id'), item.get('source')
            if not isinstance(aid, str) or aid in frozen_sources or not isinstance(source, dict):
                raise ValueError('dispatch_sources_invalid')
            frozen_sources[aid] = source
        item_ids = [str(aid) for _, encoded in items for aid in json.loads(encoded)]
        if len(item_ids) != len(set(item_ids)) or set(item_ids) != set(frozen_sources):
            raise ValueError('dispatch_item_identity_mismatch')
    actions = read_all_actions(adapter)
    by_id = {}
    for action in actions:
        aid = str(action.get('id') or '')
        if not aid or aid in by_id:
            raise ValueError('duplicate_action_identity')
        by_id[aid] = action
    result = []
    for ordinal, encoded_ids in items:
        ids = json.loads(encoded_ids)
        if not isinstance(ids, list) or not ids:
            raise ValueError('invalid_item_action_ids')
        rows = []
        for aid in ids:
            row = by_id.get(str(aid))
            if not row:
                raise ValueError('action_not_found')
            source = source_for(adapter, row)
            if frozen_sources is not None:
                current_source, issue = _safe_source(source)
                if issue or current_source != frozen_sources[str(aid)]:
                    raise ValueError('dispatch_source_changed')
            rows.append({'action_id': str(aid), 'status': row.get('status'),
                         'conversation_id': row.get('conversation_id'), 'message_id': row.get('message_id'),
                         'updated_at': row.get('updated_at'), 'business_key': row.get('business_key'),
                         'external_task_id': row.get('effective_external_task_id'),
                         'duplicate_of_action_id': row.get('duplicate_of_action_id'),
                         'source': {'group_name': source.get('group_name'),
                                    'sender_name': source.get('sender_name'),
                                    'message_time': source.get('message_time'), 'text': source['text']}})
        duplicates = sorted({str(row['duplicate_of_action_id']) for row in rows if row['duplicate_of_action_id']})
        canonical = []
        for canonical_id in duplicates:
            row = by_id.get(canonical_id)
            if not row:
                raise ValueError('canonical_action_not_found')
            source = source_for(adapter, row)
            canonical.append({'action_id': canonical_id, 'status': row.get('status'),
                              'external_task_id': row.get('effective_external_task_id'),
                              'source': {'group_name': source.get('group_name'),
                                         'message_time': source.get('message_time'), 'text': source['text']}})
        if duplicates:
            canonical_id = duplicates[0] if len(duplicates) == 1 else None
            canonical_row = next((row for row in rows if str(row['action_id']) == canonical_id), None)
            same_notice = len({_normalized_notice(row['source']['text']) for row in rows}) == 1
            if canonical_row and same_notice and all(
                    str(row['duplicate_of_action_id']) == canonical_id
                    for row in rows if str(row['action_id']) != canonical_id):
                if canonical_row['status'] == 'SUCCESS' and canonical_row['external_task_id']:
                    state = 'resolved_duplicate_family'
                elif canonical_row['status'] in ('CLAIMED', 'EXTERNAL_CREATED'):
                    state = 'duplicate_family_in_progress'
                else:
                    state = 'duplicate_link_requires_review'
            else:
                state = 'duplicate_link_requires_review'
        elif len(rows) != 1:
            state = 'multiple_sources_require_review'
        elif rows[0]['status'] == 'IGNORED' and not rows[0]['external_task_id']:
            state = 'resolved_ignore'
        elif rows[0]['status'] == 'SUCCESS' and rows[0]['external_task_id']:
            state = 'resolved_task'
        elif rows[0]['status'] != 'REVIEW' or rows[0]['external_task_id']:
            state = 'action_state_requires_reconciliation'
        else:
            state = 'ready_for_existing_worker_review'
        result.append({'ordinal': ordinal, 'user_decision': decisions[ordinal],
                       'handoff_state': state, 'sources': rows, 'canonical_sources': canonical})
    return {'batch': batch_id, 'reply_message_id': message_id,
            'reply_source': reply_source, 'items': result,
            'writes_performed': 0}


def verified_handoff(conn, batch_id, profile, bot_open_id):
        batch = conn.execute('SELECT conversation_id,owner_id,prompt_time FROM batch WHERE id=?',
                             (batch_id,)).fetchone()
        if not batch:
            raise ValueError('unknown_batch')
        validate_owner(profile, batch[1])
        reply_source = conn.execute('SELECT source FROM reply WHERE batch_id=?',
                                    (batch_id,)).fetchall()
        if len(reply_source) != 1:
            raise ValueError('reply_not_unique')
        if reply_source[0][0] == 'card':
            from review_card_adapter import verify_card_reply_evidence
            evidence = verify_card_reply_evidence(conn, batch_id)
            return build_handoff(conn, _load_work_query_adapter(), batch_id,
                                 card_evidence=evidence)
        if reply_source[0][0] != 'chat':
            raise ValueError('reply_source_invalid')
        chat = read_dws(['chat', '+chat-messages', '--open-dingtalk-id', bot_open_id,
                         '--start', batch[2], '--page-all', '--max-items', '200',
                         '--no-reactions', '--profile', profile])
        if (chat.get('complete') is not True or chat.get('partial') is not False
                or chat.get('hasMore') is not False or chat.get('failures') != []):
            raise ValueError('incomplete_message_read')
        stored = conn.execute('SELECT message_id FROM reply WHERE batch_id=?', (batch_id,)).fetchall()
        if len(stored) != 1:
            raise ValueError('reply_not_unique')
        matches = [m for m in chat.get('messages', []) if m.get('messageId') == stored[0][0]]
        if len(matches) != 1:
            raise ValueError('reply_not_visible')
        return build_handoff(conn, _load_work_query_adapter(), batch_id, matches[0])


def main():
    parser = argparse.ArgumentParser()
    for flag in ('db', 'batch', 'profile', 'bot-open-id'):
        parser.add_argument('--' + flag, required=True)
    args = parser.parse_args()
    try:
        conn = connect(args.db)
        print(json.dumps(verified_handoff(conn, args.batch, args.profile, args.bot_open_id), ensure_ascii=False))
        return 0
    except (ValueError, RuntimeError, OSError, json.JSONDecodeError, subprocess.TimeoutExpired) as exc:
        print(json.dumps({'status': 'error', 'reason': str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
