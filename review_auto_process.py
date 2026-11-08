"""Safely advance verified Cifra REVIEW replies through the existing resolvers.

The worker acknowledges a verified reply once, applies only explicit ignore
decisions automatically, and leaves creates pending until a caller supplies a
human-confirmed title and duplicate query bound to the same ordinal and source.
"""
from __future__ import annotations

import hashlib
import json
import re
import subprocess
from datetime import datetime
from typing import Any, Callable
from zoneinfo import ZoneInfo

from review_inbox import parse_reply
from review_inbox_poll import read_dws
from review_identity import validate_owner

ACK_TEXT = '收到'
COMPLETE_TEXT = '处理已完成'
PENDING_TEXT = '处理结果尚未全部确认。'


def ensure_schema(conn) -> None:
    conn.execute('''
        CREATE TABLE IF NOT EXISTS review_auto_enabled (
          batch_id TEXT PRIMARY KEY,
          enabled_at TEXT NOT NULL
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS review_auto_notice (
          batch_id TEXT NOT NULL,
          kind TEXT NOT NULL CHECK(kind IN ('ack','partial','complete')),
          text TEXT NOT NULL,
          status TEXT NOT NULL CHECK(status IN ('sending','unknown','delivered')),
          claimed_at TEXT NOT NULL,
          baseline_message_ids TEXT NOT NULL,
          delivered_message_id TEXT,
          PRIMARY KEY(batch_id, kind)
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS review_auto_terminal (
          batch_id TEXT PRIMARY KEY REFERENCES batch(id),
          status TEXT NOT NULL CHECK(status IN ('complete','all_ignored'))
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS review_clarification (
          batch_id TEXT PRIMARY KEY REFERENCES batch(id), prompt_text TEXT NOT NULL,
          status TEXT NOT NULL CHECK(status IN ('sending','unknown','delivered')),
          claimed_at TEXT NOT NULL, baseline_message_ids TEXT NOT NULL,
          bot_id TEXT NOT NULL, prompt_message_id TEXT, prompt_time TEXT
        )
    ''')


def _complete_history(payload: Any) -> list[dict[str, Any]]:
    if (not isinstance(payload, dict) or payload.get('complete') is not True
            or payload.get('partial') is not False or payload.get('hasMore') is not False
            or payload.get('failures') != [] or not isinstance(payload.get('messages'), list)):
        raise ValueError('incomplete_message_read')
    messages = payload['messages']
    if not all(isinstance(message, dict) for message in messages):
        raise ValueError('invalid_message_row')
    return messages


def _read_history(bot_open_id: str, profile: str, prompt_time: str) -> list[dict[str, Any]]:
    payload = read_dws(['chat', '+chat-messages', '--open-dingtalk-id', bot_open_id,
                        '--start', prompt_time, '--page-all', '--max-items', '200',
                        '--no-reactions', '--profile', profile])
    return _complete_history(payload)


def _verified_reply(conn, batch_id: str, history: list[dict[str, Any]]) -> dict[str, Any]:
    batch = conn.execute('SELECT conversation_id,owner_id,prompt_time,state FROM batch WHERE id=?',
                         (batch_id,)).fetchone()
    replies = conn.execute('SELECT message_id,message_time,content_sha256,content,decisions,source '
                            'FROM reply WHERE batch_id=?', (batch_id,)).fetchall()
    if not batch or batch[3] != 'answered' or len(replies) != 1:
        raise ValueError('batch_or_reply_not_ready')
    conversation, owner, prompt_time, _ = batch
    message_id, message_time, digest, content, saved, source = replies[0]
    if (not isinstance(content, str) or hashlib.sha256(content.encode('utf-8')).hexdigest() != digest
            or not isinstance(message_id, str) or not message_id
            or not isinstance(message_time, str) or message_time <= prompt_time):
        raise ValueError('stored_reply_integrity_failed')
    items = conn.execute('SELECT ordinal FROM item WHERE batch_id=? ORDER BY ordinal',
                         (batch_id,)).fetchall()
    if not items or [row[0] for row in items] != list(range(1, len(items) + 1)):
        raise ValueError('invalid_item_ordinals')
    clarification = conn.execute(
        'SELECT prompt_text,status,claimed_at,baseline_message_ids,bot_id,prompt_message_id,prompt_time '
        'FROM review_clarification WHERE batch_id=?', (batch_id,)
    ).fetchone()
    decisions = parse_reply(content, len(items), batch_id=batch_id) if clarification else parse_reply(content, len(items))
    if json.loads(saved) != {str(key): value for key, value in decisions.items()}:
        raise ValueError('reply_decision_mismatch')
    if source == 'card':
        from review_card_adapter import verify_card_reply_evidence
        card_reply = verify_card_reply_evidence(conn, batch_id)
        if (card_reply.get('conversation_id') != conversation
                or card_reply.get('owner_id') != owner
                or card_reply.get('reply_message_id') != message_id
                or card_reply.get('reply_time') != message_time
                or card_reply.get('reply_text') != content
                or card_reply.get('decisions') != decisions):
            raise ValueError('card_reply_identity_mismatch')
        return {'conversation_id': conversation, 'owner_id': owner, 'prompt_time': prompt_time,
                'reply_message_id': message_id, 'reply_time': message_time,
                'reply_text': content, 'decisions': decisions, 'reply_source': 'card'}
    if source != 'chat':
        raise ValueError('reply_source_invalid')
    candidates = []
    for message in history:
        if message.get('conversationId') != conversation or message.get('senderId') != owner:
            continue
        created = message.get('createTime')
        if not isinstance(created, str):
            raise ValueError('invalid_message_time_or_row')
        try:
            datetime.strptime(created, '%Y-%m-%d %H:%M:%S')
        except ValueError as exc:
            raise ValueError('invalid_message_time_or_row') from exc
        if created >= prompt_time:
            candidates.append(message)

    if clarification:
        clarification_text, clarification_status, claimed_at, baseline_json, bot_id, clarification_id, clarification_time = clarification
        if (clarification_status != 'delivered' or not isinstance(clarification_text, str)
                or not isinstance(bot_id, str) or not bot_id
                or not isinstance(clarification_id, str) or not clarification_id
                or not isinstance(clarification_time, str) or clarification_time <= prompt_time):
            raise ValueError('clarification_evidence_mismatch')
        clarification_messages = [message for message in history
                                  if message.get('conversationId') == conversation
                                  and message.get('senderId') == bot_id
                                  and message.get('messageId') == clarification_id
                                  and message.get('createTime') == clarification_time
                                  and isinstance(message.get('text'), str)
                                  and re.sub(r'\s+', '', message['text'])
                                  == re.sub(r'\s+', '', clarification_text)]
        if len(clarification_messages) != 1:
            raise ValueError('clarification_evidence_mismatch')

        stored_candidates = conn.execute(
            'SELECT message_id,message_time,content_sha256,content FROM reply_candidate '
            'WHERE batch_id=?', (batch_id,)
        ).fetchall()
        saved_by_id = {row[0]: (row[1], row[2], row[3]) for row in stored_candidates}
        live_by_id = {}
        for message in candidates:
            candidate_id, created, text = (message.get('messageId'), message.get('createTime'), message.get('text'))
            if not isinstance(candidate_id, str) or not candidate_id or not isinstance(text, str) or len(text) > 4000:
                raise ValueError('reply_evidence_mismatch')
            if candidate_id in live_by_id:
                raise ValueError('reply_evidence_mismatch')
            live_by_id[candidate_id] = (created, hashlib.sha256(text.encode('utf-8')).hexdigest(), text)
        if saved_by_id != live_by_id or len(stored_candidates) != len(saved_by_id):
            raise ValueError('reply_evidence_mismatch')
        post_clarification = [message for message in candidates
                              if message.get('createTime', '') > clarification_time]
        if len(post_clarification) != 1:
            raise ValueError('multiple_replies_need_review')
        live = post_clarification[0]
    else:
        if len(candidates) != 1:
            raise ValueError('multiple_replies_need_review')
        live = candidates[0]
    if (live.get('messageId') != message_id or live.get('createTime') != message_time
            or live.get('text') != content or message_time <= prompt_time):
        raise ValueError('reply_evidence_mismatch')
    return {'conversation_id': conversation, 'owner_id': owner, 'prompt_time': prompt_time,
            'reply_message_id': message_id, 'reply_time': message_time,
            'reply_text': content, 'decisions': decisions}


def _historical_bot_binding(conn, profile: str, bot_open_id: str, app_id: str,
                            conversation_id: str, owner_open_id: str) -> bool:
    from types import SimpleNamespace
    from review_dispatch_send import verify_context
    rows = conn.execute("SELECT id,prompt_message_id,prompt_time FROM dispatch "
                        "WHERE status='delivered' AND conversation_id=? AND owner_id=? "
                        "AND preview_hash IS NOT NULL AND prompt_message_id IS NOT NULL",
                        (conversation_id, owner_open_id)).fetchall()
    for batch_id, prompt_id, prompt_time in rows:
        args = SimpleNamespace(batch=batch_id, owner_open_id=owner_open_id, profile=profile,
                               bot_open_id=bot_open_id, app_id=app_id)
        context = verify_context(conn, args)
        if context.get('digest') != context.get('preview_hash'):
            continue
        history = _read_history(bot_open_id, profile, prompt_time)
        expected_text = context.get('text')
        matches = [m for m in history if m.get('messageId') == prompt_id
                   and m.get('conversationId') == conversation_id
                   and m.get('senderId') == bot_open_id and isinstance(m.get('text'), str)
                   and isinstance(expected_text, str)
                   and re.sub(r'\s+', '', m['text']) == re.sub(r'\s+', '', expected_text)]
        if len(matches) == 1:
            return True
    return False


def _verified_send_context(profile: str, bot_open_id: str, app_id: str,
                           conversation_id: str, owner_open_id: str, conn=None) -> tuple[str, str]:
    owner_user_id = validate_owner(profile, owner_open_id)
    info = read_dws(['chat', '+conversation-info', '--open-dingtalk-id', bot_open_id,
                     '--profile', profile])
    chat = info.get('result', {}).get('conversationInfo', {})
    if (info.get('success') is not True or chat.get('singleChat') is not True
            or chat.get('openConversationId') != conversation_id):
        raise ValueError('conversation_identity_mismatch')
    robot = read_dws(['dev', 'app', 'robot', 'get', '--unified-app-id', app_id,
                      '--profile', profile])
    details = robot.get('data') or {}
    robot_code = details.get('robotCode')
    if (robot.get('ok') is not True or details.get('robotStatus') != 'ONLINE'
            or not isinstance(robot_code, str) or not robot_code):
        raise ValueError('cifra_robot_identity_unverified')
    known_open_id = details.get('openDingTalkId') or details.get('openId')
    if known_open_id:
        if known_open_id != bot_open_id:
            raise ValueError('cifra_robot_identity_mismatch')
    elif conn is None or not _historical_bot_binding(conn, profile, bot_open_id, app_id,
                                                     conversation_id, owner_open_id):
        raise ValueError('cifra_robot_identity_mismatch')
    return owner_user_id, robot_code


def _reconcile_notice(conn, batch_id: str, kind: str, bot_open_id: str,
                      history: list[dict[str, Any]]) -> str:
    batch = conn.execute('SELECT conversation_id FROM batch WHERE id=?', (batch_id,)).fetchone()
    if not batch:
        raise ValueError('unknown_batch')
    conversation = batch[0]
    notice = conn.execute('SELECT text,status,claimed_at,baseline_message_ids '
                          'FROM review_auto_notice WHERE batch_id=? AND kind=?',
                          (batch_id, kind)).fetchone()
    if not notice:
        raise ValueError('notice_not_found')
    text, status, claimed_at, baseline_json = notice
    baseline_ids = set(json.loads(baseline_json))
    candidates = [message for message in history
                  if message.get('conversationId') == conversation
                  and message.get('senderId') == bot_open_id
                  and isinstance(message.get('text'), str)
                  and message['text'].strip() == text.strip()
                  and isinstance(message.get('messageId'), str)
                  and message.get('messageId') not in baseline_ids
                  and isinstance(message.get('createTime'), str)
                  and message['createTime'] >= claimed_at]
    if len(candidates) != 1:
        if status == 'sending':
            conn.execute("UPDATE review_auto_notice SET status='unknown' WHERE batch_id=? AND kind=?",
                         (batch_id, kind))
        return 'unknown'
    message_id = candidates[0]['messageId']
    conn.execute("UPDATE review_auto_notice SET status='delivered',delivered_message_id=? "
                 'WHERE batch_id=? AND kind=?', (message_id, batch_id, kind))
    return 'delivered'


def _send_once(conn, batch_id: str, kind: str, text: str, profile: str, bot_open_id: str,
               app_id: str, owner_user_id: str, robot_code: str,
               prompt_time: str) -> str:
    ensure_schema(conn)
    prior = conn.execute('SELECT text,status FROM review_auto_notice WHERE batch_id=? AND kind=?',
                         (batch_id, kind)).fetchone()
    if prior and prior[0] != text:
        raise ValueError('notice_text_conflict')
    if prior and prior[1] == 'delivered':
        return 'delivered'
    history = _read_history(bot_open_id, profile, prompt_time)
    if prior:
        return _reconcile_notice(conn, batch_id, kind, bot_open_id, history)
    conversation = conn.execute('SELECT conversation_id FROM batch WHERE id=?',
                                (batch_id,)).fetchone()
    if not conversation:
        raise ValueError('unknown_batch')
    baseline_ids = sorted({str(message['messageId']) for message in history
                           if isinstance(message.get('messageId'), str)})
    claimed_at = datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
    conn.execute('BEGIN IMMEDIATE')
    try:
        conn.execute('INSERT INTO review_auto_notice '
                     '(batch_id,kind,text,status,claimed_at,baseline_message_ids) '
                     'VALUES (?,?,?,\'sending\',?,?)',
                     (batch_id, kind, text, claimed_at,
                      json.dumps(baseline_ids, separators=(',', ':'))))
        conn.commit()
    except BaseException:
        conn.rollback()
        existing = conn.execute('SELECT status FROM review_auto_notice WHERE batch_id=? AND kind=?',
                                 (batch_id, kind)).fetchone()
        if existing:
            return _reconcile_notice(conn, batch_id, kind, bot_open_id,
                                     _read_history(bot_open_id, profile, prompt_time))
        raise
    try:
        response = read_dws(['chat', '+messages-send', '--as', 'bot', '--robot-code', robot_code,
                             '--users', owner_user_id, '--title', '待办复核', '--text', text,
                             '--profile', profile, '--yes'])
        if response.get('ok') is not True or response.get('result', {}).get('success') is not True:
            conn.execute("UPDATE review_auto_notice SET status='unknown' WHERE batch_id=? AND kind=?",
                         (batch_id, kind))
    except Exception:
        conn.execute("UPDATE review_auto_notice SET status='unknown' WHERE batch_id=? AND kind=?",
                     (batch_id, kind))
    after = _read_history(bot_open_id, profile, prompt_time)
    return _reconcile_notice(conn, batch_id, kind, bot_open_id, after)


def _send_ack_notice(conn, batch_id: str, profile: str, bot_open_id: str,
                     app_id: str, owner_user_id: str, robot_code: str,
                     prompt_time: str, *, enabled: bool = True) -> str:
    if not enabled:
        return 'delivered'
    return _send_once(conn, batch_id, 'ack', ACK_TEXT, profile, bot_open_id, app_id,
                      owner_user_id, robot_code, prompt_time)


def _verify_ambiguous_batch_evidence(conn, batch_id: str, bot_open_id: str,
                                     history: list[dict[str, Any]]) -> tuple[str, str, str, int]:
    batch = conn.execute('SELECT conversation_id,owner_id,prompt_message_id,prompt_time,state '
                         'FROM batch WHERE id=?', (batch_id,)).fetchone()
    if not batch or batch[4] != 'open':
        raise ValueError('batch_or_reply_not_ready')
    conversation, owner, prompt_id, prompt_time, _ = batch
    owner_messages = []
    for message in history:
        if message.get('conversationId') != conversation or message.get('senderId') != owner:
            continue
        created = message.get('createTime')
        if not isinstance(created, str):
            raise ValueError('invalid_message_time_or_row')
        try:
            datetime.strptime(created, '%Y-%m-%d %H:%M:%S')
        except ValueError as exc:
            raise ValueError('invalid_message_time_or_row') from exc
        if created >= prompt_time:
            message_id, text = message.get('messageId'), message.get('text')
            if not isinstance(message_id, str) or not message_id or not isinstance(text, str) or len(text) > 4000:
                raise ValueError('invalid_reply_candidate')
            owner_messages.append((message_id, created, hashlib.sha256(text.encode('utf-8')).hexdigest(), text))
    if not owner_messages:
        raise ValueError('reply_candidate_missing')
    stored = conn.execute('SELECT message_id,message_time,content_sha256,content FROM reply_candidate '
                          'WHERE batch_id=? ORDER BY message_id', (batch_id,)).fetchall()
    if (len(stored) != len(owner_messages)
            or sorted(stored) != sorted(owner_messages)):
        raise ValueError('reply_candidate_evidence_mismatch')

    prompt_matches = [message for message in history
                      if message.get('conversationId') == conversation
                      and message.get('messageId') == prompt_id
                      and message.get('createTime') == prompt_time]
    if len(prompt_matches) != 1 or prompt_matches[0].get('senderId') != bot_open_id:
        raise ValueError('prompt_evidence_mismatch')
    dispatch = conn.execute('SELECT text,status,prompt_message_id,prompt_time FROM dispatch WHERE id=?',
                            (batch_id,)).fetchone()
    if dispatch:
        expected_text, status, dispatch_prompt_id, dispatch_prompt_time = dispatch
        if (status != 'delivered' or dispatch_prompt_id != prompt_id or dispatch_prompt_time != prompt_time
                or not isinstance(expected_text, str) or not isinstance(prompt_matches[0].get('text'), str)
                or re.sub(r'\s+', '', expected_text)
                != re.sub(r'\s+', '', prompt_matches[0]['text'])):
            raise ValueError('prompt_evidence_mismatch')
    count = conn.execute('SELECT COUNT(*) FROM item WHERE batch_id=?', (batch_id,)).fetchone()[0]
    if count < 1:
        raise ValueError('invalid_item_ordinals')
    return conversation, owner, prompt_time, count


def _clarification_text(batch_id: str, count: int) -> str:
    example = '，'.join(f'{ordinal}，忽略' for ordinal in range(1, count + 1))
    return (f'复核编号：{batch_id}\n'
            '我无法确认之前的回复对应哪项决定，旧回复仅保留记录，不会据此处理。'
            '请发一条新的完整决定，为每个序号明确填写“忽略”或“创建”；如附复核编号，必须与本条一致。\n'
            f'格式示例：复核编号：{batch_id}；{example}')


def _reconcile_clarification(conn, batch_id: str, bot_open_id: str,
                             history: list[dict[str, Any]]) -> str:
    row = conn.execute('SELECT prompt_text,status,claimed_at,baseline_message_ids,bot_id '
                       'FROM review_clarification WHERE batch_id=?', (batch_id,)).fetchone()
    batch = conn.execute('SELECT conversation_id,prompt_time FROM batch WHERE id=?', (batch_id,)).fetchone()
    if not row or not batch:
        raise ValueError('clarification_not_found')
    text, status, claimed_at, baseline_json, bot_id = row
    conversation, batch_prompt_time = batch
    if bot_id != bot_open_id:
        raise ValueError('clarification_bot_identity_mismatch')
    baseline_ids = set(json.loads(baseline_json))
    matches = [message for message in history
               if message.get('conversationId') == conversation
               and message.get('senderId') == bot_open_id
               and isinstance(message.get('messageId'), str)
               and message.get('messageId') not in baseline_ids
               and isinstance(message.get('createTime'), str)
               and message['createTime'] >= claimed_at
               and isinstance(message.get('text'), str)
               and re.sub(r'\s+', '', message['text']) == re.sub(r'\s+', '', text)]
    if len(matches) != 1:
        if status == 'sending':
            conn.execute("UPDATE review_clarification SET status='unknown' WHERE batch_id=?", (batch_id,))
        return 'unknown'
    prompt = matches[0]
    try:
        datetime.strptime(prompt.get('createTime', ''), '%Y-%m-%d %H:%M:%S')
    except ValueError as exc:
        raise ValueError('clarification_time_invalid') from exc
    if prompt['createTime'] <= batch_prompt_time:
        raise ValueError('clarification_time_invalid')
    conn.execute("UPDATE review_clarification SET status='delivered',prompt_message_id=?,prompt_time=? "
                 'WHERE batch_id=?', (prompt['messageId'], prompt['createTime'], batch_id))
    return 'delivered'


def _send_clarification_once(conn, batch_id: str, text: str, profile: str, bot_open_id: str,
                             owner_user_id: str, robot_code: str, prompt_time: str) -> str:
    prior = conn.execute('SELECT prompt_text,status FROM review_clarification WHERE batch_id=?',
                         (batch_id,)).fetchone()
    if prior and prior[0] != text:
        raise ValueError('clarification_text_conflict')
    if prior and prior[1] == 'delivered':
        return 'delivered'
    history = _read_history(bot_open_id, profile, prompt_time)
    if prior:
        return _reconcile_clarification(conn, batch_id, bot_open_id, history)
    conversation = conn.execute('SELECT conversation_id FROM batch WHERE id=?', (batch_id,)).fetchone()
    if not conversation:
        raise ValueError('unknown_batch')
    baseline_ids = sorted({str(message['messageId']) for message in history
                           if isinstance(message.get('messageId'), str)})
    claimed_at = datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
    conn.execute('BEGIN IMMEDIATE')
    try:
        conn.execute('INSERT INTO review_clarification '
                     '(batch_id,prompt_text,status,claimed_at,baseline_message_ids,bot_id) '
                     'VALUES (?,? ,\'sending\',?,?,?)',
                     (batch_id, text, claimed_at, json.dumps(baseline_ids, separators=(',', ':')), bot_open_id))
        conn.commit()
    except BaseException:
        conn.rollback()
        existing = conn.execute('SELECT status FROM review_clarification WHERE batch_id=?',
                                 (batch_id,)).fetchone()
        if existing:
            return _reconcile_clarification(conn, batch_id, bot_open_id,
                                             _read_history(bot_open_id, profile, prompt_time))
        raise
    try:
        response = read_dws(['chat', '+messages-send', '--as', 'bot', '--robot-code', robot_code,
                             '--users', owner_user_id, '--title', '待办复核', '--text', text,
                             '--profile', profile, '--yes'])
        if response.get('ok') is not True or response.get('result', {}).get('success') is not True:
            conn.execute("UPDATE review_clarification SET status='unknown' WHERE batch_id=?", (batch_id,))
    except Exception:
        conn.execute("UPDATE review_clarification SET status='unknown' WHERE batch_id=?", (batch_id,))
    after = _read_history(bot_open_id, profile, prompt_time)
    return _reconcile_clarification(conn, batch_id, bot_open_id, after)


def process_needs_review_batch(conn, batch_id: str, profile: str, bot_open_id: str,
                               app_id: str | None, history_payload: Any,
                               *, send_ack: bool = True) -> dict[str, Any]:
    """Acknowledge and clarify one verified ambiguous reply without selecting it."""
    ensure_schema(conn)
    if conn.execute('SELECT 1 FROM review_auto_enabled WHERE batch_id=?', (batch_id,)).fetchone() is None:
        return {'status': 'pending', 'reason': 'batch_automation_not_enabled', 'batch': batch_id}
    if not isinstance(app_id, str) or not app_id.strip():
        return {'status': 'pending', 'reason': 'cifra_app_id_not_configured', 'batch': batch_id}
    history = _complete_history(history_payload)
    conversation, owner, prompt_time, count = _verify_ambiguous_batch_evidence(
        conn, batch_id, bot_open_id, history)
    owner_user_id, robot_code = _verified_send_context(profile, bot_open_id, app_id,
                                                        conversation, owner, conn)
    ack = _send_ack_notice(conn, batch_id, profile, bot_open_id, app_id,
                           owner_user_id, robot_code, prompt_time, enabled=send_ack)
    if ack != 'delivered':
        return {'status': 'pending', 'reason': 'ack_delivery_unknown', 'batch': batch_id}
    clarification = _send_clarification_once(
        conn, batch_id, _clarification_text(batch_id, count), profile, bot_open_id,
        owner_user_id, robot_code, prompt_time,
    )
    if clarification != 'delivered':
        return {'status': 'pending', 'reason': 'clarification_delivery_unknown', 'batch': batch_id}
    return {'status': 'clarification_sent', 'batch': batch_id}


def _resolution(conn, batch_id: str, ordinal: int):
    return conn.execute('SELECT decision,status,external_task_id,request_json FROM resolution '
                        'WHERE batch_id=? AND ordinal=?', (batch_id, ordinal)).fetchone()


def _apply_ignore(conn, batch_id: str, item: dict[str, Any], profile: str,
                  bot_open_id: str, lock_path: str) -> dict[str, Any]:
    ordinal = item['ordinal']
    previous = _resolution(conn, batch_id, ordinal)
    if previous:
        if previous[0] == 'ignore' and previous[1] == 'applied':
            return {'status': 'applied', 'ordinal': ordinal}
        return {'status': 'pending', 'ordinal': ordinal, 'reason': 'prior_resolution_needs_reconciliation'}
    if item.get('handoff_state') != 'ready_for_existing_worker_review' or len(item.get('sources', [])) != 1:
        return {'status': 'pending', 'ordinal': ordinal, 'reason': 'action_state_requires_review'}
    from review_resolve_ignore import resolve
    try:
        result = resolve(conn, batch_id, ordinal, profile, bot_open_id, lock_path, apply=True)
    except Exception:
        return {'status': 'failed', 'ordinal': ordinal, 'reason': 'ignore_resolver_failed'}
    if not isinstance(result, dict) or result.get('status') != 'applied':
        status = 'pending' if isinstance(result, dict) and 'reconciliation' in str(result.get('status')) else 'failed'
        return {'status': status, 'ordinal': ordinal,
                'reason': str(result.get('status') if isinstance(result, dict) else 'resolver_result_invalid')}
    return {'status': 'applied', 'ordinal': ordinal}


def _create_plan_is_bound(plan: Any, batch_id: str, ordinal: int,
                          action_id: str, reply_message_id: str) -> bool:
    return (isinstance(plan, dict) and plan.get('owner_confirmed') is True
            and str(plan.get('batch_id')) == batch_id and plan.get('ordinal') == ordinal
            and str(plan.get('action_id')) == str(action_id)
            and plan.get('reply_message_id') == reply_message_id
            and isinstance(plan.get('title'), str) and 1 <= len(plan['title'].strip()) <= 100
            and isinstance(plan.get('duplicate_query'), str) and bool(plan['duplicate_query'].strip()))


def source_grounded_plan(item: dict[str, Any], batch_id: str, reply_message_id: str) -> dict[str, Any] | None:
    """Use a short exact source and only a strictly evidenced deadline."""
    sources = item.get('sources')
    if item.get('user_decision') != 'create' or not isinstance(sources, list) or len(sources) != 1:
        return None
    source = sources[0]
    text = (source.get('source') or {}).get('text')
    if not isinstance(text, str):
        return None
    title = text.strip()
    if '@' in title:
        body, mentions = title.split('@', 1)
        mention = r'(?:工班长|所有人|[^@\s()]+\([^()]+\))'
        if not re.fullmatch(mention + r'(?:\s*@' + mention + r')*', mentions.strip()):
            return None
        title = body.strip()
    if not 4 <= len(title) <= 80 or any(c in title for c in ('\n', '\r', '@', '[', ']')):
        return None
    due_iso = None
    if re.search(r'\d|今天|明天|后天|今晚|周|星期|月|季度|年底|年前|截止|截至|日前|下班|上午|下午|晚上|点钟', title):
        from review_date_plan import source_deadline
        due_iso = source_deadline(title, (source.get('source') or {}).get('message_time'))
        if due_iso is None:
            return None
    result = {'owner_confirmed': True, 'batch_id': batch_id, 'ordinal': item.get('ordinal'),
            'action_id': str(source.get('action_id') or ''), 'reply_message_id': reply_message_id,
            'title': title, 'duplicate_query': title, 'title_origin': 'exact_source'}
    if due_iso:
        result['due_iso'] = due_iso
    return result


def _apply_create(conn, batch_id: str, item: dict[str, Any], profile: str,
                  bot_open_id: str, lock_path: str,
                  plan_provider: Callable[[dict[str, Any]], dict[str, Any] | None] | None,
                  reply_message_id: str) -> dict[str, Any]:
    ordinal = item['ordinal']
    previous = _resolution(conn, batch_id, ordinal)
    if previous:
        if previous[0] == 'create' and previous[1] == 'applied' and previous[2]:
            return {'status': 'applied', 'ordinal': ordinal, 'external_task_id': previous[2]}
        if previous[0] == 'create' and previous[1] == 'applying' and previous[2]:
            sources = item.get('sources')
            try:
                audit = json.loads(previous[3])
            except (TypeError, json.JSONDecodeError):
                audit = None
            source = sources[0] if isinstance(sources, list) and len(sources) == 1 else None
            claim = audit.get('claim') if isinstance(audit, dict) else None
            local_action = conn.execute(
                'SELECT action_id,reply_message_id FROM resolution WHERE batch_id=? AND ordinal=?',
                (batch_id, ordinal),
            ).fetchone()
            source_is_bound = (
                isinstance(source, dict)
                and isinstance(claim, dict)
                and item.get('user_decision') == 'create'
                and item.get('ordinal') == ordinal
                and local_action is not None
                and str(local_action[0]) == str(source.get('action_id'))
                and local_action[1] == reply_message_id
                and str(claim.get('action_id')) == str(source.get('action_id'))
                and claim.get('conversation_id') == source.get('conversation_id')
                and claim.get('message_id') == source.get('message_id')
                and audit.get('source_message_id') == source.get('message_id')
                and audit.get('reply_message_id') == reply_message_id
                and source.get('external_task_id') == previous[2]
                and isinstance(audit.get('title'), str)
                and bool(audit['title'].strip())
            )
            if not source_is_bound:
                return {'status': 'pending', 'ordinal': ordinal,
                        'reason': 'prior_resolution_source_binding_mismatch'}
            from review_resolve_create import resolve
            try:
                result = resolve(
                    conn, batch_id, ordinal, profile, bot_open_id, lock_path,
                    audit['title'], audit.get('duplicate_query') or audit['title'],
                    apply=True, reconcile=True,
                )
            except Exception:
                return {'status': 'failed', 'ordinal': ordinal,
                        'reason': 'create_reconciliation_failed'}
            if (isinstance(result, dict) and result.get('status') == 'applied'
                    and str(result.get('external_task_id') or '') == str(previous[2])):
                return {'status': 'applied', 'ordinal': ordinal,
                        'external_task_id': str(previous[2])}
            return {'status': 'pending', 'ordinal': ordinal,
                    'reason': 'create_reconciliation_incomplete'}
        return {'status': 'pending', 'ordinal': ordinal, 'reason': 'prior_resolution_needs_reconciliation'}
    sources = item.get('sources')
    if (item.get('handoff_state') != 'ready_for_existing_worker_review'
            or not isinstance(sources, list) or len(sources) != 1):
        return {'status': 'pending', 'ordinal': ordinal, 'reason': 'create_action_requires_manual_review'}
    source = sources[0]
    try:
        plan = plan_provider(item) if plan_provider else None
    except Exception:
        return {'status': 'failed', 'ordinal': ordinal, 'reason': 'create_plan_provider_failed'}
    if not _create_plan_is_bound(plan, batch_id, ordinal, source.get('action_id'), reply_message_id):
        return {'status': 'pending', 'ordinal': ordinal,
                'reason': 'create_requires_owner_confirmed_title_and_duplicate_query'}
    original = source.get('source') or {}
    original_text = original.get('text')
    has_time_context = isinstance(original_text, str) and re.search(
        r'\d|今天|明天|后天|今晚|周|星期|月|季度|年底|年前|截止|截至|日前|下班|上午|下午|晚上|点钟', original_text)
    if plan.get('due_iso') is not None or has_time_context:
        from review_date_plan import source_deadline
        expected_due = source_deadline(original_text, original.get('message_time'))
        if expected_due is None or expected_due != plan.get('due_iso'):
            return {'status': 'pending', 'ordinal': ordinal, 'reason': 'create_deadline_not_source_bound'}
    from review_resolve_create import resolve
    try:
        kwargs = {'apply': True}
        if plan.get('due_iso') is not None:
            kwargs['due_iso'] = plan['due_iso']
        result = resolve(conn, batch_id, ordinal, profile, bot_open_id, lock_path,
                         plan['title'], plan['duplicate_query'], **kwargs)
    except Exception:
        return {'status': 'failed', 'ordinal': ordinal, 'reason': 'create_resolver_failed'}
    if not isinstance(result, dict) or result.get('status') != 'applied' or not result.get('external_task_id'):
        status = 'pending' if isinstance(result, dict) and 'reconciliation' in str(result.get('status')) else 'failed'
        return {'status': status, 'ordinal': ordinal,
                'reason': str(result.get('status') if isinstance(result, dict) else 'resolver_result_invalid')}
    return {'status': 'applied', 'ordinal': ordinal, 'external_task_id': str(result['external_task_id'])}


def _verify_created_task(conn, batch_id: str, ordinal: int, profile: str,
                         task_id: str) -> None:
    row = conn.execute('SELECT request_json FROM resolution WHERE batch_id=? AND ordinal=?',
                       (batch_id, ordinal)).fetchone()
    if not row:
        raise ValueError('create_resolution_missing')
    audit = json.loads(row[0])
    title = audit.get('title')
    executor_id = str(audit.get('executor_user_id') or '')
    identity = audit.get('contact_identity') or {}
    executor_name = identity.get('name')
    if not isinstance(title, str) or not executor_id or not isinstance(executor_name, str):
        raise ValueError('create_readback_plan_missing')
    from dws_adapter import DwsClient, verify_todo
    detail = DwsClient(confirmed_official_writes=True).get_todo(task_id)
    if str(detail.get('taskId') or detail.get('todoTaskId') or '') != str(task_id):
        raise ValueError('create_task_identity_mismatch')
    plan = {'decision': 'CREATE', 'title': title, 'executor_ids': [executor_id],
            'executor_names': [executor_name], 'priority': 20,
            'allow_executor_namespace_fallback': True, 'due_iso': audit.get('due_iso')}
    problems = verify_todo(detail, plan)
    if not audit.get('due_iso') and (detail.get('dueTime') not in (None, 0, '') or detail.get('due') not in (None, 0, '')):
        problems.append('unexpected_due_date')
    if problems:
        raise ValueError('create_task_readback_mismatch:' + ','.join(problems))


def _closure_state(conn, handoff: dict[str, Any], profile: str) -> tuple[str, list[int]]:
    items = handoff.get('items')
    batch_id = handoff.get('batch')
    if not isinstance(items, list) or not items or not isinstance(batch_id, str):
        raise ValueError('handoff_invalid')
    completed, pending, created = 0, [], 0
    for item in items:
        ordinal = item.get('ordinal')
        decision = item.get('user_decision')
        resolution = _resolution(conn, batch_id, ordinal)
        if not resolution or resolution[1] != 'applied' or resolution[0] != decision:
            pending.append(ordinal)
            continue
        if decision == 'ignore':
            if item.get('handoff_state') != 'resolved_ignore':
                pending.append(ordinal)
                continue
        elif decision == 'create':
            if (not resolution[2]
                    or item.get('handoff_state') not in ('resolved_task', 'resolved_duplicate_family')):
                pending.append(ordinal)
                continue
            _verify_created_task(conn, batch_id, ordinal, profile, str(resolution[2]))
            created += 1
        else:
            pending.append(ordinal)
            continue
        completed += 1
    if pending:
        return 'pending', pending
    if completed == len(items) and created > 0:
        return 'complete', []
    if completed == len(items):
        return 'all_ignored', []
    return 'pending', list(range(1, len(items) + 1))


def process_answered_batch(conn, batch_id: str, profile: str, bot_open_id: str,
                           app_id: str | None, lock_path: str,
                           create_plan_provider: Callable[[dict[str, Any]], dict[str, Any] | None] | None = None,
                           handoff_reader: Callable[..., dict[str, Any]] | None = None,
                           *, send_ack: bool = True) -> dict[str, Any]:
    """Acknowledge, resolve safe owner choices, and report verified closure state.

    ``create_plan_provider`` is the future/manual-confirmation seam. It must return
    a plan bound to this batch, ordinal, action ID, and reply message ID with
    ``owner_confirmed=True``; this worker deliberately installs no title generator.
    """
    ensure_schema(conn)
    enabled = conn.execute('SELECT 1 FROM review_auto_enabled WHERE batch_id=?', (batch_id,)).fetchone()
    if not enabled:
        return {'status': 'pending', 'reason': 'batch_automation_not_enabled',
                'batch': batch_id, 'pending_ordinals': []}
    terminal = conn.execute('SELECT status FROM review_auto_terminal WHERE batch_id=?',
                            (batch_id,)).fetchone()
    if terminal:
        return {'status': terminal[0], 'batch': batch_id, 'pending_ordinals': []}
    if not isinstance(app_id, str) or not app_id.strip():
        return {'status': 'pending', 'reason': 'cifra_app_id_not_configured',
                'batch': batch_id, 'pending_ordinals': []}
    batch = conn.execute('SELECT conversation_id,owner_id,prompt_time,state FROM batch WHERE id=?',
                         (batch_id,)).fetchone()
    if not batch or batch[3] != 'answered':
        raise ValueError('batch_or_reply_not_ready')
    conversation, owner, prompt_time, _ = batch
    owner_user_id, robot_code = _verified_send_context(profile, bot_open_id, app_id,
                                                        conversation, owner, conn)
    history = _read_history(bot_open_id, profile, prompt_time)
    reply = _verified_reply(conn, batch_id, history)
    if reply['conversation_id'] != conversation or reply['owner_id'] != owner:
        raise ValueError('reply_identity_mismatch')
    ack = _send_ack_notice(conn, batch_id, profile, bot_open_id, app_id,
                           owner_user_id, robot_code, prompt_time, enabled=send_ack)
    if ack != 'delivered':
        return {'status': 'pending', 'reason': 'ack_delivery_unknown',
                'batch': batch_id, 'pending_ordinals': list(reply['decisions'])}

    if handoff_reader is None:
        from review_handoff import verified_handoff
        handoff_reader = verified_handoff
    try:
        handoff = handoff_reader(conn, batch_id, profile, bot_open_id)
    except (ValueError, RuntimeError, OSError, json.JSONDecodeError, subprocess.TimeoutExpired):
        notice = _send_once(conn, batch_id, 'partial', PENDING_TEXT, profile,
                            bot_open_id, app_id, owner_user_id, robot_code, prompt_time)
        return {'status': 'pending', 'reason': 'handoff_verification_failed',
                'notice_status': notice, 'batch': batch_id,
                'pending_ordinals': list(reply['decisions'])}
    if handoff.get('batch') != batch_id or handoff.get('reply_message_id') != reply['reply_message_id']:
        raise ValueError('handoff_reply_mismatch')
    items = handoff.get('items')
    if (not isinstance(items, list) or len(items) != len(reply['decisions'])
            or [item.get('ordinal') for item in items] != list(range(1, len(items) + 1))):
        raise ValueError('handoff_item_identity_mismatch')

    results = []
    for item in items:
        ordinal = item['ordinal']
        if item.get('user_decision') != reply['decisions'].get(ordinal):
            raise ValueError('handoff_decision_mismatch')
        if item['user_decision'] == 'ignore':
            results.append(_apply_ignore(conn, batch_id, item, profile, bot_open_id, lock_path))
        elif item['user_decision'] == 'create':
            provider = create_plan_provider or (lambda current: source_grounded_plan(
                current, batch_id, reply['reply_message_id']))
            results.append(_apply_create(conn, batch_id, item, profile, bot_open_id,
                                         lock_path, provider, reply['reply_message_id']))
        else:
            results.append({'status': 'pending', 'ordinal': ordinal,
                            'reason': 'decision_not_supported'})

    operation_failed = any(result.get('status') == 'failed' for result in results)
    try:
        final_handoff = handoff_reader(conn, batch_id, profile, bot_open_id)
    except Exception:
        notice = _send_once(conn, batch_id, 'partial', PENDING_TEXT, profile,
                            bot_open_id, app_id, owner_user_id, robot_code, prompt_time)
        return {'status': 'failed' if notice == 'delivered' else 'pending',
                'reason': 'final_readback_failed', 'batch': batch_id, 'items': results,
                'pending_ordinals': list(reply['decisions'])}
    if final_handoff.get('batch') != batch_id or final_handoff.get('reply_message_id') != reply['reply_message_id']:
        raise ValueError('handoff_reply_mismatch')
    state, pending_ordinals = _closure_state(conn, final_handoff, profile)
    if state == 'all_ignored':
        conn.execute('INSERT INTO review_auto_terminal (batch_id,status) VALUES (?,?)',
                     (batch_id, state))
        return {'status': 'all_ignored', 'batch': batch_id, 'items': results,
                'pending_ordinals': []}
    notice_kind, notice_text = ('complete', COMPLETE_TEXT) if state == 'complete' else ('partial', PENDING_TEXT)
    notice = _send_once(conn, batch_id, notice_kind, notice_text, profile, bot_open_id,
                        app_id, owner_user_id, robot_code, prompt_time)
    if notice != 'delivered':
        return {'status': 'pending', 'reason': f'{notice_kind}_notice_delivery_unknown',
                'batch': batch_id, 'items': results, 'pending_ordinals': pending_ordinals}
    final_status = 'failed' if operation_failed else state
    if final_status == 'complete':
        conn.execute('INSERT INTO review_auto_terminal (batch_id,status) VALUES (?,?)',
                     (batch_id, final_status))
    return {'status': final_status, 'batch': batch_id, 'items': results,
            'pending_ordinals': pending_ordinals}
