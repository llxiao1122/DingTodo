"""Durable, bounded association of owner replies with reviewed source actions.

This module never creates tasks or changes the authoritative action ledger.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


def connect(path: str) -> sqlite3.Connection:
    db = Path(path)
    db.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(db, os.O_CREAT | os.O_RDWR, 0o600)
    os.close(fd)
    conn = sqlite3.connect(str(db), timeout=30, isolation_level=None)
    conn.execute('PRAGMA foreign_keys=ON')
    conn.execute('PRAGMA journal_mode=WAL')
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS batch (
          id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL, owner_id TEXT NOT NULL,
          prompt_message_id TEXT NOT NULL UNIQUE, prompt_time TEXT NOT NULL,
          state TEXT NOT NULL CHECK(state IN ('open','answered'))
        );
        CREATE TABLE IF NOT EXISTS item (
          batch_id TEXT NOT NULL REFERENCES batch(id), ordinal INTEGER NOT NULL,
          action_ids TEXT NOT NULL, PRIMARY KEY(batch_id, ordinal)
        );
        CREATE TABLE IF NOT EXISTS reply (
          message_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL REFERENCES batch(id),
          message_time TEXT NOT NULL, content_sha256 TEXT NOT NULL, content TEXT NOT NULL,
          decisions TEXT NOT NULL, source TEXT NOT NULL DEFAULT 'chat'
          CHECK(source IN ('chat','card'))
        );
        CREATE TABLE IF NOT EXISTS review_card_evidence (
          batch_id TEXT PRIMARY KEY REFERENCES batch(id),
          callback_id TEXT NOT NULL UNIQUE, template_id TEXT NOT NULL,
          owner_id TEXT NOT NULL, message_time TEXT NOT NULL,
          content_sha256 TEXT NOT NULL, action_ids TEXT NOT NULL,
          user_id_type TEXT NOT NULL,
          work_state TEXT NOT NULL DEFAULT 'legacy_unknown'
          CHECK(work_state IN ('legacy_unknown','pending','processing','dispatched','unknown'))
        );
        CREATE TABLE IF NOT EXISTS reply_candidate (
          message_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL REFERENCES batch(id),
          message_time TEXT NOT NULL, content_sha256 TEXT NOT NULL, content TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS review_clarification (
          batch_id TEXT PRIMARY KEY REFERENCES batch(id), prompt_text TEXT NOT NULL,
          status TEXT NOT NULL CHECK(status IN ('sending','unknown','delivered')),
          claimed_at TEXT NOT NULL, baseline_message_ids TEXT NOT NULL,
          bot_id TEXT NOT NULL, prompt_message_id TEXT, prompt_time TEXT
        );
        CREATE TABLE IF NOT EXISTS resolution (
          batch_id TEXT NOT NULL REFERENCES batch(id), ordinal INTEGER NOT NULL,
          action_id TEXT NOT NULL, decision TEXT NOT NULL, reply_message_id TEXT NOT NULL,
          request_json TEXT NOT NULL, status TEXT NOT NULL
          CHECK(status IN ('prepared','applying','applied')),
          PRIMARY KEY(batch_id, ordinal)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS one_open_batch ON batch(state) WHERE state='open';
        CREATE TABLE IF NOT EXISTS dispatch (
          id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL, owner_id TEXT NOT NULL,
          text TEXT NOT NULL, items TEXT NOT NULL, status TEXT NOT NULL
          CHECK(status IN ('prepared','sending','unknown','delivered')),
          prompt_message_id TEXT, prompt_time TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS one_pending_dispatch ON dispatch((1))
          WHERE status IN ('prepared','sending','unknown');
        CREATE TABLE IF NOT EXISTS review_card_delivery (
          batch_id TEXT PRIMARY KEY REFERENCES batch(id),
          template_id TEXT NOT NULL, template_version TEXT NOT NULL,
          status TEXT NOT NULL CHECK(status IN ('attempting','unknown','not_delivered','delivered')),
          attempted_at TEXT NOT NULL, updated_at TEXT NOT NULL, error_code TEXT
        );
    ''')
    columns = {row[1] for row in conn.execute('PRAGMA table_info(dispatch)')}
    if 'sending_at' not in columns:
        conn.execute('ALTER TABLE dispatch ADD COLUMN sending_at TEXT')
    if 'preview_hash' not in columns:
        conn.execute('ALTER TABLE dispatch ADD COLUMN preview_hash TEXT')
    resolution_columns = {row[1] for row in conn.execute('PRAGMA table_info(resolution)')}
    if 'external_task_id' not in resolution_columns:
        conn.execute('ALTER TABLE resolution ADD COLUMN external_task_id TEXT')
    reply_columns = {row[1] for row in conn.execute('PRAGMA table_info(reply)')}
    if 'source' not in reply_columns:
        conn.execute("ALTER TABLE reply ADD COLUMN source TEXT NOT NULL DEFAULT 'chat'")
    card_columns = {row[1] for row in conn.execute('PRAGMA table_info(review_card_evidence)')}
    if 'work_state' not in card_columns:
        conn.execute('BEGIN IMMEDIATE')
        try:
            conn.execute(
                "ALTER TABLE review_card_evidence ADD COLUMN work_state TEXT NOT NULL "
                "DEFAULT 'legacy_unknown' CHECK(work_state IN "
                "('legacy_unknown','pending','processing','dispatched','unknown'))"
            )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
    return conn


def register_batch(conn: sqlite3.Connection, batch_id: str, conversation_id: str,
                   owner_id: str, prompt_message_id: str, prompt_time: str,
                   items: dict[int, list[str]], *, enable_automation: bool = False) -> None:
    if not all(isinstance(v, str) and v.strip() for v in
               (batch_id, conversation_id, owner_id, prompt_message_id, prompt_time)):
        raise ValueError('missing_batch_identity')
    if not items or sorted(items) != list(range(1, len(items) + 1)):
        raise ValueError('invalid_item_ordinals')
    ids = [str(x) for group in items.values() for x in group]
    if not all(re.fullmatch(r'[1-9][0-9]{0,18}', x) for x in ids) or len(ids) != len(set(ids)):
        raise ValueError('invalid_or_duplicate_action_id')
    conn.execute('BEGIN IMMEDIATE')
    try:
        if enable_automation:
            conn.execute('CREATE TABLE IF NOT EXISTS review_auto_enabled '
                         '(batch_id TEXT PRIMARY KEY, enabled_at TEXT NOT NULL)')
        conn.execute('INSERT INTO batch VALUES (?,?,?,?,?,?)',
                     (batch_id, conversation_id, owner_id, prompt_message_id, prompt_time, 'open'))
        conn.executemany('INSERT INTO item VALUES (?,?,?)',
                         [(batch_id, n, json.dumps(items[n])) for n in sorted(items)])
        if enable_automation:
            conn.execute('INSERT INTO review_auto_enabled VALUES (?, ?)', (batch_id, prompt_time))
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def parse_reply(text: str, count: int, *, batch_id: str | None = None) -> dict[int, str]:
    """Parse only explicit numbered decisions; anything ambiguous needs review."""
    if not isinstance(text, str) or len(text) > 4000:
        raise ValueError('invalid_reply')
    if batch_id is not None:
        prefix = re.compile(r'^\s*复核编号\s*[:：]\s*([A-Za-z0-9_-]{1,64})\s*[;；,，]\s*')
        match = prefix.match(text)
        if match:
            if match.group(1) != batch_id:
                raise ValueError('clarification_batch_mismatch')
            text = text[match.end():]
        elif '复核编号' in text:
            raise ValueError('clarification_batch_mismatch')
    text = text.replace('，', ',').replace('；', ';').replace('。', '.')
    if count == 1:
        compact = re.sub(r'\s+', '', text).strip(',;.')
        if re.fullmatch(r'(?:请)?(?:创建|建立)(?:一条)?待办(?:提醒我|由我执行|执行人是我|分配给我)?(?:,?标注测试用)?', compact):
            return {1: 'create'}
        if re.fullmatch(r'(?:忽略|不创建待办|无需创建待办|不用创建待办)', compact):
            return {1: 'ignore'}
    matches = list(re.finditer(r'(?:^|[,;\s])([1-9][0-9]*)\s*[,.:、]\s*', text))
    if len(matches) != count:
        raise ValueError('incomplete_or_ambiguous_reply')
    result: dict[int, str] = {}
    for i, match in enumerate(matches):
        number = int(match.group(1))
        segment = text[match.end():matches[i + 1].start() if i + 1 < len(matches) else len(text)]
        segment = re.sub(r'\s+', '', segment).strip(',;.')
        ignore = bool(re.fullmatch(r'(?:忽略|不创建|无需创建)', segment))
        create = bool(re.fullmatch(r'(?:这条)?(?:创建待办|创建|建立待办|建待办)(?:,?执行人是我)?', segment))
        if number in result or ignore == create:
            raise ValueError('incomplete_or_ambiguous_reply')
        result[number] = 'ignore' if ignore else 'create'
    if sorted(result) != list(range(1, count + 1)):
        raise ValueError('incomplete_or_ambiguous_reply')
    return result


def ingest_reply(conn: sqlite3.Connection, batch_id: str, message: dict[str, Any]) -> str:
    """Persist exact-owner reply once, rejecting conflicts and wrong conversations."""
    conn.execute('BEGIN IMMEDIATE')
    try:
        batch = conn.execute('SELECT conversation_id,owner_id,prompt_time,state FROM batch WHERE id=?',
                             (batch_id,)).fetchone()
        if not batch:
            raise ValueError('unknown_batch')
        conversation, owner, prompt_time, state = batch
        msg_id, msg_time, text = (message.get('messageId'), message.get('createTime'), message.get('text'))
        if message.get('conversationId') != conversation or message.get('senderId') != owner:
            raise ValueError('unauthorized_reply')
        if not isinstance(msg_id, str) or not msg_id or not isinstance(msg_time, str) or msg_time <= prompt_time:
            raise ValueError('invalid_reply_identity_or_time')
        if not isinstance(text, str):
            raise ValueError('invalid_reply')
        digest = hashlib.sha256(text.encode('utf-8')).hexdigest()
        prior = conn.execute('SELECT batch_id,content_sha256 FROM reply WHERE message_id=?', (msg_id,)).fetchone()
        if prior:
            if prior != (batch_id, digest):
                raise ValueError('reply_identity_conflict')
            conn.commit()
            return 'duplicate'
        if state != 'open':
            raise ValueError('batch_already_answered')
        count = conn.execute('SELECT COUNT(*) FROM item WHERE batch_id=?', (batch_id,)).fetchone()[0]
        clarification = conn.execute(
            'SELECT status,prompt_time FROM review_clarification WHERE batch_id=?', (batch_id,)
        ).fetchone()
        if clarification:
            status, clarification_time = clarification
            if status != 'delivered' or not isinstance(clarification_time, str) or msg_time <= clarification_time:
                raise ValueError('reply_not_after_clarification')
            saved_candidate = conn.execute(
                'SELECT message_time,content_sha256,content FROM reply_candidate '
                'WHERE message_id=? AND batch_id=?', (msg_id, batch_id)
            ).fetchone()
            candidates_after_prompt = conn.execute(
                'SELECT message_id FROM reply_candidate WHERE batch_id=? AND message_time>?',
                (batch_id, clarification_time),
            ).fetchall()
            if (not saved_candidate or saved_candidate[0] != msg_time
                    or saved_candidate[1] != digest or saved_candidate[2] != text
                    or len(candidates_after_prompt) != 1
                    or candidates_after_prompt[0][0] != msg_id):
                raise ValueError('clarification_reply_not_unique_candidate')
            decisions = parse_reply(text, count, batch_id=batch_id)
        else:
            decisions = parse_reply(text, count)
        conn.execute('INSERT INTO reply '
                     '(message_id,batch_id,message_time,content_sha256,content,decisions,source) '
                     'VALUES (?,?,?,?,?,?,?)',
                     (msg_id, batch_id, msg_time, digest, text,
                      json.dumps(decisions, ensure_ascii=False), 'chat'))
        conn.execute("UPDATE batch SET state='answered' WHERE id=?", (batch_id,))
        conn.commit()
        return 'recorded'
    except BaseException:
        conn.rollback()
        raise


def poll_batch(conn: sqlite3.Connection, batch_id: str, payload: dict[str, Any]) -> str:
    """Process a fully read conversation window, never choosing between replies."""
    row = conn.execute('SELECT conversation_id,owner_id,prompt_time,state FROM batch WHERE id=?',
                       (batch_id,)).fetchone()
    if not row:
        raise ValueError('unknown_batch')
    conversation, owner, prompt_time, state = row
    if state == 'answered':
        return 'already_answered'
    if (not isinstance(payload, dict) or payload.get('complete') is not True
            or payload.get('partial') is not False or payload.get('hasMore') is not False
            or payload.get('failures') != [] or not isinstance(payload.get('messages'), list)):
        raise ValueError('incomplete_message_read')
    try:
        boundary = datetime.strptime(prompt_time, '%Y-%m-%d %H:%M:%S')
        candidates = []
        for message in payload['messages']:
            if not isinstance(message, dict):
                raise ValueError('invalid_message_row')
            if message.get('conversationId') != conversation or message.get('senderId') != owner:
                continue
            when = datetime.strptime(message.get('createTime', ''), '%Y-%m-%d %H:%M:%S')
            if when >= boundary:
                candidates.append(message)
    except (TypeError, ValueError) as exc:
        raise ValueError('invalid_message_time_or_row') from exc
    clarification = conn.execute(
        'SELECT prompt_text,status,bot_id,prompt_message_id,prompt_time '
        'FROM review_clarification WHERE batch_id=?', (batch_id,)
    ).fetchone()
    conn.execute('BEGIN IMMEDIATE')
    try:
        for candidate in candidates:
            msg_id, text = candidate.get('messageId'), candidate.get('text')
            if not isinstance(msg_id, str) or not msg_id or not isinstance(text, str) or len(text) > 4000:
                raise ValueError('invalid_reply_candidate')
            digest = hashlib.sha256(text.encode('utf-8')).hexdigest()
            prior = conn.execute('SELECT batch_id,content_sha256 FROM reply_candidate WHERE message_id=?',
                                 (msg_id,)).fetchone()
            if prior and prior != (batch_id, digest):
                raise ValueError('reply_identity_conflict')
            if not prior:
                conn.execute('INSERT INTO reply_candidate VALUES (?,?,?,?,?)',
                             (msg_id, batch_id, candidate['createTime'], digest, text))
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    if clarification:
        prompt_text, clarification_status, bot_id, clarification_id, clarification_time = clarification
        if clarification_status != 'delivered' or not clarification_id or not clarification_time:
            return 'clarification_delivery_unknown'
        prompt_matches = [message for message in payload['messages']
                          if isinstance(message, dict)
                          and message.get('conversationId') == conversation
                          and message.get('senderId') == bot_id
                          and message.get('messageId') == clarification_id
                          and message.get('createTime') == clarification_time
                          and isinstance(message.get('text'), str)
                          and re.sub(r'\s+', '', message['text']) == re.sub(r'\s+', '', prompt_text)]
        if len(prompt_matches) != 1:
            raise ValueError('clarification_evidence_mismatch')
        followups = [message for message in candidates
                     if message.get('createTime', '') > clarification_time]
        if not followups:
            return 'waiting_for_clarification'
        if len(followups) != 1:
            return 'multiple_replies_need_review'
        try:
            return ingest_reply(conn, batch_id, followups[0])
        except ValueError as exc:
            if str(exc) in ('incomplete_or_ambiguous_reply', 'clarification_batch_mismatch'):
                return 'reply_needs_review'
            raise
    if not candidates:
        return 'waiting'
    if len(candidates) != 1:
        return 'multiple_replies_need_review'
    if candidates[0]['createTime'] == prompt_time:
        return 'reply_needs_review'
    try:
        return ingest_reply(conn, batch_id, candidates[0])
    except ValueError as exc:
        if str(exc) == 'incomplete_or_ambiguous_reply':
            return 'reply_needs_review'
        raise


def prepare_dispatch(conn: sqlite3.Connection, batch_id: str, conversation_id: str,
                     owner_id: str, items: list[dict[str, Any]]) -> str:
    """Freeze exact REVIEW sources and message text before any remote write."""
    if not re.fullmatch(r'[A-Za-z0-9_-]{8,64}', batch_id):
        raise ValueError('invalid_batch_id')
    if not conversation_id or not owner_id or not 1 <= len(items) <= 5:
        raise ValueError('invalid_dispatch')
    safe = []
    seen = set()
    for item in items:
        if not isinstance(item, dict) or set(item) != {'action_id', 'source_date', 'source'}:
            raise ValueError('invalid_review_source')
        aid, day, source = item['action_id'], item['source_date'], item['source']
        if not isinstance(aid, str) or not re.fullmatch(r'[1-9][0-9]{0,18}', aid) or aid in seen:
            raise ValueError('invalid_or_duplicate_action_id')
        if not isinstance(day, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', day):
            raise ValueError('invalid_source_date')
        if not isinstance(source, dict) or not isinstance(source.get('text'), str):
            raise ValueError('source_text_missing')
        if source.get('text_truncated') is not False or len(source['text']) > 1600:
            raise ValueError('source_too_long')
        seen.add(aid)
        safe.append({'action_id': aid, 'source_date': day, 'source': source})
    lines = [f'复核编号：{batch_id}', '请逐条确认是否需要你执行。回复示例：1，忽略，2，创建，执行人是我。']
    for number, item in enumerate(safe, 1):
        source = item['source']
        text = source['text'].replace('\r', ' ').replace('\n', ' ')
        group = str(source.get('group_name') or '')[:128].replace('\n', ' ')
        when = str(source.get('message_time') or '')[:40]
        lines.append(f"{number}、来源：{group} {when}\n{text}")
    rendered = '\n\n'.join(lines)
    conn.execute('BEGIN IMMEDIATE')
    try:
        if conn.execute("SELECT 1 FROM batch WHERE state='open'").fetchone():
            raise ValueError('another_batch_is_open')
        conn.execute('INSERT INTO dispatch (id,conversation_id,owner_id,text,items,status) VALUES (?,?,?,?,?,?)',
                     (batch_id, conversation_id, owner_id, rendered,
                      json.dumps(safe, ensure_ascii=False, separators=(',', ':')), 'prepared'))
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return rendered


def prepare_from_reviews(conn: sqlite3.Connection, gateway: Any, conversation_id: str,
                         owner_id: str, selections: list[tuple[str, str]]) -> tuple[str, str]:
    """Resolve each selected action against the current authoritative REVIEW view."""
    if not selections or len(selections) > 5:
        raise ValueError('invalid_selection')
    items = []
    for day, action_id in selections:
        try:
            requested_day = date.fromisoformat(day)
        except (TypeError, ValueError) as exc:
            raise ValueError('invalid_source_date') from exc
        result = gateway.review_detail(action_id, requested_day)
        action = result.get('action') if isinstance(result, dict) else None
        if (not isinstance(result, dict) or result.get('ok') is not True
                or result.get('complete') is not True or result.get('found') is not True
                or not isinstance(action, dict) or action.get('action_id') != action_id
                or action.get('status') != 'REVIEW' or not isinstance(result.get('source'), dict)):
            raise ValueError('review_source_not_verified')
        items.append({'action_id': action_id, 'source_date': day, 'source': result['source']})
    batch_id = 'review_' + secrets.token_hex(16)
    return batch_id, prepare_dispatch(conn, batch_id, conversation_id, owner_id, items)


def claim_dispatch(conn: sqlite3.Connection, batch_id: str) -> str:
    """Mark before sending; a crash or uncertain response must never be replayed."""
    conn.execute('BEGIN IMMEDIATE')
    try:
        row = conn.execute('SELECT text,status FROM dispatch WHERE id=?', (batch_id,)).fetchone()
        if not row or row[1] != 'prepared':
            raise ValueError('dispatch_not_prepared')
        now = datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
        conn.execute("UPDATE dispatch SET status='sending',sending_at=? WHERE id=?", (now, batch_id))
        conn.commit()
        return row[0]
    except BaseException:
        conn.rollback()
        raise


def mark_dispatch_unknown(conn: sqlite3.Connection, batch_id: str) -> None:
    changed = conn.execute("UPDATE dispatch SET status='unknown' WHERE id=? AND status='sending'",
                           (batch_id,)).rowcount
    if changed != 1:
        raise ValueError('dispatch_not_sending')


def reconcile_dispatch(conn: sqlite3.Connection, batch_id: str, bot_id: str,
                       payload: dict[str, Any]) -> str:
    """Use a complete history window as delivery evidence; never resend."""
    row = conn.execute('SELECT conversation_id,owner_id,items,status,text FROM dispatch WHERE id=?',
                       (batch_id,)).fetchone()
    if not row or row[3] not in ('sending', 'unknown'):
        raise ValueError('dispatch_not_reconcilable')
    conversation, owner, frozen, _, expected_text = row
    if (not isinstance(payload, dict) or payload.get('complete') is not True
            or payload.get('partial') is not False or payload.get('hasMore') is not False
            or payload.get('failures') != [] or not isinstance(payload.get('messages'), list)):
        raise ValueError('incomplete_message_read')
    marker = '复核编号：' + batch_id
    found = [m for m in payload['messages'] if isinstance(m, dict)
             and m.get('conversationId') == conversation and m.get('senderId') == bot_id
             and isinstance(m.get('text'), str) and marker in m['text']
             and re.sub(r'\s+', '', m['text']) == re.sub(r'\s+', '', expected_text)]
    if len(found) != 1:
        return 'delivery_not_proven'
    msg = found[0]
    items = json.loads(frozen)
    existing = conn.execute('SELECT prompt_message_id,prompt_time FROM batch WHERE id=?',
                            (batch_id,)).fetchone()
    if existing:
        if existing != (msg.get('messageId'), msg.get('createTime')):
            raise ValueError('batch_delivery_conflict')
    else:
        register_batch(conn, batch_id, conversation, owner, msg.get('messageId'), msg.get('createTime'),
                       {i: [item['action_id']] for i, item in enumerate(items, 1)}, enable_automation=True)
    conn.execute("UPDATE dispatch SET status='delivered',prompt_message_id=?,prompt_time=? WHERE id=?",
                 (msg['messageId'], msg['createTime'], batch_id))
    return 'delivered'


def claim_review_card_delivery(conn: sqlite3.Connection, batch_id: str,
                               template_id: str, template_version: str,
                               attempted_at: str) -> tuple[bool, str]:
    """Claim the one permitted card attempt for an already delivered text batch."""
    if not all(isinstance(value, str) and value.strip() for value in
               (batch_id, template_id, template_version, attempted_at)):
        raise ValueError('review_card_delivery_identity_invalid')
    conn.execute('BEGIN IMMEDIATE')
    try:
        batch = conn.execute(
            "SELECT b.state,d.status FROM batch AS b JOIN dispatch AS d ON d.id=b.id "
            "WHERE b.id=?", (batch_id,),
        ).fetchone()
        if batch != ('open', 'delivered'):
            raise ValueError('review_card_text_anchor_unverified')
        existing = conn.execute(
            'SELECT status FROM review_card_delivery WHERE batch_id=?', (batch_id,),
        ).fetchone()
        if existing:
            conn.commit()
            return False, existing[0]
        conn.execute(
            'INSERT INTO review_card_delivery '
            '(batch_id,template_id,template_version,status,attempted_at,updated_at,error_code) '
            "VALUES (?,?,?,'attempting',?,?,NULL)",
            (batch_id, template_id, template_version, attempted_at, attempted_at),
        )
        conn.commit()
        return True, 'attempting'
    except BaseException:
        conn.rollback()
        raise


def finish_review_card_delivery(conn: sqlite3.Connection, batch_id: str, status: str,
                                updated_at: str, error_code: str | None = None) -> None:
    """Finish a claimed card attempt with a bounded, non-sensitive status."""
    if status not in ('unknown', 'not_delivered', 'delivered'):
        raise ValueError('review_card_delivery_status_invalid')
    if not isinstance(updated_at, str) or not updated_at.strip():
        raise ValueError('review_card_delivery_time_invalid')
    if error_code is not None and error_code not in (
            'delivery_exception', 'delivery_response_unverified'):
        raise ValueError('review_card_delivery_error_invalid')
    changed = conn.execute(
        "UPDATE review_card_delivery SET status=?,updated_at=?,error_code=? "
        "WHERE batch_id=? AND status='attempting'",
        (status, updated_at, error_code, batch_id),
    ).rowcount
    if changed != 1:
        raise ValueError('review_card_delivery_not_claimed')
