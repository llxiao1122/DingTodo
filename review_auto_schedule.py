"""Discover, queue, and safely ask about new Cifra REVIEW actions.

All network boundaries are injected so scheduling can be exercised with an
isolated inbox database. A dispatch is claimed before calling the sender. Once
claimed, later ticks only reconcile delivery from a complete history read.
"""
from __future__ import annotations

import json
import re
import secrets
import sqlite3
from datetime import date, datetime
from typing import Any, Callable
from zoneinfo import ZoneInfo

from review_inbox import claim_dispatch, reconcile_dispatch

CHINA_TZ = ZoneInfo('Asia/Shanghai')
SCHEDULE_KEY = 1
MAX_BATCH_ITEMS = 5
MAX_DISCOVERY_DAYS_PER_TICK = 7
MAX_RETRY_ACTIONS_PER_TICK = 4


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute('''
        CREATE TABLE IF NOT EXISTS review_auto_schedule (
          schedule_key INTEGER PRIMARY KEY CHECK(schedule_key=1),
          enabled_at TEXT NOT NULL,
          scan_day TEXT NOT NULL
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS review_auto_scheduled_dispatch (
          batch_id TEXT PRIMARY KEY,
          registered_at TEXT NOT NULL
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS review_auto_quarantined_action (
          action_id TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          reason TEXT NOT NULL,
          recorded_at TEXT NOT NULL,
          PRIMARY KEY(action_id,updated_at)
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS review_auto_retry_action (
          action_id TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          source_day TEXT NOT NULL,
          queued_at TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0,
          last_attempt_at TEXT,
          status TEXT NOT NULL DEFAULT 'pending',
          last_error TEXT,
          resolved_at TEXT,
          dispatch_id TEXT,
          PRIMARY KEY(action_id,updated_at)
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS review_auto_retry_pending_order '
                 'ON review_auto_retry_action(status,attempts,source_day,action_id)')
    columns = {row[1] for row in conn.execute('PRAGMA table_info(review_auto_schedule)')}
    if 'scan_day' not in columns:
        conn.execute('ALTER TABLE review_auto_schedule ADD COLUMN scan_day TEXT')
        conn.execute("UPDATE review_auto_schedule SET scan_day=substr(enabled_at,1,10) "
                     'WHERE scan_day IS NULL')


def _local_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip().replace('Z', '+00:00'))
        except ValueError as exc:
            raise ValueError('invalid_schedule_or_action_time') from exc
    else:
        raise ValueError('invalid_schedule_or_action_time')
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(CHINA_TZ).replace(tzinfo=None)
    return parsed


def _timestamp(value: datetime | str) -> str:
    return _local_datetime(value).isoformat(sep=' ', timespec='microseconds')


def enable(conn: sqlite3.Connection, enabled_at: datetime | str) -> None:
    """Enable once at an explicit boundary; an existing boundary is immutable."""
    ensure_schema(conn)
    boundary = _timestamp(enabled_at)
    conn.execute('BEGIN IMMEDIATE')
    try:
        current = conn.execute('SELECT enabled_at FROM review_auto_schedule '
                               'WHERE schedule_key=?', (SCHEDULE_KEY,)).fetchone()
        if current and current[0] != boundary:
            raise ValueError('review_schedule_already_enabled')
        if not current:
            conn.execute('INSERT INTO review_auto_schedule (schedule_key,enabled_at,scan_day) '
                         'VALUES (?,?,?)',
                         (SCHEDULE_KEY, boundary, boundary[:10]))
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute('SELECT 1 FROM sqlite_master WHERE type=? AND name=?',
                        ('table', table)).fetchone() is not None


def _active_answered_batch(conn: sqlite3.Connection) -> str | None:
    if not _table_exists(conn, 'review_auto_enabled'):
        return None
    if _table_exists(conn, 'review_auto_terminal'):
        row = conn.execute('''
            SELECT b.id
            FROM batch AS b
            JOIN review_auto_enabled AS enabled ON enabled.batch_id=b.id
            LEFT JOIN review_auto_terminal AS terminal ON terminal.batch_id=b.id
            WHERE b.state='answered' AND terminal.batch_id IS NULL
            ORDER BY b.prompt_time,b.id LIMIT 1
        ''').fetchone()
    else:
        row = conn.execute('''
            SELECT b.id
            FROM batch AS b
            JOIN review_auto_enabled AS enabled ON enabled.batch_id=b.id
            WHERE b.state='answered'
            ORDER BY b.prompt_time,b.id LIMIT 1
        ''').fetchone()
    return row[0] if row else None


def _seen_action_ids(conn: sqlite3.Connection) -> set[str]:
    seen: set[str] = set()
    for (encoded,) in conn.execute('SELECT action_ids FROM item').fetchall():
        try:
            action_ids = json.loads(encoded)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError('stored_action_ids_invalid') from exc
        if not isinstance(action_ids, list) or not all(isinstance(value, str) for value in action_ids):
            raise ValueError('stored_action_ids_invalid')
        seen.update(action_ids)
    for (encoded,) in conn.execute('SELECT items FROM dispatch').fetchall():
        try:
            items = json.loads(encoded)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError('stored_dispatch_items_invalid') from exc
        if not isinstance(items, list):
            raise ValueError('stored_dispatch_items_invalid')
        for item in items:
            if not isinstance(item, dict) or not isinstance(item.get('action_id'), str):
                raise ValueError('stored_dispatch_items_invalid')
            seen.add(item['action_id'])
    return seen


def _complete_reviews(result: Any) -> list[dict[str, Any]]:
    if (not isinstance(result, dict) or result.get('ok') is not True
            or result.get('complete') is not True or not isinstance(result.get('actions'), list)):
        raise ValueError('review_list_incomplete')
    if not all(isinstance(action, dict) for action in result['actions']):
        raise ValueError('review_action_invalid')
    return result['actions']


def _eligible_actions(actions: list[dict[str, Any]], enabled_at: str,
                      requested_day: date, seen_ids: set[str],
                      quarantined: set[tuple[str, str]]) -> list[dict[str, Any]]:
    activation = _local_datetime(enabled_at)
    candidates: dict[str, tuple[datetime, dict[str, Any]]] = {}
    for action in actions:
        action_id = action.get('action_id')
        if (not isinstance(action_id, str) or not re.fullmatch(r'[1-9][0-9]{0,18}', action_id)
                or action.get('status') != 'REVIEW'):
            raise ValueError('review_action_invalid')
        updated_at = _local_datetime(action.get('updated_at'))
        if updated_at.date() != requested_day:
            raise ValueError('review_action_out_of_scope')
        if (action_id in seen_ids or updated_at <= activation
                or (action_id, str(action.get('updated_at'))) in quarantined):
            continue
        if not isinstance(action.get('conversation_id'), str) or not action['conversation_id'].strip():
            raise ValueError('review_action_identity_invalid')
        if not isinstance(action.get('message_id'), str) or not action['message_id'].strip():
            raise ValueError('review_action_identity_invalid')
        previous = candidates.get(action_id)
        if previous is not None and previous[1] != action:
            raise ValueError('review_action_conflict')
        candidates[action_id] = (updated_at, action)
    return [row for _, row in sorted(candidates.values(), key=lambda pair: (pair[0], pair[1]['action_id']))]


def _verified_source(gateway: Any, action: dict[str, Any], requested_day: date) -> dict[str, Any]:
    detail = gateway.review_detail(action['action_id'], requested_day)
    detailed_action = detail.get('action') if isinstance(detail, dict) else None
    source = detail.get('source') if isinstance(detail, dict) else None
    source_text = source.get('text') if isinstance(source, dict) else None
    if (isinstance(source, dict) and (source.get('text_truncated') is True
                                      or (isinstance(source_text, str) and len(source_text) > 1600))):
        raise ValueError('review_source_too_long')
    if not isinstance(detail, dict) or detail.get('complete') is not True:
        raise ValueError('review_source_read_incomplete')
    if (detail.get('ok') is not True or detail.get('found') is not True
            or not isinstance(detailed_action, dict) or detailed_action.get('status') != 'REVIEW'
            or detailed_action.get('action_id') != action['action_id']
            or detailed_action.get('updated_at') != action['updated_at']
            or detailed_action.get('conversation_id') != action['conversation_id']
            or detailed_action.get('message_id') != action['message_id']
            or not isinstance(source, dict) or source.get('text_truncated') is not False
            or not isinstance(source.get('text'), str)):
        raise ValueError('review_source_unverified')
    return {'action_id': action['action_id'], 'source_date': requested_day.isoformat(),
            'source': source}


def _pending_dispatch(conn: sqlite3.Connection) -> tuple[str, str] | None:
    row = conn.execute("SELECT id,status FROM dispatch "
                       "WHERE status IN ('prepared','sending','unknown') ORDER BY rowid LIMIT 1").fetchone()
    return (row[0], row[1]) if row else None


def _fresh_history(payload: Any, sending_at: str) -> dict[str, Any]:
    if (not isinstance(payload, dict) or payload.get('complete') is not True
            or payload.get('partial') is not False or payload.get('hasMore') is not False
            or payload.get('failures') != [] or not isinstance(payload.get('messages'), list)):
        raise ValueError('incomplete_message_read')
    fresh: list[dict[str, Any]] = []
    for message in payload['messages']:
        if not isinstance(message, dict):
            raise ValueError('invalid_message_row')
        created_at = message.get('createTime')
        if not isinstance(created_at, str):
            continue
        try:
            datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            continue
        if created_at >= sending_at:
            fresh.append(message)
    return {**payload, 'messages': fresh}


def _reconcile(conn: sqlite3.Connection, batch_id: str, bot_open_id: str,
               history_reader: Callable[[str, str], dict[str, Any]] | None) -> str:
    if history_reader is None:
        return 'delivery_unknown'
    row = conn.execute('SELECT sending_at FROM dispatch WHERE id=?', (batch_id,)).fetchone()
    if not row or not isinstance(row[0], str):
        return 'delivery_unknown'
    try:
        payload = history_reader(batch_id, row[0])
        fresh = _fresh_history(payload, row[0])
        result = reconcile_dispatch(conn, batch_id, bot_open_id, fresh)
    except (ValueError, RuntimeError, OSError, sqlite3.Error):
        return 'delivery_unknown'
    return 'delivered' if result == 'delivered' else 'delivery_unknown'


def _mark_unknown_if_claimed(conn: sqlite3.Connection, batch_id: str) -> None:
    conn.execute("UPDATE dispatch SET status='unknown' WHERE id=? AND status='sending'", (batch_id,))


def _card_delivery_outcome(batch_id: str,
                           card_sender: Callable[[str], dict[str, Any]]) -> dict[str, str]:
    try:
        card = card_sender(batch_id)
    except Exception:
        card = {'status': 'delivery_unknown', 'reason': 'sender_exception'}
    allowed_statuses = {'delivered', 'not_delivered', 'delivery_unknown', 'not_attempted'}
    allowed_reasons = {'configuration_missing', 'identity_unmapped', 'text_anchor_unverified',
                       'already_attempted', 'sender_exception', 'delivery_exception',
                       'delivery_response_unverified', 'preflight_failed',
                       'template_mapping_invalid'}
    if not isinstance(card, dict) or card.get('status') not in allowed_statuses:
        card = {'status': 'delivery_unknown', 'reason': 'delivery_response_unverified'}
    result = {'status': card['status']}
    if card.get('reason') in allowed_reasons:
        result['reason'] = card['reason']
    return result


def _dispatch_result(batch_id: str, delivery: str,
                     card_sender: Callable[[str], dict[str, Any]] | None = None) -> dict[str, Any]:
    result = {'status': 'dispatch_delivered' if delivery == 'delivered'
              else 'dispatch_delivery_unknown', 'batch': batch_id}
    if delivery == 'delivered' and card_sender is not None:
        result['card_delivery'] = _card_delivery_outcome(batch_id, card_sender)
    return result


def _attempt_auto_dispatch(conn: sqlite3.Connection, batch_id: str, bot_open_id: str,
                           preflight: Callable[[str], Any] | None,
                           sender: Callable[[str, Any, str], Any] | None,
                           history_reader: Callable[[str, str], dict[str, Any]] | None,
                           card_sender: Callable[[str], dict[str, Any]] | None = None) -> dict[str, Any]:
    if preflight is None or sender is None or history_reader is None:
        return {'status': 'queued', 'batch': batch_id}
    try:
        context = preflight(batch_id)
    except Exception:
        return {'status': 'dispatch_preflight_failed', 'batch': batch_id}
    try:
        text = claim_dispatch(conn, batch_id)
    except ValueError:
        current = conn.execute('SELECT status FROM dispatch WHERE id=?', (batch_id,)).fetchone()
        if current and current[0] in ('sending', 'unknown'):
            delivery = _reconcile(conn, batch_id, bot_open_id, history_reader)
            return _dispatch_result(batch_id, delivery, card_sender)
        return {'status': 'dispatch_claim_lost', 'batch': batch_id}
    try:
        sender(batch_id, context, text)
    except BaseException as exc:
        _mark_unknown_if_claimed(conn, batch_id)
        if not isinstance(exc, Exception):
            raise
        delivery = _reconcile(conn, batch_id, bot_open_id, history_reader)
        return _dispatch_result(batch_id, delivery, card_sender)
    _mark_unknown_if_claimed(conn, batch_id)
    delivery = _reconcile(conn, batch_id, bot_open_id, history_reader)
    return _dispatch_result(batch_id, delivery, card_sender)


def _queue(conn: sqlite3.Connection, batch_id: str, conversation_id: str,
           owner_id: str, items: list[dict[str, Any]], registered_at: str,
           retry_keys: list[tuple[str, str]]) -> str:
    if not re.fullmatch(r'[A-Za-z0-9_-]{8,64}', batch_id):
        raise ValueError('invalid_batch_id')
    if not conversation_id or not owner_id or not 1 <= len(items) <= MAX_BATCH_ITEMS:
        raise ValueError('invalid_dispatch')
    safe: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        if not isinstance(item, dict) or set(item) != {'action_id', 'source_date', 'source'}:
            raise ValueError('invalid_review_source')
        action_id, source_day, source = item['action_id'], item['source_date'], item['source']
        if (not isinstance(action_id, str) or not re.fullmatch(r'[1-9][0-9]{0,18}', action_id)
                or action_id in seen):
            raise ValueError('invalid_or_duplicate_action_id')
        if not isinstance(source_day, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', source_day):
            raise ValueError('invalid_source_date')
        if not isinstance(source, dict) or not isinstance(source.get('text'), str):
            raise ValueError('source_text_missing')
        if source.get('text_truncated') is not False or len(source['text']) > 1600:
            raise ValueError('source_too_long')
        seen.add(action_id)
        safe.append({'action_id': action_id, 'source_date': source_day, 'source': source})
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
        # A second scheduler may have verified the same source concurrently.
        # Serialize the final dedupe check with dispatch creation.
        if seen.intersection(_seen_action_ids(conn)):
            raise ValueError('duplicate_action_race_lost')
        for action_id, updated_at in retry_keys:
            row = conn.execute("SELECT status FROM review_auto_retry_action "
                               'WHERE action_id=? AND updated_at=?',
                               (action_id, updated_at)).fetchone()
            if not row or row[0] != 'pending':
                raise ValueError('retry_claim_lost')
        if conn.execute("SELECT 1 FROM batch WHERE state='open'").fetchone():
            raise ValueError('another_batch_is_open')
        conn.execute('INSERT INTO review_auto_scheduled_dispatch VALUES (?,?)',
                     (batch_id, registered_at))
        conn.execute('INSERT INTO dispatch '
                     '(id,conversation_id,owner_id,text,items,status) '
                     'VALUES (?,?,?,?,?,?)',
                     (batch_id, conversation_id, owner_id, rendered,
                      json.dumps(safe, ensure_ascii=False, separators=(',', ':')), 'prepared'))
        conn.executemany("UPDATE review_auto_retry_action "
                         "SET status='queued',resolved_at=?,dispatch_id=?,last_error=NULL "
                         "WHERE action_id=? AND updated_at=? AND status='pending'",
                         [(registered_at, batch_id, action_id, updated_at)
                          for action_id, updated_at in retry_keys])
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return batch_id


def _advance_scan_day(conn: sqlite3.Connection, current_day: date) -> bool:
    next_day = date.fromordinal(current_day.toordinal() + 1).isoformat()
    changed = conn.execute('UPDATE review_auto_schedule SET scan_day=? '
                           'WHERE schedule_key=? AND scan_day=?',
                           (next_day, SCHEDULE_KEY, current_day.isoformat())).rowcount
    return changed == 1


def _quarantined_versions(conn: sqlite3.Connection) -> set[tuple[str, str]]:
    return set(conn.execute('SELECT action_id,updated_at FROM review_auto_quarantined_action').fetchall())


def _quarantine(conn: sqlite3.Connection, action: dict[str, Any], reason: str, recorded_at: str) -> None:
    conn.execute('INSERT OR IGNORE INTO review_auto_quarantined_action '
                 '(action_id,updated_at,reason,recorded_at) VALUES (?,?,?,?)',
                 (action['action_id'], str(action['updated_at']), reason, recorded_at))


def _enqueue_retry_actions(conn: sqlite3.Connection, actions: list[dict[str, Any]],
                           source_day: date, queued_at: str) -> None:
    """Persist discovered versions before any source detail reads are attempted."""
    conn.execute('BEGIN IMMEDIATE')
    try:
        seen = _seen_action_ids(conn)
        for action in actions:
            action_id = action['action_id']
            updated_at = str(action['updated_at'])
            if action_id in seen:
                continue
            conn.execute("UPDATE review_auto_retry_action SET status='stale', "
                         "last_error='review_action_version_changed',resolved_at=? "
                         "WHERE action_id=? AND updated_at<>? AND status='pending'",
                         (queued_at, action_id, updated_at))
            conn.execute("INSERT INTO review_auto_retry_action "
                         '(action_id,updated_at,source_day,queued_at) VALUES (?,?,?,?) '
                         "ON CONFLICT(action_id,updated_at) DO UPDATE SET "
                         "source_day=excluded.source_day,queued_at=excluded.queued_at,attempts=0, "
                         "last_attempt_at=NULL,status='pending',last_error='review_action_reappeared', "
                         'resolved_at=NULL,dispatch_id=NULL '
                         "WHERE review_auto_retry_action.status='stale'",
                         (action_id, updated_at, source_day.isoformat(), queued_at))
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def _retry_attempted(conn: sqlite3.Connection, action_id: str, updated_at: str,
                     attempted_at: str, error: str | None = None) -> None:
    conn.execute("UPDATE review_auto_retry_action SET attempts=attempts+1, "
                 'last_attempt_at=?,last_error=? '
                 "WHERE action_id=? AND updated_at=? AND status='pending'",
                 (attempted_at, error, action_id, updated_at))


def _retry_state(conn: sqlite3.Connection, action_id: str, updated_at: str,
                 status: str, reason: str, resolved_at: str) -> None:
    conn.execute('BEGIN IMMEDIATE')
    try:
        conn.execute('UPDATE review_auto_retry_action SET status=?,last_error=?,resolved_at=? '
                     "WHERE action_id=? AND updated_at=? AND status='pending'",
                     (status, reason, resolved_at, action_id, updated_at))
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def _retry_versions(conn: sqlite3.Connection, *, before_day: date | None,
                    limit: int) -> list[tuple[str, str, str]]:
    sql = "SELECT action_id,updated_at,source_day FROM review_auto_retry_action " \
          "WHERE status='pending'"
    parameters: list[Any] = []
    if before_day is not None:
        sql += ' AND source_day<?'
        parameters.append(before_day.isoformat())
    sql += ' ORDER BY attempts,COALESCE(last_attempt_at,\'\'),source_day,action_id,updated_at LIMIT ?'
    parameters.append(limit)
    return conn.execute(sql, parameters).fetchall()


def _retry_source_actions(conn: sqlite3.Connection, gateway: Any,
                          versions: list[tuple[str, str, str]], enabled_at: str,
                          seen: set[str], quarantined: set[tuple[str, str]],
                          checked_at: str) -> tuple[list[dict[str, Any]], list[tuple[str, str]],
                                                    set[tuple[str, str]], list[str]]:
    """Re-read complete day listings before retrying each frozen action version."""
    verified: list[dict[str, Any]] = []
    verified_keys: list[tuple[str, str]] = []
    attempted: set[tuple[str, str]] = set()
    failed_ids: list[str] = []
    by_day: dict[str, list[tuple[str, str]]] = {}
    for action_id, updated_at, source_day in versions:
        by_day.setdefault(source_day, []).append((action_id, updated_at))

    for source_day, keys in by_day.items():
        day = date.fromisoformat(source_day)
        try:
            actions = _complete_reviews(gateway.list_reviews(day))
        except Exception:
            for action_id, updated_at in keys:
                attempted.add((action_id, updated_at))
                _retry_attempted(conn, action_id, updated_at, checked_at,
                                 'review_retry_listing_incomplete')
                failed_ids.append(action_id)
            continue

        for action_id, updated_at in keys:
            key = (action_id, updated_at)
            attempted.add(key)
            matches = [row for row in actions if isinstance(row, dict)
                       and row.get('action_id') == action_id]
            exact = [row for row in matches if row.get('updated_at') == updated_at
                     and row.get('status') == 'REVIEW']
            if not exact:
                reason = ('review_action_version_changed' if matches
                          else 'review_action_not_in_source_day')
                _retry_state(conn, action_id, updated_at, 'stale', reason, checked_at)
                continue
            try:
                candidates = _eligible_actions(exact, enabled_at, day, seen, quarantined)
                if not candidates:
                    _retry_state(conn, action_id, updated_at, 'stale',
                                 'review_action_no_longer_eligible', checked_at)
                    continue
                item = _verified_source(gateway, candidates[0], day)
            except ValueError as exc:
                reason = str(exc)
                if reason == 'review_source_too_long':
                    matching_action = exact[0]
                    _quarantine(conn, matching_action, reason, checked_at)
                    quarantined.add(key)
                    _retry_state(conn, action_id, updated_at, 'quarantined', reason, checked_at)
                else:
                    _retry_attempted(conn, action_id, updated_at, checked_at, reason)
                    failed_ids.append(action_id)
                continue
            except Exception:
                _retry_attempted(conn, action_id, updated_at, checked_at,
                                 'review_source_read_failed')
                failed_ids.append(action_id)
                continue
            verified.append(item)
            verified_keys.append(key)

    return verified, verified_keys, attempted, failed_ids


def run_tick(conn: sqlite3.Connection, gateway: Any, *, today: date,
             conversation_id: str, owner_id: str, bot_open_id: str,
             now: datetime | str | None = None,
             batch_id_factory: Callable[[], str] | None = None,
             preflight: Callable[[str], Any] | None = None,
             sender: Callable[[str, Any, str], Any] | None = None,
             history_reader: Callable[[str, str], dict[str, Any]] | None = None,
             card_sender: Callable[[str], dict[str, Any]] | None = None,
             advance_batch: Callable[[str], dict[str, Any]] | None = None) -> dict[str, Any]:
    """Advance one bounded schedule tick using injected read/write adapters.

    ``preflight`` must bind the current Cifra bot, owner, conversation, and all
    frozen source rows. ``sender`` is called only after an atomic dispatch claim.
    ``history_reader`` must return a complete bounded read of that same DM.
    """
    ensure_schema(conn)
    enabled = conn.execute('SELECT enabled_at,scan_day FROM review_auto_schedule WHERE schedule_key=?',
                           (SCHEDULE_KEY,)).fetchone()
    if not enabled:
        return {'status': 'scheduler_not_enabled'}

    pending = _pending_dispatch(conn)
    if pending:
        batch_id, dispatch_status = pending
        auto = conn.execute('SELECT 1 FROM review_auto_scheduled_dispatch WHERE batch_id=?',
                            (batch_id,)).fetchone()
        if dispatch_status == 'prepared':
            if not auto:
                return {'status': 'manual_dispatch_pending', 'batch': batch_id}
            return _attempt_auto_dispatch(conn, batch_id, bot_open_id, preflight,
                                          sender, history_reader, card_sender)
        auto = conn.execute('SELECT 1 FROM review_auto_scheduled_dispatch WHERE batch_id=?',
                            (batch_id,)).fetchone()
        delivery = _reconcile(conn, batch_id, bot_open_id, history_reader)
        return _dispatch_result(batch_id, delivery, card_sender if auto else None)

    open_batch = conn.execute("SELECT id FROM batch WHERE state='open' ORDER BY prompt_time,id LIMIT 1").fetchone()
    if open_batch:
        batch_id = open_batch[0]
        card_delivery = None
        auto = conn.execute('SELECT 1 FROM review_auto_scheduled_dispatch WHERE batch_id=?',
                            (batch_id,)).fetchone()
        delivered = conn.execute('SELECT 1 FROM dispatch WHERE id=? AND status=?',
                                  (batch_id, 'delivered')).fetchone()
        if auto and delivered and card_sender is not None:
            card_delivery = _card_delivery_outcome(batch_id, card_sender)
        if advance_batch is not None:
            try:
                outcome = advance_batch(batch_id)
            except Exception:
                result = {'status': 'open_batch_advance_failed', 'batch': batch_id}
            else:
                result = {'status': 'open_batch_advanced', 'batch': batch_id, 'result': outcome}
        else:
            result = {'status': 'open_batch_pending', 'batch': batch_id}
        if card_delivery is not None:
            result['card_delivery'] = card_delivery
        return result

    answered_batch = _active_answered_batch(conn)
    if answered_batch:
        if advance_batch is not None:
            try:
                outcome = advance_batch(answered_batch)
            except Exception:
                return {'status': 'answered_batch_advance_failed', 'batch': answered_batch}
            return {'status': 'answered_batch_advanced', 'batch': answered_batch, 'result': outcome}
        return {'status': 'answered_batch_pending', 'batch': answered_batch}

    try:
        scan_day = date.fromisoformat(enabled[1])
    except (TypeError, ValueError):
        return {'status': 'review_scan_cursor_invalid'}
    seen = _seen_action_ids(conn)
    quarantined = _quarantined_versions(conn)
    registered_at = _timestamp(now or datetime.now(CHINA_TZ))
    retry_versions = _retry_versions(conn, before_day=scan_day,
                                     limit=MAX_RETRY_ACTIONS_PER_TICK)
    retry_items, retry_keys, attempted_retry_keys, retry_failures = _retry_source_actions(
        conn, gateway, retry_versions, enabled[0], seen, quarantined, registered_at)
    rejected_action_ids: list[str] = []
    retry_action_ids = list(retry_failures)

    def queue_verified(items: list[dict[str, Any]], keys: list[tuple[str, str]]) -> dict[str, Any]:
        unique_items: list[dict[str, Any]] = []
        unique_keys: list[tuple[str, str]] = []
        item_ids: set[str] = set()
        for item, key in zip(items, keys):
            status = conn.execute('SELECT status FROM review_auto_retry_action '
                                  'WHERE action_id=? AND updated_at=?', key).fetchone()
            if (not status or status[0] != 'pending' or item['action_id'] in item_ids):
                continue
            unique_items.append(item)
            unique_keys.append(key)
            item_ids.add(item['action_id'])
            if len(unique_items) == MAX_BATCH_ITEMS:
                break
        if not unique_items:
            return {'status': 'dispatch_queue_race_lost'}
        batch_id = (batch_id_factory or (lambda: 'auto_review_' + secrets.token_hex(12)))()
        try:
            _queue(conn, batch_id, conversation_id, owner_id, unique_items,
                   registered_at, unique_keys)
        except sqlite3.IntegrityError:
            return {'status': 'dispatch_queue_race_lost'}
        except ValueError as exc:
            if str(exc) in ('retry_claim_lost', 'duplicate_action_race_lost'):
                return {'status': 'dispatch_queue_race_lost'}
            return {'status': 'dispatch_queue_failed'}
        result = _attempt_auto_dispatch(conn, batch_id, bot_open_id, preflight,
                                        sender, history_reader, card_sender)
        if rejected_action_ids:
            result['rejected_action_ids'] = list(dict.fromkeys(rejected_action_ids))
        if retry_action_ids:
            result['retry_action_ids'] = list(dict.fromkeys(retry_action_ids))
        return result

    days_scanned = 0
    scan_attempts = 0
    quarantine_day: str | None = None
    today_processed = False
    while scan_day <= today and days_scanned < MAX_DISCOVERY_DAYS_PER_TICK:
        try:
            listing = _complete_reviews(gateway.list_reviews(scan_day))
            actions = _eligible_actions(listing, enabled[0], scan_day, seen, quarantined)
        except (ValueError, TypeError):
            return {'status': 'review_discovery_incomplete', 'day': scan_day.isoformat()}
        if actions:
            try:
                _enqueue_retry_actions(conn, actions, scan_day, registered_at)
            except sqlite3.Error:
                return {'status': 'review_retry_queue_failed', 'day': scan_day.isoformat()}

            scan_items: list[dict[str, Any]] = []
            scan_keys: list[tuple[str, str]] = []
            for action in actions:
                if scan_attempts == MAX_BATCH_ITEMS:
                    break
                key = (action['action_id'], str(action['updated_at']))
                if key in attempted_retry_keys:
                    continue
                row = conn.execute('SELECT status FROM review_auto_retry_action '
                                   'WHERE action_id=? AND updated_at=?', key).fetchone()
                if not row or row[0] != 'pending':
                    continue
                attempted_retry_keys.add(key)
                scan_attempts += 1
                try:
                    scan_items.append(_verified_source(gateway, action, scan_day))
                    scan_keys.append(key)
                except ValueError as exc:
                    reason = str(exc)
                    if reason == 'review_source_too_long':
                        _quarantine(conn, action, reason, registered_at)
                        quarantined.add(key)
                        _retry_state(conn, key[0], key[1], 'quarantined', reason, registered_at)
                        rejected_action_ids.append(action['action_id'])
                        quarantine_day = quarantine_day or scan_day.isoformat()
                    else:
                        _retry_attempted(conn, key[0], key[1], registered_at, reason)
                        retry_action_ids.append(action['action_id'])
                except Exception:
                    _retry_attempted(conn, key[0], key[1], registered_at,
                                     'review_source_read_failed')
                    retry_action_ids.append(action['action_id'])

            if scan_items:
                pending_retries: list[tuple[dict[str, Any], tuple[str, str]]] = []
                scan_action_ids = {item['action_id'] for item in scan_items}
                for item, key in zip(retry_items, retry_keys):
                    row = conn.execute('SELECT status FROM review_auto_retry_action '
                                       'WHERE action_id=? AND updated_at=?', key).fetchone()
                    if (row and row[0] == 'pending' and item['action_id'] not in scan_action_ids):
                        pending_retries.append((item, key))
                retry_slots = min(MAX_BATCH_ITEMS - 1, len(pending_retries))
                selected_scan_count = min(len(scan_items), MAX_BATCH_ITEMS - retry_slots)
                combined_items = scan_items[:selected_scan_count]
                combined_keys = scan_keys[:selected_scan_count]
                for item, key in pending_retries[:retry_slots]:
                    combined_items.append(item)
                    combined_keys.append(key)
                return queue_verified(combined_items, combined_keys)

        if scan_day == today:
            today_processed = True
            break
        if not _advance_scan_day(conn, scan_day):
            return {'status': 'review_scan_cursor_raced', 'day': scan_day.isoformat()}
        scan_day = date.fromordinal(scan_day.toordinal() + 1)
        days_scanned += 1

    if retry_items:
        selected_items: list[dict[str, Any]] = []
        selected_keys: list[tuple[str, str]] = []
        selected_ids: set[str] = set()
        for item, key in zip(retry_items, retry_keys):
            row = conn.execute('SELECT status FROM review_auto_retry_action '
                               'WHERE action_id=? AND updated_at=?', key).fetchone()
            if (row and row[0] == 'pending' and item['action_id'] not in selected_ids):
                selected_items.append(item)
                selected_keys.append(key)
                selected_ids.add(item['action_id'])
        if selected_items:
            return queue_verified(selected_items, selected_keys)

    pending_retries = conn.execute("SELECT source_day,action_id FROM review_auto_retry_action "
                                   "WHERE status='pending' ORDER BY source_day,action_id").fetchall()
    if pending_retries:
        result = {'status': 'review_items_retrying', 'day': pending_retries[0][0],
                  'retry_action_ids': [row[1] for row in pending_retries]}
        if scan_day <= today:
            result['next_day'] = scan_day.isoformat()
        if rejected_action_ids:
            result['quarantined_action_ids'] = list(dict.fromkeys(rejected_action_ids))
        return result
    if rejected_action_ids:
        return {'status': 'review_items_quarantined',
                'day': quarantine_day or scan_day.isoformat(),
                'action_ids': list(dict.fromkeys(rejected_action_ids))}
    if scan_day <= today and not today_processed:
        return {'status': 'review_scan_deferred', 'next_day': scan_day.isoformat()}
    return {'status': 'idle'}

