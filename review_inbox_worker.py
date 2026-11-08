"""Single bounded poll for the one active human-review batch, if present."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from review_inbox import connect, poll_batch
from review_inbox_poll import read_dws
from review_identity import validate_owner
from review_auto_process import process_answered_batch, process_needs_review_batch


def run(conn, profile: str, bot_open_id: str, app_id: str | None = None,
        lock_path: str | None = None, batch_id: str | None = None, *,
        send_ack: bool = True, open_only: bool = False) -> dict:
    if open_only and batch_id is None:
        return {'status': 'batch_id_required'}
    open_rows = conn.execute("SELECT id,conversation_id,prompt_time,owner_id FROM batch WHERE state='open'").fetchall()
    if len(open_rows) > 1:
        raise ValueError('multiple_open_batches')
    if open_only and (not open_rows or open_rows[0][0] != batch_id):
        return {'status': 'not_open', 'batch': batch_id}
    if batch_id is not None and open_rows and open_rows[0][0] != batch_id:
        return {'status': 'busy'}
    rows = [row for row in open_rows if batch_id is None or row[0] == batch_id]
    if rows:
        batch, conversation, prompt_time, owner = rows[0]
        validate_owner(profile, owner)
        info = read_dws(['chat', '+conversation-info', '--open-dingtalk-id', bot_open_id,
                         '--profile', profile])
        chat = info.get('result', {}).get('conversationInfo', {})
        if (info.get('success') is not True or chat.get('singleChat') is not True
                or chat.get('openConversationId') != conversation):
            raise ValueError('conversation_identity_mismatch')
        messages = read_dws(['chat', '+chat-messages', '--open-dingtalk-id', bot_open_id,
                             '--start', prompt_time, '--page-all', '--max-items', '200',
                             '--no-reactions', '--profile', profile])
        poll_status = poll_batch(conn, batch, messages)
        if poll_status in ('reply_needs_review', 'multiple_replies_need_review',
                           'clarification_delivery_unknown'):
            result = process_needs_review_batch(
                conn, batch, profile, bot_open_id, app_id, messages, send_ack=send_ack,
            )
            return {**result, 'poll_status': poll_status}
        if open_only and poll_status == 'already_answered':
            return {'status': 'not_open', 'batch': batch}
        if poll_status not in ('recorded', 'already_answered'):
            return {'status': poll_status, 'batch': batch}
        result = process_answered_batch(conn, batch, profile, bot_open_id, app_id,
                                        lock_path or _default_lock_path(conn), send_ack=send_ack)
        return {**result, 'poll_status': poll_status}

    if open_only:
        return {'status': 'not_open', 'batch': batch_id}

    if batch_id is None:
        answered = conn.execute("SELECT id FROM batch WHERE state='answered' ORDER BY prompt_time,id").fetchall()
    else:
        answered = conn.execute("SELECT id FROM batch WHERE id=? AND state='answered'", (batch_id,)).fetchall()
    if not answered:
        return {'status': 'idle'}
    results = []
    for row in answered:
        try:
            results.append(process_answered_batch(conn, row[0], profile, bot_open_id, app_id,
                                                  lock_path or _default_lock_path(conn),
                                                  send_ack=send_ack))
        except (ValueError, RuntimeError, OSError, json.JSONDecodeError, subprocess.TimeoutExpired):
            results.append({'status': 'pending', 'reason': 'batch_processing_failed',
                            'batch': row[0]})
    statuses = {result.get('status') for result in results}
    if len(results) == 1:
        return results[0]
    if statuses == {'all_ignored'}:
        return {'status': 'all_ignored', 'batches': results}
    if statuses == {'complete'}:
        return {'status': 'complete', 'batches': results}
    return {'status': 'pending', 'batches': results}


def _default_lock_path(conn) -> str:
    database_path = conn.execute('PRAGMA database_list').fetchone()[2]
    if not database_path:
        raise ValueError('review_database_path_unavailable')
    return str(Path(database_path).with_suffix('.review-resolution.lock'))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    parser.add_argument('--profile', required=True)
    parser.add_argument('--bot-open-id', required=True)
    parser.add_argument('--app-id', default=os.environ.get('DINGTODO_REVIEW_APP_ID'))
    parser.add_argument('--lock')
    parser.add_argument('--batch')
    parser.add_argument('--open-only', action='store_true')
    parser.add_argument('--skip-ack', action='store_true')
    parser.add_argument('--scheduler-owner-open-id')
    args = parser.parse_args()
    try:
        conn = connect(args.db)
        if args.scheduler_owner_open_id:
            if not args.app_id:
                raise ValueError('cifra_app_id_not_configured')
            from review_scheduler_runner import run as run_scheduler
            result = run_scheduler(conn, profile=args.profile, bot_open_id=args.bot_open_id,
                                   owner_open_id=args.scheduler_owner_open_id, app_id=args.app_id,
                                   lock_path=args.lock or _default_lock_path(conn))
            if result.get('status') == 'scheduler_not_enabled':
                result = run(conn, args.profile, args.bot_open_id, args.app_id, args.lock,
                             batch_id=args.batch, send_ack=not args.skip_ack,
                             open_only=args.open_only)
        else:
            result = run(conn, args.profile, args.bot_open_id, args.app_id, args.lock,
                         batch_id=args.batch, send_ack=not args.skip_ack,
                         open_only=args.open_only)
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except (ValueError, RuntimeError, OSError, json.JSONDecodeError, subprocess.TimeoutExpired) as exc:
        print(json.dumps({'status': 'error', 'reason': str(exc)}), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
