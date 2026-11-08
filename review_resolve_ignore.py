"""Resolve one owner-confirmed ignore using the existing transactional writer."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import subprocess
import sys

from review_gateway import _load_work_query_adapter
from review_handoff import verified_handoff
from review_inbox import connect

sys.path.insert(0, os.environ.get('HERMES_POLARDB_RUNTIME', '/root/.hermes/polardb_runtime'))
from workflows.polardb_work_writer.writer import WorkWriter


def resolve(conn, batch_id, ordinal, profile, bot_id, lock_path, apply=False):
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        prior = conn.execute('SELECT status FROM resolution WHERE batch_id=? AND ordinal=?',
                             (batch_id, ordinal)).fetchone()
        if prior:
            return {'status': 'already_recorded_needs_reconciliation', 'resolution_status': prior[0]}
        handoff = verified_handoff(conn, batch_id, profile, bot_id)
        selected = [row for row in handoff['items'] if row['ordinal'] == ordinal]
        if len(selected) != 1 or selected[0]['user_decision'] != 'ignore':
            raise ValueError('ignore_decision_not_verified')
        item = selected[0]
        if item['handoff_state'] != 'ready_for_existing_worker_review' or len(item['sources']) != 1:
            raise ValueError('review_action_not_safe_to_resolve')
        source = item['sources'][0]
        request = {'operation': 'resolve_review_ignore', 'action_id': int(source['action_id']),
                   'conversation_id': source['conversation_id'], 'message_id': source['message_id'],
                   'expected_updated_at': source['updated_at'],
                   'expected_business_key': source['business_key']}
        writer = WorkWriter()
        preview = writer.execute({**request, 'dry_run': True})
        if preview.get('ok') is not True or preview.get('committed') is not False or preview.get('row', {}).get('status') != 'IGNORED':
            raise ValueError('writer_preview_failed')
        if not apply:
            return {'status': 'preview', 'action_id': source['action_id'], 'decision': 'ignore'}
        conn.execute('BEGIN IMMEDIATE')
        try:
            conn.execute('INSERT INTO resolution (batch_id,ordinal,action_id,decision,reply_message_id,request_json,status) VALUES (?,?,?,?,?,?,?)',
                         (batch_id, ordinal, source['action_id'], 'ignore', handoff['reply_message_id'],
                          json.dumps(request, ensure_ascii=False, separators=(',', ':')), 'applying'))
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        result = writer.execute(request)
        if result.get('ok') is not True or result.get('committed') is not True:
            raise ValueError('writer_commit_unknown')
        adapter = _load_work_query_adapter()
        readback = adapter.query({'view': 'actions', 'conversation_id': source['conversation_id'],
                                  'message_id': source['message_id'], 'limit': 10})
        rows = [row for row in readback.get('rows', []) if str(row.get('id')) == source['action_id']]
        if (readback.get('has_more') is not False or len(rows) != 1 or rows[0].get('status') != 'IGNORED'
                or rows[0].get('external_task_id') or rows[0].get('duplicate_of_action_id')):
            raise ValueError('authoritative_readback_incomplete')
        conn.execute("UPDATE resolution SET status='applied' WHERE batch_id=? AND ordinal=? AND status='applying'",
                     (batch_id, ordinal))
        return {'status': 'applied', 'action_id': source['action_id'], 'decision': 'ignore'}
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def main():
    parser = argparse.ArgumentParser()
    for flag in ('db', 'batch', 'profile', 'bot-open-id', 'lock'):
        parser.add_argument('--' + flag, required=True)
    parser.add_argument('--ordinal', required=True, type=int)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    try:
        print(json.dumps(resolve(connect(args.db), args.batch, args.ordinal, args.profile,
                                 args.bot_open_id, args.lock, args.apply), ensure_ascii=False))
        return 0
    except (ValueError, RuntimeError, OSError, json.JSONDecodeError, subprocess.TimeoutExpired) as exc:
        print(json.dumps({'status': 'error', 'reason': str(exc)}), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
