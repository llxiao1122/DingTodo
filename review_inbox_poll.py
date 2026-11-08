"""Read one bounded Cifra DM window via DWS and persist an unambiguous reply."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys

from dws_runtime import run_dws_json_object
from review_inbox import connect, poll_batch


def read_dws(arguments: list[str]) -> dict:
    return run_dws_json_object(
        arguments,
        timeout=90,
        invalid_json_reason='dws_response_invalid',
        command_failure_reason='dws_read_failed',
        invalid_object_reason='invalid_dws_response',
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    parser.add_argument('--batch', required=True)
    parser.add_argument('--profile', required=True)
    parser.add_argument('--bot-open-id', required=True)
    args = parser.parse_args()
    try:
        conn = connect(args.db)
        batch = conn.execute('SELECT conversation_id,prompt_time,state FROM batch WHERE id=?',
                             (args.batch,)).fetchone()
        if not batch:
            raise ValueError('unknown_batch')
        conversation, prompt_time, state = batch
        if state == 'answered':
            print(json.dumps({'status': 'already_answered'}))
            return 0
        from review_identity import validate_owner
        owner = conn.execute('SELECT owner_id FROM batch WHERE id=?', (args.batch,)).fetchone()[0]
        validate_owner(args.profile, owner)
        info = read_dws(['chat', '+conversation-info', '--open-dingtalk-id', args.bot_open_id,
                         '--profile', args.profile])
        actual = info.get('result', {}).get('conversationInfo', {})
        if info.get('success') is not True or actual.get('openConversationId') != conversation:
            raise ValueError('conversation_identity_mismatch')
        messages = read_dws(['chat', '+chat-messages', '--open-dingtalk-id', args.bot_open_id,
                             '--start', prompt_time, '--page-all', '--max-items', '200',
                             '--no-reactions', '--profile', args.profile])
        print(json.dumps({'status': poll_batch(conn, args.batch, messages)}))
        return 0
    except (ValueError, RuntimeError, OSError, json.JSONDecodeError, subprocess.TimeoutExpired) as exc:
        print(json.dumps({'status': 'error', 'reason': str(exc)}), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
