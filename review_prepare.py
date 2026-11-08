"""Prepare an explicit set of authoritative REVIEW rows for Cifra inquiry."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys

from review_gateway import ReviewGateway, _load_work_query_adapter
from review_inbox import connect, prepare_from_reviews
from review_inbox_poll import read_dws
from review_identity import validate_owner


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    parser.add_argument('--profile', required=True)
    parser.add_argument('--bot-open-id', required=True)
    parser.add_argument('--owner-open-id', required=True)
    parser.add_argument('--review', action='append', required=True, metavar='YYYY-MM-DD:ACTION_ID')
    args = parser.parse_args()
    try:
        validate_owner(args.profile, args.owner_open_id)
        info = read_dws(['chat', '+conversation-info', '--open-dingtalk-id', args.bot_open_id,
                         '--profile', args.profile])
        conversation = info.get('result', {}).get('conversationInfo', {})
        if info.get('success') is not True or conversation.get('singleChat') is not True:
            raise ValueError('bot_conversation_unverified')
        cid = conversation.get('openConversationId')
        if not isinstance(cid, str) or not cid:
            raise ValueError('bot_conversation_missing')
        selections = []
        for entry in args.review:
            parts = entry.split(':')
            if len(parts) != 2:
                raise ValueError('invalid_review_selection')
            selections.append((parts[0], parts[1]))
        conn = connect(args.db)
        batch, text = prepare_from_reviews(conn, ReviewGateway(_load_work_query_adapter()), cid,
                                           args.owner_open_id, selections)
        print(json.dumps({'status': 'prepared', 'batch': batch, 'text': text}, ensure_ascii=False))
        return 0
    except (ValueError, RuntimeError, OSError, json.JSONDecodeError, subprocess.TimeoutExpired) as exc:
        print(json.dumps({'status': 'error', 'reason': str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
