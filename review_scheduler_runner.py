"""Explicitly enabled, bounded entry point for the Cifra review scheduler."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from review_gateway import ReviewGateway, _load_work_query_adapter
from review_identity import validate_owner
from review_inbox import connect
from review_inbox_poll import read_dws
from review_inbox_worker import run as advance_inbox
from review_schedule_dws import ReviewCardDeliveryConfig, ReviewScheduleDws
from review_auto_schedule import enable, run_tick


def run(conn, *, profile: str, bot_open_id: str, owner_open_id: str,
        app_id: str, lock_path: str, activate: bool = False, now=None):
    if not activate:
        table = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                             ('review_auto_schedule',)).fetchone()
        if not table or not conn.execute('SELECT 1 FROM review_auto_schedule WHERE schedule_key=1').fetchone():
            return {'status': 'scheduler_not_enabled'}
    now = now or datetime.now(ZoneInfo('Asia/Shanghai'))
    if now.tzinfo is None:
        raise ValueError('timezone_required')
    now = now.astimezone(ZoneInfo('Asia/Shanghai'))
    validate_owner(profile, owner_open_id)
    info = read_dws(['chat', '+conversation-info', '--open-dingtalk-id', bot_open_id,
                     '--profile', profile])
    conversation = info.get('result', {}).get('conversationInfo', {})
    if (info.get('success') is not True or conversation.get('singleChat') is not True
            or not isinstance(conversation.get('openConversationId'), str)
            or not conversation['openConversationId']):
        raise ValueError('conversation_identity_mismatch')
    if activate:
        enable(conn, now)
    card_config = ReviewCardDeliveryConfig.from_environment(os.environ)
    client = ReviewScheduleDws(conn, profile=profile, bot_open_id=bot_open_id,
                               owner_open_id=owner_open_id, app_id=app_id,
                               card_config=card_config)
    return run_tick(conn, ReviewGateway(_load_work_query_adapter()), today=now.date(),
                    conversation_id=conversation['openConversationId'], owner_id=owner_open_id,
                    bot_open_id=bot_open_id, now=now, preflight=client.preflight,
                    sender=client.send, history_reader=client.history,
                    card_sender=client.send_card if card_config.enabled else None,
                    advance_batch=lambda _batch: advance_inbox(conn, profile, bot_open_id,
                                                                app_id, lock_path))


def main():
    parser = argparse.ArgumentParser()
    for name in ('db', 'profile', 'bot-open-id', 'owner-open-id', 'app-id', 'lock'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--activate', action='store_true')
    args = parser.parse_args()
    try:
        result = run(connect(args.db), profile=args.profile, bot_open_id=args.bot_open_id,
                     owner_open_id=args.owner_open_id, app_id=args.app_id,
                     lock_path=args.lock, activate=args.activate)
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except (ValueError, RuntimeError, OSError, json.JSONDecodeError, subprocess.TimeoutExpired):
        print(json.dumps({'status': 'error', 'reason': 'review_scheduler_failed'}), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
