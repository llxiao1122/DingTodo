"""Preview or explicitly confirm a prepared Cifra review message via DWS."""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys

from dws_runtime import run_dws_json_object
from review_gateway import ReviewGateway, _load_work_query_adapter
from review_inbox import claim_dispatch, connect, mark_dispatch_unknown, reconcile_dispatch
from review_identity import validate_owner


def dws(arguments: list[str]) -> dict:
    return run_dws_json_object(
        arguments,
        timeout=90,
        invalid_json_reason='dws_response_invalid',
        command_failure_reason='dws_command_failed',
        invalid_object_reason='dws_response_invalid',
    )


def verify_context(conn, args):
    row = conn.execute('SELECT conversation_id,owner_id,text,items,status,preview_hash,sending_at '
                       'FROM dispatch WHERE id=?', (args.batch,)).fetchone()
    if not row:
        raise ValueError('unknown_dispatch')
    conversation, owner, text, frozen, status, preview_hash, sending_at = row
    if owner != args.owner_open_id:
        raise ValueError('owner_identity_mismatch')
    owner_user = validate_owner(args.profile, owner)
    info = dws(['chat', '+conversation-info', '--open-dingtalk-id', args.bot_open_id,
                '--profile', args.profile])
    dm = info.get('result', {}).get('conversationInfo', {})
    if info.get('success') is not True or dm.get('singleChat') is not True or dm.get('openConversationId') != conversation:
        raise ValueError('conversation_identity_mismatch')
    robot = dws(['dev', 'app', 'robot', 'get', '--unified-app-id', args.app_id,
                 '--profile', args.profile])
    bot = robot.get('data') or {}
    if robot.get('ok') is not True or bot.get('robotStatus') != 'ONLINE' or not bot.get('robotCode'):
        raise ValueError('robot_not_ready')
    robot_code = bot['robotCode']
    digest = hashlib.sha256(json.dumps([args.batch, args.profile, args.bot_open_id, args.app_id,
                                        owner, owner_user, robot_code, text, frozen],
                                       ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()
    return {'conversation': conversation, 'owner_user': owner_user, 'text': text,
            'items': json.loads(frozen), 'status': status, 'preview_hash': preview_hash,
            'sending_at': sending_at, 'robot_code': robot_code, 'digest': digest}


def verify_sources(context):
    from datetime import date
    gateway = ReviewGateway(_load_work_query_adapter())
    for item in context['items']:
        result = gateway.review_detail(item['action_id'], date.fromisoformat(item['source_date']))
        if (result.get('ok') is not True or result.get('complete') is not True
                or result.get('action', {}).get('action_id') != item['action_id']
                or result.get('action', {}).get('status') != 'REVIEW'
                or result.get('source') != item['source']):
            raise ValueError('review_source_changed')


def message_args(args, ctx):
    return ['chat', '+messages-send', '--as', 'bot', '--robot-code', ctx['robot_code'],
            '--users', ctx['owner_user'], '--title', '待办复核', '--text', ctx['text'],
            '--profile', args.profile]


def reconcile(conn, args, ctx):
    if ctx['status'] not in ('sending', 'unknown') or not ctx['sending_at']:
        raise ValueError('dispatch_not_reconcilable')
    payload = dws(['chat', '+chat-messages', '--open-dingtalk-id', args.bot_open_id,
                   '--start', ctx['sending_at'], '--page-all', '--max-items', '200',
                   '--no-reactions', '--profile', args.profile])
    return reconcile_dispatch(conn, args.batch, args.bot_open_id, payload)


def run(conn, args):
    ctx = verify_context(conn, args)
    if args.mode == 'reconcile':
        return {'status': reconcile(conn, args, ctx)}
    if ctx['status'] != 'prepared':
        raise ValueError('dispatch_not_prepared')
    verify_sources(ctx)
    if args.mode == 'preview':
        preview = dws([*message_args(args, ctx), '--dry-run'])
        actions = preview.get('actions')
        if (preview.get('dry_run') is not True or preview.get('executed') is not False
                or not isinstance(actions, list) or len(actions) != 1
                or actions[0].get('identity') != 'bot'):
            raise ValueError('unexpected_send_preview')
        a = actions[0].get('arguments') or {}
        if (a.get('robotCode') != ctx['robot_code'] or a.get('userIds') != [ctx['owner_user']]
                or a.get('markdown') != ctx['text']):
            raise ValueError('send_preview_mismatch')
        conn.execute('UPDATE dispatch SET preview_hash=? WHERE id=? AND status=?',
                     (ctx['digest'], args.batch, 'prepared'))
        return {'status': 'preview', 'batch': args.batch, 'text': ctx['text']}
    if args.confirm_batch != args.batch or ctx['preview_hash'] != ctx['digest']:
        raise ValueError('matching_preview_and_confirmation_required')
    claim_dispatch(conn, args.batch)
    try:
        response = dws([*message_args(args, ctx), '--yes'])
        result = response.get('result') or {}
        if response.get('ok') is not True or result.get('success') is not True:
            raise RuntimeError('send_result_unknown')
    except BaseException:
        mark_dispatch_unknown(conn, args.batch)
        raise
    ctx = verify_context(conn, args)
    try:
        status = reconcile(conn, args, ctx)
    except (ValueError, RuntimeError, OSError, subprocess.TimeoutExpired):
        status = 'sent_delivery_unverified'
    return {'status': status}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['preview', 'send', 'reconcile'])
    for flag in ('db', 'batch', 'profile', 'bot-open-id', 'owner-open-id', 'app-id'):
        parser.add_argument('--' + flag, required=True)
    parser.add_argument('--confirm-batch')
    args = parser.parse_args()
    try:
        print(json.dumps(run(connect(args.db), args), ensure_ascii=False))
        return 0
    except (ValueError, RuntimeError, OSError, json.JSONDecodeError, subprocess.TimeoutExpired) as exc:
        print(json.dumps({'status': 'error', 'reason': str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
