"""Evidence-bound manual backfill through the shared todo execution boundary."""
from __future__ import annotations

from typing import Any, Callable

from dws_adapter import verify_todo
from routing import build_source_comment
from todo_execution import add_verified_source_comment, create_claimed_task


def backfill(*, action: dict[str, Any], source: dict[str, Any], plan: dict[str, Any],
             writer: Any, dws: Any, check_duplicates: Callable[[], None],
             record_remote_task: Callable[[int, str], None]) -> str:
    """Process one already-authorized source; callers own operator authentication.

    A durable CLAIMED action is left intact when remote creation is uncertain.
    Callers must reconcile that state before attempting any later operation.
    """
    action_id = action.get('id')
    if (not isinstance(action_id, int) or isinstance(action_id, bool) or action_id <= 0
            or action.get('status') != 'REVIEW' or action.get('external_task_id')
            or action.get('duplicate_of_action_id')):
        raise ValueError('manual_action_not_safe')
    if (source.get('conversation_id') != action.get('conversation_id')
            or source.get('message_id') != action.get('message_id')
            or not isinstance(source.get('text'), str) or not source['text'].strip()):
        raise ValueError('manual_source_mismatch')
    if (plan.get('decision') != 'CREATE' or not plan.get('title')
            or not plan.get('executor_ids') or not plan.get('executor_names')):
        raise ValueError('manual_plan_incomplete')
    check_duplicates()
    claim = {'operation': 'claim_review_create', 'action_id': action_id,
             'conversation_id': action['conversation_id'], 'message_id': action['message_id'],
             'expected_updated_at': action['updated_at'],
             'expected_business_key': action.get('business_key')}
    result = writer.execute(claim)
    if (result.get('ok') is not True or result.get('committed') is not True
            or str(result.get('action_id')) != str(action_id)
            or result.get('row', {}).get('status') != 'CLAIMED'):
        raise RuntimeError('manual_claim_unconfirmed')

    def mark(status: str, error: str | None):
        result = writer.execute({'operation': 'save_action_result', 'action_id': action_id,
                                 'status': status, 'last_error': error})
        if (result.get('ok') is not True or result.get('committed') is not True
                or result.get('row', {}).get('status') != status):
            raise RuntimeError('manual_status_unconfirmed')

    mark('CLAIMED', 'create_write_started')

    def link(_action_id: str, task_id: str):
        record_remote_task(action_id, task_id)
        result = writer.execute({'operation': 'link_external_task', 'action_id': action_id,
                                 'external_task_id': task_id})
        if (result.get('ok') is not True or result.get('committed') is not True
                or result.get('row', {}).get('external_task_id') != task_id):
            raise RuntimeError('manual_link_unconfirmed')

    task_id = create_claimed_task(action_id=action_id, claim_acquired=True,
                                  create=lambda: dws.create_todo(plan, priority=plan.get('priority', 20)),
                                  link=link)
    comment = build_source_comment({'group': source.get('group_name'), 'sender': source.get('sender_name'),
                                    'create_time': source.get('message_time'),
                                    'message_id': source['message_id'], 'text': source['text']})
    add_verified_source_comment(dws, task_id, comment, check_existing=False,
                                on_attempt=lambda: mark('EXTERNAL_CREATED', 'source_comment_write_started'),
                                on_verified=lambda: mark('EXTERNAL_CREATED', None))
    detail = dws.get_todo(task_id)
    if str(detail.get('taskId') or detail.get('todoTaskId') or '') != task_id or verify_todo(detail, plan):
        raise RuntimeError('manual_task_readback_mismatch')
    return task_id


def prepare(*, gateway: Any, adapter: Any, day: Any, action_id: str, title: str,
            query: str, identity: tuple[str, str], dws: Any) -> tuple[dict, dict, dict, str]:
    """Build a read-only, exact-source manual plan and stable confirmation digest."""
    import hashlib
    import json
    from review_resolve_create import check_candidates
    from dws_adapter import build_create_args

    detail = gateway.review_detail(action_id, day)
    if detail.get('ok') is not True or detail.get('complete') is not True or not detail.get('source'):
        raise ValueError('manual_review_read_incomplete')
    ref = detail['action']
    result = adapter.query({'view': 'actions', 'conversation_id': ref['conversation_id'],
                            'message_id': ref['message_id'], 'limit': 10})
    rows = [row for row in result.get('rows', []) if str(row.get('id')) == str(action_id)]
    if result.get('has_more') is not False or len(rows) != 1:
        raise ValueError('manual_action_read_incomplete')
    action = rows[0]
    src = detail['source']
    if (action.get('status') != 'REVIEW' or action.get('external_task_id')
            or action.get('duplicate_of_action_id') or src.get('text_truncated')):
        raise ValueError('manual_action_not_safe')
    if (not isinstance(title, str) or not 1 <= len(title.strip()) <= 100
            or not isinstance(query, str) or not query.strip()
            or query not in title or query not in src['text']):
        raise ValueError('manual_title_or_query_invalid')
    uid, name = identity
    if not uid or not name:
        raise ValueError('manual_identity_missing')
    source = {'conversation_id': ref['conversation_id'], 'message_id': ref['message_id'],
              'group_name': src['group_name'], 'sender_name': src['sender_name'],
              'message_time': src['message_time'], 'text': src['text']}
    plan = {'decision': 'CREATE', 'title': title.strip(), 'executor_ids': [uid],
            'executor_names': [name], 'priority': 20, 'allow_executor_namespace_fallback': True}
    check_candidates(dws, query, title, source['message_id'])
    preview = dws.run([*build_create_args(title=plan['title'], executor_ids=[uid],
                                           due_iso=None, priority=20), '--dry-run'])
    create = preview.get('arguments', {}).get('PersonalTodoCreateVO', {})
    if (preview.get('dry_run') is not True or preview.get('executed') is not False
            or create.get('subject') != plan['title'] or create.get('executorIds') != [uid]
            or create.get('priority') != 20 or create.get('dueTime') not in (None, 0)):
        raise ValueError('manual_todo_preview_mismatch')
    snapshot = {'action': action, 'source': source, 'plan': plan, 'query': query}
    digest = hashlib.sha256(json.dumps(snapshot, ensure_ascii=False, sort_keys=True,
                                      separators=(',', ':'), default=str).encode()).hexdigest()
    return action, source, plan, digest


def execute_prepared(*, gateway: Any, adapter: Any, day: Any, action_id: str,
                     title: str, query: str, profile: str, owner_open_id: str,
                     writer: Any, dws: Any, journal: Any, lock_path: str,
                     expected_digest: str) -> str:
    """Apply only the same preview, with unique current-user identity and receipt."""
    import fcntl
    import os
    from review_resolve_create import check_candidates, owner_identity

    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        uid, name, _ = owner_identity(profile, owner_open_id)
        action, source, plan, digest = prepare(gateway=gateway, adapter=adapter, day=day,
                                                action_id=action_id, title=title, query=query,
                                                identity=(uid, name), dws=dws)
        if not expected_digest or expected_digest != digest:
            raise ValueError('manual_preview_changed')
        if journal.lookup(action['id']) is not None:
            raise ValueError('manual_existing_receipt_requires_reconciliation')
        return backfill(action=action, source=source, plan=plan, writer=writer, dws=dws,
                        check_duplicates=lambda: check_candidates(dws, query, title, source['message_id']),
                        record_remote_task=journal.record)
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def main(argv: list[str] | None = None) -> int:
    import argparse
    import json
    import os
    import sys
    from datetime import date
    from dws_adapter import DwsClient
    from manual_receipts import ManualReceiptJournal
    from review_gateway import ReviewGateway, _load_work_query_adapter
    from review_resolve_create import owner_identity
    from workflows.polardb_work_writer.writer import WorkWriter

    parser = argparse.ArgumentParser(description='Evidence-bound manual todo backfill')
    parser.add_argument('--day', required=True)
    parser.add_argument('--action-id', required=True)
    parser.add_argument('--title', required=True)
    parser.add_argument('--query', required=True)
    parser.add_argument('--profile', required=True)
    parser.add_argument('--owner-open-id', required=True)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--expected-digest')
    parser.add_argument('--receipt-db', default='/root/.hermes/dingtalk-listener-state/manual-receipts.sqlite3')
    parser.add_argument('--lock', default='/root/.hermes/dingtalk-listener-state/work.lock')
    args = parser.parse_args(argv)
    try:
        if args.apply and not args.expected_digest:
            raise ValueError('manual_preview_digest_required')
        day = date.fromisoformat(args.day)
        adapter = _load_work_query_adapter()
        gateway = ReviewGateway(adapter)
        dws = DwsClient(confirmed_official_writes=args.apply)
        uid, name, _ = owner_identity(args.profile, args.owner_open_id)
        if not args.apply:
            _, _, plan, digest = prepare(gateway=gateway, adapter=adapter, day=day,
                                         action_id=args.action_id, title=args.title, query=args.query,
                                         identity=(uid, name), dws=dws)
            print(json.dumps({'status': 'preview', 'digest': digest, 'title': plan['title'],
                              'executor': name}, ensure_ascii=False))
            return 0
        os.environ.setdefault('HERMES_WORK_WRITER_ENV', '/etc/cipher-work.env')
        journal = ManualReceiptJournal(args.receipt_db)
        try:
            task = execute_prepared(gateway=gateway, adapter=adapter, day=day, action_id=args.action_id,
                                    title=args.title, query=args.query, profile=args.profile,
                                    owner_open_id=args.owner_open_id, writer=WorkWriter(), dws=dws,
                                    journal=journal, lock_path=args.lock, expected_digest=args.expected_digest)
        finally:
            journal.close()
        print(json.dumps({'status': 'created_and_verified', 'task_id': task}, ensure_ascii=False))
        return 0
    except (ValueError, RuntimeError, OSError) as exc:
        print(json.dumps({'status': 'error', 'reason': str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
