"""Repair one verified mislinked review family and create one user-approved task."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import subprocess
import sys
import unicodedata
from datetime import datetime
from zoneinfo import ZoneInfo

from dws_adapter import DwsClient, build_create_args, verify_todo
from review_gateway import _load_work_query_adapter
from review_handoff import read_all_actions, verified_handoff
from review_inbox import connect
from review_inbox_poll import read_dws
from review_resolve_create import owner_identity
from routing import build_source_comment
from todo_execution import add_verified_source_comment, create_claimed_task

sys.path.insert(0, os.environ.get('HERMES_POLARDB_RUNTIME', '/root/.hermes/polardb_runtime'))
from workflows.polardb_work_writer.writer import WorkWriter


def normalize_notice(text: str) -> str:
    value = unicodedata.normalize('NFKC', str(text or '')).strip()
    value = re.sub(r'^\[DING\]\s*', '', value)
    value = re.sub(r'@(?:所有人|工班长)', '', value)
    return re.sub(r'\s+', '', value)


def source_event(source):
    return {'group': source['source']['group_name'], 'sender': source['source']['sender_name'],
            'create_time': source['source']['message_time'], 'message_id': source['message_id'],
            'text': source['source']['text']}


def snapshot(row):
    return {'action_id': int(row['id']), 'conversation_id': row['conversation_id'],
            'message_id': row['message_id'], 'action_key': row['action_key'],
            'status': row['status'], 'business_key': row.get('business_key'),
            'external_task_id': row.get('external_task_id'),
            'duplicate_of_action_id': row.get('duplicate_of_action_id'),
            'parent_action_id': row.get('parent_action_id'), 'updated_at': row['updated_at']}


def read_action(adapter, source):
    page = adapter.query({'view': 'actions', 'conversation_id': source['conversation_id'],
                          'message_id': source['message_id'], 'limit': 10})
    matches = [row for row in page.get('rows', []) if str(row.get('id')) == str(source['action_id'])]
    if page.get('has_more') is not False or len(matches) != 1:
        raise ValueError('action_readback_incomplete')
    return matches[0]


def read_family(handoff, adapter):
    item = next((row for row in handoff['items'] if row['ordinal'] == 3), None)
    if not item or item['user_decision'] != 'create' or len(item['sources']) != 3:
        raise ValueError('duplicate_family_decision_not_verified')
    sources = sorted(item['sources'], key=lambda row: int(row['action_id']))
    if len({normalize_notice(row['source']['text']) for row in sources}) != 1:
        raise ValueError('duplicate_sources_are_not_the_same_notice')
    duplicates = {str(row['duplicate_of_action_id']) for row in sources}
    if len(duplicates) != 1 or 'None' in duplicates:
        raise ValueError('duplicate_family_pointer_not_unique')
    old_canonical_id = int(next(iter(duplicates)))
    canonical_sources = [row for row in item['canonical_sources']
                         if str(row['action_id']) == str(old_canonical_id)]
    if len(canonical_sources) != 1 or normalize_notice(canonical_sources[0]['source']['text']) in {
            normalize_notice(row['source']['text']) for row in sources}:
        raise ValueError('old_canonical_source_not_proven_unrelated')
    all_rows = read_all_actions(adapter)
    by_id = {str(row['id']): row for row in all_rows}
    if len(by_id) != len(all_rows):
        raise ValueError('duplicate_action_identity')
    source_ids = [str(row['action_id']) for row in sources]
    if len(source_ids) != len(set(source_ids)) or any(action_id not in by_id for action_id in source_ids):
        raise ValueError('duplicate_family_action_missing')
    children = {str(row['id']) for row in all_rows
                if str(row.get('duplicate_of_action_id')) == str(old_canonical_id)}
    if children != set(source_ids):
        raise ValueError('old_canonical_has_unverified_duplicate_children')
    old_row = by_id.get(str(old_canonical_id))
    if (not old_row or old_row.get('duplicate_of_action_id') not in (None, '')
            or old_row.get('external_task_id') not in (None, '')
            or old_row.get('parent_action_id') not in (None, '')):
        raise ValueError('old_canonical_state_changed')
    selected = [by_id[action_id] for action_id in source_ids]
    for row in selected:
        if (row.get('action_key') != 'todo' or row.get('status') != 'REVIEW'
                or str(row.get('duplicate_of_action_id')) != str(old_canonical_id)
                or row.get('external_task_id') not in (None, '')
                or row.get('parent_action_id') not in (None, '')):
            raise ValueError('duplicate_family_state_changed')
    canonical_id = int(source_ids[0])
    request = {'operation': 'repair_review_duplicate_family',
               'canonical_action_id': canonical_id,
               'old_canonical_action_id': old_canonical_id,
               'duplicate_action_ids': [int(value) for value in source_ids[1:]],
               'expected_rows': [snapshot(by_id[key]) for key in
                                 sorted([*source_ids, str(old_canonical_id)], key=int)]}
    return sources, canonical_id, old_canonical_id, request


def check_no_existing_task(dws, query, title, source_ids):
    rows = dws.search_todos(query)
    if len(rows) > 50:
        raise ValueError('too_many_duplicate_candidates')
    normalized_title = re.sub(r'\s+', '', unicodedata.normalize('NFKC', title))
    for row in rows:
        task_id = row['taskId']
        detail = dws.get_todo(task_id)
        if str(detail.get('taskId') or detail.get('todoTaskId') or '') != task_id:
            raise ValueError('duplicate_candidate_identity_unverified')
        if re.sub(r'\s+', '', unicodedata.normalize('NFKC', str(detail.get('subject') or ''))) == normalized_title:
            raise ValueError('exact_title_task_already_exists')
        comments = dws.list_task_comments(task_id)
        if any(any(source_id in json.dumps(comment, ensure_ascii=False) for source_id in source_ids)
               for comment in comments):
            raise ValueError('source_already_linked_to_todo')
    return len(rows)


def update_audit(conn, batch_id, audit, *, task_id=None):
    if task_id is None:
        changed = conn.execute("UPDATE resolution SET request_json=? WHERE batch_id=? AND ordinal=3 AND status='applying'",
                               (json.dumps(audit, ensure_ascii=False, separators=(',', ':')), batch_id)).rowcount
    else:
        changed = conn.execute("UPDATE resolution SET request_json=?,external_task_id=? "
                               "WHERE batch_id=? AND ordinal=3 AND status='applying' AND external_task_id IS NULL",
                               (json.dumps(audit, ensure_ascii=False, separators=(',', ':')),
                                str(task_id), batch_id)).rowcount
    if changed != 1:
        raise ValueError('review_resolution_state_changed')


def verify_linked_actions(adapter, sources, canonical_id, task_id, expected_status):
    rows = [read_action(adapter, source) for source in sources]
    canonical = next(row for row in rows if str(row['id']) == str(canonical_id))
    allowed_statuses = expected_status if isinstance(expected_status, tuple) else (expected_status,)
    if canonical.get('status') not in allowed_statuses or canonical.get('external_task_id') != task_id:
        raise ValueError('canonical_action_task_link_mismatch')
    for row in rows:
        if str(row['id']) == str(canonical_id):
            continue
        if (str(row.get('duplicate_of_action_id')) != str(canonical_id)
                or row.get('external_task_id') not in (None, '')
                or row.get('effective_external_task_id') != task_id):
            raise ValueError('duplicate_action_task_link_mismatch')


def finalize(conn, batch_id, ordinal, profile, bot_id, audit, task_id):
    handoff = verified_handoff(conn, batch_id, profile, bot_id)
    item = next((row for row in handoff['items'] if row['ordinal'] == ordinal), None)
    if not item or item['user_decision'] != 'create' or len(item['sources']) != 3:
        raise ValueError('duplicate_family_decision_not_verified')
    sources = sorted(item['sources'], key=lambda row: int(row['action_id']))
    canonical_id = int(audit['canonical_action_id'])
    old_id = int(audit['old_canonical_action_id'])
    if ([str(row['action_id']) for row in sources] != audit['source_action_ids']
            or len({normalize_notice(row['source']['text']) for row in sources}) != 1):
        raise ValueError('duplicate_family_identity_changed')
    adapter = _load_work_query_adapter()
    action_rows = [read_action(adapter, source) for source in sources]
    canonical = next((row for row in action_rows if str(row['id']) == str(canonical_id)), None)
    if (not canonical or canonical.get('duplicate_of_action_id') not in (None, '')
            or any(str(row.get('duplicate_of_action_id')) != str(canonical_id)
                   for row in action_rows if str(row['id']) != str(canonical_id))):
        raise ValueError('duplicate_family_repair_readback_failed')
    all_actions = read_all_actions(adapter)
    if any(str(row.get('duplicate_of_action_id')) == str(old_id) for row in all_actions):
        raise ValueError('old_canonical_still_has_duplicate_children')
    old_row = next((row for row in all_actions if str(row.get('id')) == str(old_id)), None)
    if not old_row:
        raise ValueError('old_canonical_missing')
    old_source_response = adapter.query({'view': 'message',
                                         'conversation_id': old_row['conversation_id'],
                                         'message_id': old_row['message_id'], 'limit': 2})
    old_sources = old_source_response.get('rows', [])
    if (old_source_response.get('has_more') is not False or len(old_sources) != 1
            or normalize_notice(old_sources[0].get('text')) in
            {normalize_notice(row['source']['text']) for row in sources}):
        raise ValueError('old_canonical_source_relation_not_verified')
    batch_owner = conn.execute('SELECT owner_id FROM batch WHERE id=?', (batch_id,)).fetchone()[0]
    user_id, user_name, contact_evidence = owner_identity(profile, batch_owner)
    if user_id != audit['executor_user_id']:
        raise ValueError('executor_identity_changed')
    dws = DwsClient(confirmed_official_writes=True)
    detail = dws.get_todo(task_id)
    if str(detail.get('taskId') or detail.get('todoTaskId') or '') != str(task_id):
        raise ValueError('todo_identity_mismatch')
    plan = {'decision': 'CREATE', 'title': audit['title'], 'executor_ids': [user_id],
            'executor_names': [user_name], 'priority': 20,
            'allow_executor_namespace_fallback': True}
    problems = verify_todo(detail, plan)
    if detail.get('dueTime') not in (None, 0, '') or detail.get('due') not in (None, 0, ''):
        problems.append('unexpected_due_date')
    if problems:
        raise ValueError('todo_readback_mismatch:' + ','.join(problems))
    for source in sources:
        comment = build_source_comment(source_event(source))
        message_id = source['message_id']
        attempted = set(audit.get('comment_attempted_message_ids', []))

        def mark_comment_attempted():
            audit['comment_attempted_message_ids'] = sorted(attempted | {message_id})
            update_audit(conn, batch_id, audit)

        add_verified_source_comment(
            dws,
            task_id,
            comment,
            already_attempted=message_id in attempted,
            check_existing=True,
            on_attempt=mark_comment_attempted,
        )
    before = [read_action(adapter, source) for source in sources]
    canonical = next(row for row in before if str(row['id']) == str(canonical_id))
    if canonical.get('external_task_id') not in (None, '', task_id):
        raise ValueError('canonical_task_id_conflict')
    if canonical.get('status') == 'CLAIMED':
        linked = WorkWriter().execute({'operation': 'link_external_task', 'action_id': canonical_id,
                                       'external_task_id': str(task_id)})
        if linked.get('ok') is not True or linked.get('committed') is not True:
            raise ValueError('canonical_task_link_unknown')
    elif canonical.get('status') not in ('EXTERNAL_CREATED', 'SUCCESS'):
        raise ValueError('canonical_action_not_claimed')
    verify_linked_actions(adapter, sources, canonical_id, str(task_id),
                          ('EXTERNAL_CREATED', 'SUCCESS'))
    current = read_action(adapter, next(row for row in sources if str(row['action_id']) == str(canonical_id)))
    if current.get('status') == 'EXTERNAL_CREATED':
        saved = WorkWriter().execute({'operation': 'save_action_result', 'action_id': canonical_id,
                                      'status': 'SUCCESS'})
        if saved.get('ok') is not True or saved.get('committed') is not True:
            raise ValueError('canonical_task_result_unknown')
    verify_linked_actions(adapter, sources, canonical_id, str(task_id), 'SUCCESS')
    audit['verification'] = {'task_id': str(task_id), 'executor': user_name,
                             'contact_identity': contact_evidence,
                             'executor_id_namespace_normalized': bool(plan.get('identity_fallback_evidence')),
                             'source_comment_message_ids': [row['message_id'] for row in sources],
                             'duplicate_family_repaired': True}
    changed = conn.execute("UPDATE resolution SET status='applied',request_json=? "
                           "WHERE batch_id=? AND ordinal=? AND status='applying' AND external_task_id=?",
                           (json.dumps(audit, ensure_ascii=False, separators=(',', ':')),
                            batch_id, ordinal, str(task_id))).rowcount
    if changed != 1:
        raise ValueError('review_resolution_state_changed')
    return {'status': 'applied', 'action_id': str(canonical_id),
            'duplicate_action_ids': [str(row['action_id']) for row in sources if str(row['action_id']) != str(canonical_id)],
            'external_task_id': str(task_id), 'executor': user_name}


def resolve(conn, batch_id, ordinal, profile, bot_id, lock_path, title, query, apply=False, reconcile=False):
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        prior = conn.execute('SELECT status,external_task_id,request_json FROM resolution WHERE batch_id=? AND ordinal=?',
                             (batch_id, ordinal)).fetchone()
        if prior:
            if reconcile and prior[0] == 'applying' and prior[1]:
                return finalize(conn, batch_id, ordinal, profile, bot_id,
                                json.loads(prior[2]), prior[1])
            return {'status': 'already_recorded_needs_reconciliation',
                    'resolution_status': prior[0], 'external_task_id': prior[1]}
        if reconcile:
            raise ValueError('no_existing_duplicate_create_to_reconcile')
        if ordinal != 3:
            raise ValueError('duplicate_family_handler_requires_ordinal_3')
        handoff = verified_handoff(conn, batch_id, profile, bot_id)
        adapter = _load_work_query_adapter()
        sources, canonical_id, old_id, repair = read_family(handoff, adapter)
        owner = conn.execute('SELECT owner_id FROM batch WHERE id=?', (batch_id,)).fetchone()[0]
        user_id, user_name, contact_evidence = owner_identity(profile, owner)
        reply = conn.execute('SELECT content FROM reply WHERE batch_id=?', (batch_id,)).fetchone()
        if not reply or '执行人是我' not in reply[0]:
            raise ValueError('executor_not_explicitly_confirmed')
        if (not isinstance(title, str) or not 1 <= len(title.strip()) <= 100
                or not query or query not in title):
            raise ValueError('title_or_duplicate_query_invalid')
        plan = {'decision': 'CREATE', 'title': title.strip(), 'executor_ids': [user_id],
                'executor_names': [user_name], 'priority': 20,
                'allow_executor_namespace_fallback': True}
        dws = DwsClient(confirmed_official_writes=True)
        candidate_count = check_no_existing_task(dws, query, title, [row['message_id'] for row in sources])
        writer = WorkWriter()
        repair_preview = writer.execute({**repair, 'dry_run': True})
        if repair_preview.get('ok') is not True or repair_preview.get('committed') is not False:
            raise ValueError('duplicate_repair_preview_failed')
        create_args = build_create_args(title=title, executor_ids=[user_id], due_iso=None, priority=20)
        preview = dws.run([*create_args, '--dry-run'])
        create = preview.get('arguments', {}).get('PersonalTodoCreateVO', {})
        if (preview.get('dry_run') is not True or preview.get('executed') is not False
                or create.get('subject') != title or create.get('executorIds') != [user_id]
                or create.get('priority') != 20 or create.get('dueTime') not in (None, 0)):
            raise ValueError('todo_preview_mismatch')
        if not apply:
            return {'status': 'preview', 'title': title, 'executor': user_name, 'due': None,
                    'source_action_ids': [row['action_id'] for row in sources],
                    'repaired_wrong_canonical_action_id': str(old_id),
                    'new_canonical_action_id': str(canonical_id),
                    'duplicate_candidates_checked': candidate_count}

        audit = {'title': title.strip(), 'executor_user_id': user_id,
                 'contact_identity': contact_evidence,
                 'source_action_ids': [str(row['action_id']) for row in sources],
                 'source_message_ids': [row['message_id'] for row in sources],
                 'canonical_action_id': canonical_id, 'old_canonical_action_id': old_id,
                 'repair_request': repair, 'create_attempted': False,
                 'comment_attempted_message_ids': []}
        conn.execute('BEGIN IMMEDIATE')
        try:
            conn.execute('INSERT INTO resolution '
                         '(batch_id,ordinal,action_id,decision,reply_message_id,request_json,status) '
                         'VALUES (?,?,?,?,?,?,?)',
                         (batch_id, ordinal, str(canonical_id), 'create', handoff['reply_message_id'],
                          json.dumps(audit, ensure_ascii=False, separators=(',', ':')), 'applying'))
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        repaired = writer.execute(repair)
        if repaired.get('ok') is not True or repaired.get('committed') is not True:
            raise ValueError('duplicate_family_repair_unknown')
        repaired_rows = [read_action(adapter, source) for source in sources]
        canonical = next(row for row in repaired_rows if str(row['id']) == str(canonical_id))
        if canonical.get('duplicate_of_action_id') not in (None, ''):
            raise ValueError('canonical_repair_readback_failed')
        for row in repaired_rows:
            if str(row['id']) != str(canonical_id) and str(row.get('duplicate_of_action_id')) != str(canonical_id):
                raise ValueError('duplicate_repair_readback_failed')
        claim_request = {'operation': 'claim_review_create', 'action_id': canonical_id,
                         'conversation_id': canonical['conversation_id'], 'message_id': canonical['message_id'],
                         'expected_updated_at': canonical['updated_at'],
                         'expected_business_key': canonical.get('business_key')}
        claim_preview = writer.execute({**claim_request, 'dry_run': True})
        if (claim_preview.get('ok') is not True or claim_preview.get('committed') is not False
                or claim_preview.get('row', {}).get('status') != 'CLAIMED'):
            raise ValueError('canonical_claim_preview_failed')
        claimed = writer.execute(claim_request)
        claimed_action_id = claimed.get('action_id') or claimed.get('row', {}).get('id')
        if (claimed.get('ok') is not True or claimed.get('committed') is not True
                or claimed.get('row', {}).get('status') != 'CLAIMED'
                or str(claimed_action_id) != str(canonical_id)):
            raise ValueError('canonical_claim_result_unknown')
        check_no_existing_task(dws, query, title, [row['message_id'] for row in sources])
        audit['create_started_at'] = datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(timespec='seconds')
        audit['create_attempted'] = True
        update_audit(conn, batch_id, audit)

        def link_canonical_task(action_id, task_id):
            if int(action_id) != canonical_id:
                raise ValueError('canonical_claim_action_id_mismatch')
            update_audit(conn, batch_id, audit, task_id=task_id)
            linked = writer.execute({'operation': 'link_external_task', 'action_id': canonical_id,
                                     'external_task_id': task_id})
            if linked.get('ok') is not True or linked.get('committed') is not True:
                raise ValueError('canonical_task_link_unknown')

        task_id = create_claimed_task(
            action_id=canonical_id,
            claim_acquired=True,
            create=lambda: dws.create_todo(plan, priority=20),
            link=link_canonical_task,
        )
        return finalize(conn, batch_id, ordinal, profile, bot_id, audit, task_id)
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def main():
    parser = argparse.ArgumentParser()
    for flag in ('db', 'batch', 'profile', 'bot-open-id', 'lock', 'title', 'duplicate-query'):
        parser.add_argument('--' + flag, required=True)
    parser.add_argument('--ordinal', required=True, type=int)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--reconcile', action='store_true')
    args = parser.parse_args()
    try:
        result = resolve(connect(args.db), args.batch, args.ordinal, args.profile, args.bot_open_id,
                         args.lock, args.title, args.duplicate_query, args.apply, args.reconcile)
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except (ValueError, RuntimeError, OSError, json.JSONDecodeError, subprocess.TimeoutExpired) as exc:
        print(json.dumps({'status': 'error', 'reason': str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
