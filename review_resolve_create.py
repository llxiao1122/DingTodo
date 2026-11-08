"""Hand an explicit owner-approved REVIEW to the existing task writer once."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from dws_adapter import DwsClient, _epoch_ms, build_create_args, build_update_args, verify_todo
from review_gateway import _load_work_query_adapter
from review_handoff import verified_handoff
from review_identity import validate_owner
from review_inbox import connect
from review_inbox_poll import read_dws
from routing import build_source_comment, extract_due_candidates
from review_date_plan import source_deadline
from todo_execution import add_verified_source_comment, create_claimed_task

sys.path.insert(0, os.environ.get('HERMES_POLARDB_RUNTIME', '/root/.hermes/polardb_runtime'))
from workflows.polardb_work_writer.writer import WorkWriter

_UNSUPPORTED_DEADLINE_CUE = re.compile(
    r'周[一二三四五六日天]|星期[一二三四五六日天]|今晚|下班前|季度|年底|年前|'
    r'\d{1,2}\s*(?:点|时)|截止|截至|最晚'
)


def self_assigned(content, ordinal):
    text = content.replace('，', ',').replace('；', ';').replace('。', '.')
    compact = re.sub(r'\s+', '', text).strip(',;.')
    if ordinal == 1 and re.fullmatch(
            r'(?:请)?(?:创建|建立)(?:一条)?待办(?:提醒我|由我执行|执行人是我|分配给我)(?:,?标注测试用)?',
            compact):
        return True
    matches = list(re.finditer(r'(?:^|[,;\s])([1-9][0-9]*)\s*[,.:、]\s*', text))
    for i, match in enumerate(matches):
        if int(match.group(1)) == ordinal:
            segment = text[match.end():matches[i + 1].start() if i + 1 < len(matches) else len(text)]
            return '执行人是我' in re.sub(r'\s+', '', segment)
    return False


def owner_identity(profile, owner):
    user_id = validate_owner(profile, owner)
    profiles = read_dws(['profile', 'list']).get('profiles') or []
    active = [x for x in profiles if x.get('isOrgCurrent') is True and x.get('profile') == profile]
    if len(active) != 1 or active[0].get('userId') != user_id:
        raise ValueError('owner_profile_mismatch')
    user_name = active[0].get('userName')
    if not isinstance(user_name, str) or not user_name:
        raise ValueError('owner_name_missing')
    response = read_dws(['contact', 'user', 'search', '--query', user_name, '--profile', profile])
    rows = response.get('result')
    if response.get('success') is not True or not isinstance(rows, list):
        raise ValueError('owner_contact_read_incomplete')
    if response.get('hasMore') is True or response.get('has_more') is True:
        raise ValueError('owner_contact_read_incomplete')
    exact = [x for x in rows if isinstance(x, dict) and x.get('name') == user_name]
    if (len(exact) != 1 or exact[0].get('userId') != user_id
            or exact[0].get('openDingTalkId') != owner):
        raise ValueError('owner_contact_not_unique')
    evidence = {'method': 'unique_exact_contact_name', 'user_id': user_id,
                'open_dingtalk_id': owner, 'name': user_name}
    return user_id, user_name, evidence


def _validated_due_iso(value):
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError('due_iso_invalid')
    try:
        build_update_args('validation-only', due_iso=value)
    except ValueError as exc:
        raise ValueError('due_iso_invalid') from exc
    return value


def _due_matches(actual, expected):
    if expected is None:
        return actual in (None, 0, '')
    expected_ms = _epoch_ms(expected)
    actual_ms = _epoch_ms(actual)
    return (expected_ms is not None and actual_ms is not None
            and abs(expected_ms - actual_ms) <= 1000)


def _source_due_iso(source):
    source_data = source.get('source') if isinstance(source, dict) else None
    text = source_data.get('text') if isinstance(source_data, dict) else None
    message_time = source_data.get('message_time') if isinstance(source_data, dict) else None
    if not isinstance(text, str) or not isinstance(message_time, str):
        raise ValueError('due_source_incomplete')
    try:
        parsed_time = datetime.fromisoformat(message_time.strip().replace('Z', '+00:00'))
        china = ZoneInfo('Asia/Shanghai')
        if parsed_time.tzinfo is None:
            parsed_time = parsed_time.replace(tzinfo=china)
        run_day = parsed_time.astimezone(china).date()
        candidates = extract_due_candidates(text, run_day, include_context=True)
    except (ValueError, TypeError) as exc:
        raise ValueError('due_source_unverified') from exc

    roles = {item.get('role') for item in candidates}
    if candidates and not roles.issubset({'deadline', 'effective', 'event', 'historical'}):
        raise ValueError('due_source_role_unverified')
    deadlines = [item for item in candidates if item.get('role') == 'deadline']
    if not deadlines:
        if not candidates and _UNSUPPORTED_DEADLINE_CUE.search(text):
            raise ValueError('due_source_deadline_unsupported')
        return None
    if len(deadlines) != 1:
        raise ValueError('due_source_multiple_deadlines')
    due_iso = source_deadline(text, message_time)
    if due_iso is None:
        raise ValueError('due_source_deadline_ambiguous')
    return _validated_due_iso(due_iso)


def reconcile_existing(conn, batch_id, ordinal, profile, bot_id, task_id, request_json,
                       expected_due_iso=None):
    handoff = verified_handoff(conn, batch_id, profile, bot_id)
    matches = [row for row in handoff['items'] if row['ordinal'] == ordinal]
    if len(matches) != 1 or matches[0]['user_decision'] != 'create' or len(matches[0]['sources']) != 1:
        raise ValueError('reconciliation_decision_not_verified')
    item = matches[0]
    source = item['sources'][0]
    audit = json.loads(request_json)
    if (str(source['action_id']) != str(audit.get('claim', {}).get('action_id'))
            or source['message_id'] != audit.get('source_message_id')
            or source.get('external_task_id') != task_id
            or str(audit.get('executor_user_id') or '') == ''):
        raise ValueError('reconciliation_source_or_task_mismatch')
    owner = conn.execute('SELECT owner_id FROM batch WHERE id=?', (batch_id,)).fetchone()[0]
    user_id, user_name, contact_evidence = owner_identity(profile, owner)
    reply = conn.execute('SELECT content FROM reply WHERE batch_id=?', (batch_id,)).fetchone()
    default_self = os.environ.get('DINGTODO_ASSIGN_SELF_ONLY') == '1'
    if (user_id != audit['executor_user_id'] or not reply
            or (not default_self and not self_assigned(reply[0], ordinal))):
        raise ValueError('reconciliation_executor_not_confirmed')
    title = audit.get('title')
    if not isinstance(title, str) or not title.strip():
        raise ValueError('reconciliation_title_missing')
    due_iso = _validated_due_iso(audit.get('due_iso'))
    source_due_iso = _source_due_iso(source)
    if not _due_matches(due_iso, source_due_iso):
        raise ValueError('reconciliation_due_source_mismatch')
    if expected_due_iso is not None and not _due_matches(_validated_due_iso(expected_due_iso), due_iso):
        raise ValueError('reconciliation_due_binding_mismatch')
    dws = DwsClient(confirmed_official_writes=True)
    detail = dws.get_todo(task_id)
    if str(detail.get('taskId') or detail.get('todoTaskId') or '') != str(task_id):
        raise ValueError('reconciliation_task_identity_mismatch')
    plan = {'decision': 'CREATE', 'title': title, 'executor_ids': [user_id],
            'executor_names': [user_name], 'priority': 20,
            'allow_executor_namespace_fallback': True}
    if due_iso is not None:
        plan['due_iso'] = due_iso
    problems = verify_todo(detail, plan)
    if not _due_matches(detail.get('dueTime') or detail.get('due'), due_iso):
        problems.append('unexpected_due_date')
    if problems:
        raise ValueError('reconciliation_todo_readback_mismatch:' + ','.join(problems))
    event = {'group': source['source']['group_name'], 'sender': source['source']['sender_name'],
             'create_time': source['source']['message_time'], 'message_id': source['message_id'],
             'text': source['source']['text']}
    comment = build_source_comment(event)
    dws.verify_source_comment(task_id, comment)

    adapter = _load_work_query_adapter()
    request = {'view': 'actions', 'conversation_id': source['conversation_id'],
               'message_id': source['message_id'], 'limit': 10}
    before = adapter.query(request)
    action_rows = [row for row in before.get('rows', []) if str(row.get('id')) == str(source['action_id'])]
    if (before.get('has_more') is not False or len(action_rows) != 1
            or action_rows[0].get('status') not in ('EXTERNAL_CREATED', 'SUCCESS')
            or action_rows[0].get('external_task_id') != task_id):
        raise ValueError('reconciliation_ledger_link_mismatch')
    if action_rows[0].get('status') == 'EXTERNAL_CREATED':
        result = WorkWriter().execute({'operation': 'save_action_result',
                                       'action_id': int(source['action_id']), 'status': 'SUCCESS'})
        if result.get('ok') is not True or result.get('committed') is not True:
            raise ValueError('reconciliation_ledger_update_unknown')
    after = adapter.query(request)
    verified = [row for row in after.get('rows', []) if str(row.get('id')) == str(source['action_id'])]
    if (after.get('has_more') is not False or len(verified) != 1
            or verified[0].get('status') != 'SUCCESS'
            or verified[0].get('external_task_id') != task_id):
        raise ValueError('reconciliation_ledger_readback_incomplete')
    audit['verification'] = {'task_id': str(task_id), 'contact_identity': contact_evidence,
                             'remote_executor_names': [user_name],
                             'executor_id_namespace_normalized': True,
                             'source_comment_verified': True}
    changed = conn.execute("UPDATE resolution SET status='applied',request_json=? "
                           "WHERE batch_id=? AND ordinal=? AND status='applying' AND external_task_id=?",
                           (json.dumps(audit, ensure_ascii=False, separators=(',', ':')),
                            batch_id, ordinal, task_id)).rowcount
    if changed != 1:
        raise ValueError('reconciliation_local_state_changed')
    return {'status': 'applied', 'action_id': source['action_id'], 'external_task_id': task_id,
            'executor': user_name, 'executor_identity_verified_by': 'unique_exact_contact_name'}


def check_candidates(dws, query, title, source_message_id):
    rows = dws.search_todos(query)
    if len(rows) > 50:
        raise ValueError('too_many_duplicate_candidates')
    core = re.sub(r'[^\w\u4e00-\u9fff]', '', title)
    fragments = {core[i:i + 4] for i in range(max(0, len(core) - 3))}
    for row in rows:
        task_id = row['taskId']
        detail = dws.get_todo(task_id)
        if str(detail.get('taskId') or detail.get('todoTaskId') or '') != task_id:
            raise ValueError('duplicate_candidate_identity_unverified')
        comments = dws.list_task_comments(task_id)
        if any(source_message_id in json.dumps(comment, ensure_ascii=False) for comment in comments):
            raise ValueError('source_already_linked_to_todo')
        subject = re.sub(r'[^\w\u4e00-\u9fff]', '', str(detail.get('subject') or ''))
        if any(fragment in subject for fragment in fragments):
            raise ValueError('similar_todo_needs_review')
    return len(rows)


def resolve(conn, batch_id, ordinal, profile, bot_id, lock_path, title, query,
            apply=False, reconcile=False, *, due_iso=None):
    due_iso = _validated_due_iso(due_iso)
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        prior = conn.execute('SELECT status,external_task_id,request_json FROM resolution WHERE batch_id=? AND ordinal=?',
                             (batch_id, ordinal)).fetchone()
        if prior:
            if reconcile and prior[0] == 'applying' and prior[1]:
                return reconcile_existing(conn, batch_id, ordinal, profile, bot_id, prior[1], prior[2],
                                          expected_due_iso=due_iso)
            return {'status': 'already_recorded_needs_reconciliation', 'resolution_status': prior[0],
                    'external_task_id': prior[1]}
        if reconcile:
            raise ValueError('no_existing_create_to_reconcile')
        handoff = verified_handoff(conn, batch_id, profile, bot_id)
        items = [row for row in handoff['items'] if row['ordinal'] == ordinal]
        if len(items) != 1 or items[0]['user_decision'] != 'create':
            raise ValueError('create_decision_not_verified')
        item = items[0]
        if item['handoff_state'] != 'ready_for_existing_worker_review' or len(item['sources']) != 1:
            raise ValueError('review_action_not_safe_to_resolve')
        reply = conn.execute('SELECT content FROM reply WHERE batch_id=?', (batch_id,)).fetchone()
        default_self = os.environ.get('DINGTODO_ASSIGN_SELF_ONLY') == '1'
        if not reply or (not default_self and not self_assigned(reply[0], ordinal)):
            raise ValueError('executor_not_explicitly_confirmed')
        source = item['sources'][0]
        source_due_iso = _source_due_iso(source)
        if not _due_matches(due_iso, source_due_iso):
            raise ValueError('due_iso_source_mismatch')
        due_iso = source_due_iso
        owner = conn.execute('SELECT owner_id FROM batch WHERE id=?', (batch_id,)).fetchone()[0]
        user_id, user_name, contact_evidence = owner_identity(profile, owner)
        if not isinstance(title, str) or not 1 <= len(title.strip()) <= 100 or not query or query not in title or query not in source['source']['text']:
            raise ValueError('title_or_duplicate_query_invalid')
        plan = {'decision': 'CREATE', 'title': title.strip(), 'executor_ids': [user_id],
                'executor_names': [user_name], 'priority': 20,
                'allow_executor_namespace_fallback': True}
        if due_iso is not None:
            plan['due_iso'] = due_iso
        event = {'group': source['source']['group_name'], 'sender': source['source']['sender_name'],
                 'create_time': source['source']['message_time'], 'message_id': source['message_id'],
                 'text': source['source']['text']}
        comment = build_source_comment(event)
        dws = DwsClient(confirmed_official_writes=True)
        candidate_count = check_candidates(dws, query, title, source['message_id'])
        request = {'operation': 'claim_review_create', 'action_id': int(source['action_id']),
                   'conversation_id': source['conversation_id'], 'message_id': source['message_id'],
                   'expected_updated_at': source['updated_at'], 'expected_business_key': source['business_key']}
        writer = WorkWriter()
        writer_preview = writer.execute({**request, 'dry_run': True})
        if writer_preview.get('ok') is not True or writer_preview.get('committed') is not False or writer_preview.get('row', {}).get('status') != 'CLAIMED':
            raise ValueError('writer_preview_failed')
        args = build_create_args(title=title, executor_ids=[user_id], due_iso=due_iso, priority=20)
        preview = dws.run([*args, '--dry-run'])
        create = preview.get('arguments', {}).get('PersonalTodoCreateVO', {})
        preview_due = create.get('dueTime')
        if (preview.get('dry_run') is not True or preview.get('executed') is not False
                or create.get('subject') != title or create.get('executorIds') != [user_id]
                or create.get('priority') != 20 or not _due_matches(preview_due, due_iso)):
            raise ValueError('todo_preview_mismatch')
        if not apply:
            return {'status': 'preview', 'action_id': source['action_id'], 'title': title,
                    'executor': user_name, 'due': None, 'source_deadline': '原消息中的相对日期和下班前',
                    'duplicate_candidates_checked': candidate_count}
        conn.execute('BEGIN IMMEDIATE')
        try:
            audit = {'claim': request, 'title': title, 'due_iso': due_iso,
                     'executor_user_id': user_id,
                     'source_message_id': source['message_id'], 'reply_message_id': handoff['reply_message_id'],
                     'contact_identity': contact_evidence}
            conn.execute('INSERT INTO resolution (batch_id,ordinal,action_id,decision,reply_message_id,request_json,status) VALUES (?,?,?,?,?,?,?)',
                         (batch_id, ordinal, source['action_id'], 'create', handoff['reply_message_id'],
                          json.dumps(audit, ensure_ascii=False, separators=(',', ':')), 'applying'))
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        claimed = writer.execute(request)
        claimed_action_id = claimed.get('action_id') or claimed.get('row', {}).get('id')
        if (claimed.get('ok') is not True or claimed.get('committed') is not True
                or claimed.get('row', {}).get('status') != 'CLAIMED'
                or str(claimed_action_id) != str(source['action_id'])):
            raise ValueError('claim_result_unknown')
        audit['create_attempted'] = True
        changed = conn.execute(
            'UPDATE resolution SET request_json=? '
            'WHERE batch_id=? AND ordinal=? AND status=? AND external_task_id IS NULL',
            (json.dumps(audit, ensure_ascii=False, separators=(',', ':')),
             batch_id, ordinal, 'applying'),
        ).rowcount
        if changed != 1:
            raise ValueError('review_create_attempt_state_changed')

        def link_review_task(action_id, task_id):
            if str(action_id) != str(source['action_id']):
                raise ValueError('review_claim_action_id_mismatch')
            changed = conn.execute(
                'UPDATE resolution SET external_task_id=? '
                'WHERE batch_id=? AND ordinal=? AND status=? AND external_task_id IS NULL',
                (task_id, batch_id, ordinal, 'applying'),
            ).rowcount
            if changed != 1:
                raise ValueError('review_task_link_local_state_changed')
            linked = writer.execute({
                'operation': 'link_external_task', 'action_id': int(action_id),
                'external_task_id': task_id,
            })
            if linked.get('ok') is not True or linked.get('committed') is not True:
                raise ValueError('review_task_link_unknown')

        task_id = create_claimed_task(
            action_id=source['action_id'],
            claim_acquired=True,
            create=lambda: dws.create_todo(plan, priority=20),
            link=link_review_task,
        )

        def save_comment_audit(**values):
            audit.update(values)
            changed = conn.execute(
                'UPDATE resolution SET request_json=? '
                'WHERE batch_id=? AND ordinal=? AND status=? AND external_task_id=?',
                (json.dumps(audit, ensure_ascii=False, separators=(',', ':')),
                 batch_id, ordinal, 'applying', task_id),
            ).rowcount
            if changed != 1:
                raise ValueError('review_comment_audit_state_changed')

        add_verified_source_comment(
            dws,
            task_id,
            comment,
            check_existing=False,
            on_attempt=lambda: save_comment_audit(source_comment_write_started=True),
            on_verified=lambda: save_comment_audit(source_comment_verified=True),
        )
        detail = dws.get_todo(task_id)
        problems = verify_todo(detail, plan)
        if not _due_matches(detail.get('dueTime') or detail.get('due'), due_iso):
            problems.append('unexpected_due_date')
        if problems:
            raise ValueError('todo_readback_mismatch:' + ','.join(problems))
        if plan.get('identity_fallback_evidence'):
            row = conn.execute('SELECT request_json FROM resolution WHERE batch_id=? AND ordinal=? AND status=?',
                               (batch_id, ordinal, 'applying')).fetchone()
            evidence = json.loads(row[0])
            evidence['executor_identity_fallback'] = plan['identity_fallback_evidence']
            conn.execute('UPDATE resolution SET request_json=? WHERE batch_id=? AND ordinal=? AND status=?',
                         (json.dumps(evidence, ensure_ascii=False, separators=(',', ':')),
                          batch_id, ordinal, 'applying'))
        writer.execute({'operation': 'save_action_result', 'action_id': int(source['action_id']), 'status': 'SUCCESS'})
        adapter = _load_work_query_adapter()
        readback = adapter.query({'view': 'actions', 'conversation_id': source['conversation_id'],
                                  'message_id': source['message_id'], 'limit': 10})
        actual = [row for row in readback.get('rows', []) if str(row.get('id')) == source['action_id']]
        if (readback.get('has_more') is not False or len(actual) != 1 or actual[0].get('status') != 'SUCCESS'
                or actual[0].get('external_task_id') != task_id):
            raise ValueError('authoritative_link_readback_incomplete')
        conn.execute("UPDATE resolution SET status='applied' WHERE batch_id=? AND ordinal=? AND status='applying'",
                     (batch_id, ordinal))
        return {'status': 'applied', 'action_id': source['action_id'], 'external_task_id': task_id}
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
    parser.add_argument('--due-iso')
    args = parser.parse_args()
    try:
        print(json.dumps(resolve(connect(args.db), args.batch, args.ordinal, args.profile, args.bot_open_id,
                                 args.lock, args.title, args.duplicate_query, args.apply, args.reconcile,
                                 due_iso=args.due_iso), ensure_ascii=False))
        return 0
    except (ValueError, RuntimeError, OSError, json.JSONDecodeError, subprocess.TimeoutExpired) as exc:
        print(json.dumps({'status': 'error', 'reason': str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
