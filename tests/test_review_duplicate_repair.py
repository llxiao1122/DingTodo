import copy
import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, '/root/.hermes/polardb_runtime')
from workflows.polardb_work_writer.policy import normalize_request
from workflows.polardb_work_writer.writer import WorkWriter, WriteError


def row(action_id, conversation, message, duplicate, updated):
    return {'id': action_id, 'conversation_id': conversation, 'message_id': message,
            'action_key': 'todo', 'status': 'REVIEW', 'business_key': None,
            'external_task_id': None, 'duplicate_of_action_id': duplicate,
            'parent_action_id': None, 'updated_at': datetime.fromisoformat(updated)}


def snapshot(value):
    return {'action_id': value['id'], 'conversation_id': value['conversation_id'],
            'message_id': value['message_id'], 'action_key': value['action_key'],
            'status': value['status'], 'business_key': value['business_key'],
            'external_task_id': value['external_task_id'],
            'duplicate_of_action_id': value['duplicate_of_action_id'],
            'parent_action_id': value['parent_action_id'],
            'updated_at': value['updated_at'].isoformat()}


def request(rows):
    return {'operation': 'repair_review_duplicate_family', 'canonical_action_id': 742,
            'old_canonical_action_id': 345, 'duplicate_action_ids': [743, 744],
            'expected_rows': [snapshot(rows[key]) for key in (345, 742, 743, 744)]}


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.rowcount = 0
        self.result = None
    def __enter__(self):
        return self
    def __exit__(self, *args):
        return False
    def execute(self, sql, params):
        compact = ' '.join(sql.split()).lower()
        if compact.startswith('select id, conversation_id'):
            self.result = copy.deepcopy(self.conn.rows.get(int(params[0])))
            self.rowcount = 1 if self.result else 0
        elif compact.startswith('update message_action set duplicate_of_action_id'):
            target, action_id, old, status = params
            current = self.conn.rows.get(int(action_id))
            changed = bool(current and current['duplicate_of_action_id'] == old
                           and current['status'] == status
                           and current['external_task_id'] is None
                           and current['parent_action_id'] is None)
            if changed:
                current['duplicate_of_action_id'] = target
            self.rowcount = 1 if changed else 0
            self.result = None
        elif compact.startswith('select id, status, external_task_id, duplicate_of_action_id'):
            self.result = copy.deepcopy(self.conn.rows.get(int(params[0])))
            self.rowcount = 1 if self.result else 0
        else:
            raise AssertionError(f'unexpected SQL: {sql}')
    def fetchone(self):
        return self.result


class FakeConnection:
    def __init__(self, rows):
        self.rows = rows
    def cursor(self):
        return FakeCursor(self)
    def commit(self):
        pass
    def rollback(self):
        pass
    def close(self):
        pass


class DuplicateRepairTests(unittest.TestCase):
    def setUp(self):
        self.rows = {
            345: row(345, 'old-conversation', 'old-message', None, '2026-10-10T12:35:40.913605'),
            742: row(742, 'conversation-1', 'message-1', 345, '2026-10-28T06:50:38.438797'),
            743: row(743, 'conversation-2', 'message-2', 345, '2026-10-28T06:50:38.704558'),
            744: row(744, 'conversation-3', 'message-3', 345, '2026-10-28T06:50:38.879506'),
        }

    def test_reparents_only_the_verified_family(self):
        conn = FakeConnection(self.rows)
        writer = WorkWriter(connect_factory=lambda: conn)
        writer._schema_contract_cache = {'schema_supported': True}
        result = writer.execute(request(self.rows))
        self.assertTrue(result['committed'])
        self.assertIsNone(self.rows[742]['duplicate_of_action_id'])
        self.assertEqual(self.rows[743]['duplicate_of_action_id'], 742)
        self.assertEqual(self.rows[744]['duplicate_of_action_id'], 742)
        self.assertIsNone(self.rows[345]['duplicate_of_action_id'])

    def test_stale_snapshot_changes_nothing(self):
        conn = FakeConnection(self.rows)
        writer = WorkWriter(connect_factory=lambda: conn)
        writer._schema_contract_cache = {'schema_supported': True}
        stale = request(self.rows)
        self.rows[743]['updated_at'] = datetime.fromisoformat('2026-10-28T07:00:00')
        with self.assertRaisesRegex(WriteError, 'snapshot_mismatch'):
            writer.execute(stale)
        self.assertEqual(self.rows[742]['duplicate_of_action_id'], 345)
        self.assertEqual(self.rows[743]['duplicate_of_action_id'], 345)

    def test_malformed_family_is_rejected_before_database_access(self):
        malformed = request(self.rows)
        malformed['duplicate_action_ids'] = [743, 743]
        with self.assertRaises(ValueError):
            normalize_request(malformed)


if __name__ == '__main__':
    unittest.main()
