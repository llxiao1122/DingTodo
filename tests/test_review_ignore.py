import sys
import unittest
from datetime import datetime

sys.path.insert(0, '/root/.hermes/polardb_runtime')
from workflows.polardb_work_writer.policy import InvalidWriteRequest, normalize_request
from workflows.polardb_work_writer.writer import WorkWriter, WriteError


class Cursor:
    def __init__(self, row):
        self.row, self.rowcount = row, 0
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def execute(self, sql, params):
        if sql.startswith('UPDATE'):
            self.row['status'] = params[0]
            self.rowcount = 1
    def fetchone(self): return self.row.copy() if self.row else None


class Connection:
    def __init__(self, row): self.row = row
    def cursor(self): return Cursor(self.row)


def action():
    return {'id': 751, 'conversation_id': 'cid', 'message_id': 'msg', 'action_key': 'todo',
            'status': 'REVIEW', 'business_key': 'key', 'external_task_id': None,
            'duplicate_of_action_id': None, 'parent_action_id': None,
            'updated_at': datetime(2026, 10, 29, 1, 1, 38)}


def request():
    return normalize_request({'operation': 'resolve_review_ignore', 'action_id': 751,
                              'conversation_id': 'cid', 'message_id': 'msg',
                              'expected_business_key': 'key',
                              'expected_updated_at': '2026-10-29T01:01:38'})


class ReviewIgnoreTests(unittest.TestCase):
    def test_unchanged_review_becomes_ignored(self):
        conn = Connection(action())
        self.assertEqual(WorkWriter._resolve_review_ignore(conn, request())['row']['status'], 'IGNORED')

    def test_unchanged_review_can_be_claimed_once(self):
        conn = Connection(action())
        self.assertEqual(WorkWriter._resolve_review_action(conn, request(), 'CLAIMED')['row']['status'], 'CLAIMED')
        with self.assertRaisesRegex(WriteError, 'review_action_changed_or_linked'):
            WorkWriter._resolve_review_action(conn, request(), 'CLAIMED')

    def test_changed_or_linked_review_is_not_modified(self):
        for field, value in [('status', 'CLAIMED'), ('external_task_id', 'task'),
                             ('duplicate_of_action_id', 345), ('business_key', 'other'),
                             ('updated_at', datetime(2026, 10, 29, 1, 2)),
                             ('message_id', 'other')]:
            with self.subTest(field=field):
                row = {**action(), field: value}
                with self.assertRaisesRegex(WriteError, 'review_action_changed_or_linked'):
                    WorkWriter._resolve_review_ignore(Connection(row), request())
                self.assertEqual(row[field], value)

    def test_unexpected_fields_rejected(self):
        with self.assertRaises(InvalidWriteRequest):
            normalize_request({'operation': 'resolve_review_ignore', 'action_id': 751,
                               'conversation_id': 'cid', 'message_id': 'msg',
                               'expected_updated_at': '2026-10-29T01:01:38', 'sql': 'bad'})


if __name__ == '__main__':
    unittest.main()
