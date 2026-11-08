import unittest
from unittest.mock import Mock

from todo_execution import TodoExecutionError, add_verified_source_comment, create_claimed_task
from event_worker import EventProcessor


class TodoExecutionTests(unittest.TestCase):
    def test_unconfirmed_link_does_not_update_in_memory_state(self):
        writer = Mock()
        writer.execute.return_value = {'ok': False, 'committed': False}
        processor = EventProcessor(
            {'config_group': [], 'config_staff': [], 'active_routing_loaded': True},
            {'name': 'test', 'user_id': 'test'}, mode='production', writer=writer,
            existing_actions=[{'action_id': 1, 'status': 'CLAIMED'}],
        )
        with self.assertRaisesRegex(RuntimeError, 'external_task_link_unconfirmed'):
            processor._persist_created_task({'action_id': 1}, 'task-1')
        self.assertEqual(processor.existing_actions[0]['status'], 'CLAIMED')

    def test_unconfirmed_ledger_write_never_calls_remote_create(self):
        writer = Mock()
        writer.execute.return_value = {"ok": False, "committed": False}
        dws = Mock()
        processor = EventProcessor(
            {"config_group": [], "config_staff": [], "active_routing_loaded": True},
            {"name": "test", "user_id": "test"}, mode="production", writer=writer, dws=dws,
        )
        with self.assertRaisesRegex(RuntimeError, "action_status_write_unconfirmed"):
            processor._write_external(
                {"conversation_id": "c", "message_id": "m"}, {"comment": "source"},
                action_id=1, claim_acquired=True, test_only=False, on_created=lambda _: None,
            )
        dws.create_todo.assert_not_called()

    def test_missing_claim_never_creates(self):
        create = Mock()
        with self.assertRaises(TodoExecutionError):
            create_claimed_task(action_id=None, claim_acquired=False, create=create, link=Mock())
        create.assert_not_called()

    def test_link_failure_preserves_returned_task_id(self):
        link = Mock(side_effect=RuntimeError('database unavailable'))
        with self.assertRaises(TodoExecutionError) as error:
            create_claimed_task(action_id=42, claim_acquired=True,
                                create=lambda: {'task_id': 'task-42'}, link=link)
        self.assertEqual(error.exception.task_id, 'task-42')
        link.assert_called_once_with('42', 'task-42')

    def test_unknown_comment_outcome_does_not_write_again(self):
        dws = Mock()
        dws.list_task_comments.return_value = []
        with self.assertRaisesRegex(TodoExecutionError, 'source_comment_outcome_unknown'):
            add_verified_source_comment(dws, 'task-42', 'source', already_attempted=True)
        dws.add_source_comment.assert_not_called()

    def test_existing_comment_is_verified_without_second_write(self):
        dws = Mock()
        dws.list_task_comments.return_value = [{'content': 'source'}]
        self.assertFalse(add_verified_source_comment(dws, 'task-42', 'source', already_attempted=True))
        dws.verify_source_comment.assert_called_once_with('task-42', 'source')
        dws.add_source_comment.assert_not_called()
