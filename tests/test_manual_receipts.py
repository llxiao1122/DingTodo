import sqlite3

import pytest

from manual_receipts import ManualReceiptJournal


def test_receipt_survives_reopen_and_rejects_conflicting_ids(tmp_path):
    path = tmp_path / 'receipts.sqlite3'
    journal = ManualReceiptJournal(str(path))
    journal.record(1, 'task-one')
    journal.record(1, 'task-one')
    with pytest.raises(ValueError, match='receipt_task_conflict'):
        journal.record(1, 'task-two')
    with pytest.raises(sqlite3.IntegrityError):
        journal.record(2, 'task-one')
    journal.close()
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT action_id,task_id FROM receipt').fetchall() == [(1, 'task-one')]


def test_symlink_receipt_path_is_rejected(tmp_path):
    target = tmp_path / 'target'
    target.touch()
    link = tmp_path / 'link'
    link.symlink_to(target)
    with pytest.raises(ValueError, match='receipt_path_is_symlink'):
        ManualReceiptJournal(str(link))


def test_permissive_existing_receipt_is_rejected(tmp_path):
    path = tmp_path / 'receipts.sqlite3'
    path.touch(mode=0o644)
    path.chmod(0o644)
    with pytest.raises(ValueError, match='receipt_file_permissions_unsafe'):
        ManualReceiptJournal(str(path))
