"""Durable local receipts for manual todo creation and reconciliation."""
from __future__ import annotations

import os
import sqlite3
import stat
from pathlib import Path


class ManualReceiptJournal:
    def __init__(self, path: str):
        target = Path(path)
        if target.is_symlink():
            raise ValueError('receipt_path_is_symlink')
        fd = os.open(target, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(fd)
            parent = target.parent.stat()
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                    or stat.S_IMODE(info.st_mode) & 0o077
                    or parent.st_uid != os.geteuid() or stat.S_IMODE(parent.st_mode) & 0o022):
                raise ValueError('receipt_file_permissions_unsafe')
        finally:
            os.close(fd)
        self.conn = sqlite3.connect(str(target), isolation_level=None, timeout=30)
        self.conn.execute('PRAGMA synchronous=FULL')
        self.conn.execute('CREATE TABLE IF NOT EXISTS receipt ('
                          'action_id INTEGER PRIMARY KEY, task_id TEXT NOT NULL UNIQUE)')

    def record(self, action_id: int, task_id: str) -> None:
        if (isinstance(action_id, bool) or not isinstance(action_id, int) or action_id <= 0
                or not isinstance(task_id, str) or not task_id.strip() or task_id != task_id.strip()):
            raise ValueError('invalid_receipt')
        self.conn.execute('BEGIN IMMEDIATE')
        try:
            existing = self.conn.execute('SELECT task_id FROM receipt WHERE action_id=?', (action_id,)).fetchone()
            if existing and existing[0] != task_id:
                raise ValueError('receipt_task_conflict')
            if not existing:
                self.conn.execute('INSERT INTO receipt VALUES (?, ?)', (action_id, task_id))
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise

    def close(self) -> None:
        self.conn.close()

    def lookup(self, action_id: int) -> str | None:
        row = self.conn.execute('SELECT task_id FROM receipt WHERE action_id=?', (action_id,)).fetchone()
        return str(row[0]) if row else None
