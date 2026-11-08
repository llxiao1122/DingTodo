#!/usr/bin/env python3
"""Apply the additive reporting views, and nothing else."""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from workflows.polardb_work_adapter.db import connect  # noqa: E402

MIGRATION = REPO_ROOT / "workflows/hermes_task_processor/migrations/005_reporting_provenance_views.sql"


def statements() -> list[str]:
    parts = MIGRATION.read_text(encoding="utf-8").split(";\n")
    return [part.strip() for part in parts if part.strip()]


def main() -> int:
    conn = connect()
    try:
        with conn.cursor() as cur:
            for index, statement in enumerate(statements(), start=1):
                cur.execute(statement)
                print({"statement": index, "kind": "CREATE_OR_REPLACE_VIEW", "result": "ok"})
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
