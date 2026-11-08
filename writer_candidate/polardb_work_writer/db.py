"""Separate least-privilege connection boundary for the PolarDB writer."""

from __future__ import annotations

import os
import shlex
from pathlib import Path
from typing import Any

import pymysql


DATABASE = "agentic"
LOCAL_DATABASE = "agentic_local"
LOCAL_SCHEMA_OPT_IN = "HERMES_WORK_WRITER_ALLOW_LOCAL"
DEFAULT_ENV_FILE = Path("/etc/cipher-work-writer.env")


def _read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        parsed = shlex.split(value, comments=True)
        values[key.strip()] = parsed[0] if parsed else ""
    return values


def writer_db_config() -> dict[str, Any]:
    env_path = Path(os.environ.get("HERMES_WORK_WRITER_ENV", str(DEFAULT_ENV_FILE)))
    values = _read_env(env_path)
    values.update({key: value for key, value in os.environ.items() if value})

    def pick(*names: str, default: str = "") -> str:
        for name in names:
            if values.get(name):
                return str(values[name])
        return default

    config: dict[str, Any] = {
        "host": pick(
            "HERMES_WORK_WRITER_DB_HOST", "HERMES_WORK_DB_HOST", "CIPHER_WORK_DB_HOST", "ROSTER_POLARDB_HOST"
        ),
        "port": int(pick("HERMES_WORK_WRITER_DB_PORT", "HERMES_WORK_DB_PORT", "CIPHER_WORK_DB_PORT", default="3306")),
        "user": pick("HERMES_WORK_WRITER_DB_USER", "HERMES_WORK_DB_USER", "CIPHER_WORK_DB_USER", "ROSTER_POLARDB_USER"),
        "password": pick("HERMES_WORK_WRITER_DB_PASSWORD", "HERMES_WORK_DB_PASSWORD", "CIPHER_WORK_DB_PASSWORD", "ROSTER_POLARDB_PASSWORD"),
        "database": pick("HERMES_WORK_WRITER_DB_DATABASE", "HERMES_WORK_DB_DATABASE", "CIPHER_WORK_DB_DATABASE", "ROSTER_POLARDB_DATABASE", default=DATABASE),
    }
    if config["database"] == LOCAL_DATABASE:
        if values.get(LOCAL_SCHEMA_OPT_IN) != "1" or config["host"].lower() not in {"localhost", "127.0.0.1", "::1"}:
            raise RuntimeError("writer_database_outside_controlled_local_allowlist")
    elif config["database"] != DATABASE:
        raise RuntimeError("writer_database_not_allowed")
    if not config["host"] or not config["user"]:
        raise RuntimeError("writer_credentials_unavailable")
    return config


def connect_writer():
    return pymysql.connect(
        **writer_db_config(),
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=8,
        read_timeout=15,
        write_timeout=15,
        autocommit=False,
    )
