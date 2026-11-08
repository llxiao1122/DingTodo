"""Controlled connection boundary for the authoritative PolarDB work database.

This module contains no memory-provider or Cifra imports.  It only resolves the
host's protected database environment and opens a bounded PyMySQL connection to
agentic.  Callers own the query/write policy; the MCP layer uses this boundary
for SELECT-only operations.
"""
from __future__ import annotations

import os
import shlex
from pathlib import Path
from typing import Any

import pymysql

ENV_FILE = Path("/etc/cipher-work.env")
DATABASE = "agentic"
LOCAL_DATABASE = "agentic_local"
LOCAL_SCHEMA_OPT_IN = "HERMES_WORK_DB_ALLOW_LOCAL"


def read_env_file(path: Path = ENV_FILE) -> dict[str, str]:
    """Read key/value pairs without logging or returning them to user output."""
    values: dict[str, str] = {}
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


def db_config() -> dict[str, Any]:
    """Resolve the protected connection without permitting another database."""
    values = read_env_file()
    values.update({key: value for key, value in os.environ.items() if value})

    def pick(primary: str, legacy: str, default: str = "") -> str:
        return str(values.get(primary) or values.get(legacy) or default)

    config: dict[str, Any] = {
        "host": pick("HERMES_WORK_DB_HOST", "CIPHER_WORK_DB_HOST"),
        "port": int(pick("HERMES_WORK_DB_PORT", "CIPHER_WORK_DB_PORT", "3306")),
        "user": pick("HERMES_WORK_DB_USER", "CIPHER_WORK_DB_USER", ""),
        "password": pick("HERMES_WORK_DB_PASSWORD", "CIPHER_WORK_DB_PASSWORD", ""),
        "database": pick("HERMES_WORK_DB_DATABASE", "CIPHER_WORK_DB_DATABASE", ""),
    }
    # Preserve the existing controlled deployment's legacy roster names.
    if not config["host"]:
        config["host"] = pick("ROSTER_POLARDB_HOST", "ROSTER_POLARDB_HOST")
    if not config["user"]:
        config["user"] = pick("ROSTER_POLARDB_USER", "ROSTER_POLARDB_USER")
    if not config["password"]:
        config["password"] = pick("ROSTER_POLARDB_PASSWORD", "ROSTER_POLARDB_PASSWORD")
    if not config["database"]:
        config["database"] = pick("ROSTER_POLARDB_DATABASE", "ROSTER_POLARDB_DATABASE")
    if config["database"] == LOCAL_DATABASE:
        if values.get(LOCAL_SCHEMA_OPT_IN) != "1" or config["host"].lower() not in {"localhost", "127.0.0.1", "::1"}:
            raise RuntimeError("refusing database outside controlled local allowlist")
    elif config["database"] != DATABASE:
        raise RuntimeError("refusing database outside agentic")
    if not config["host"] or not config["user"]:
        raise RuntimeError("missing protected PolarDB connection settings")
    return config


def connect():
    """Open a bounded connection for the neutral Work Adapter."""
    return pymysql.connect(
        **db_config(),
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=8,
        read_timeout=15,
        write_timeout=15,
        autocommit=True,
    )
