"""Shared, privacy-safe invocation primitives for the DWS CLI."""

from __future__ import annotations

import json
import subprocess
from collections.abc import Mapping, Sequence
from typing import Any, Callable


class DwsInvocationError(RuntimeError):
    """A DWS response could not be decoded or the CLI returned a failure."""

    def __init__(self, reason: str, *, returncode: int | None = None):
        super().__init__(reason)
        self.reason = reason
        self.returncode = returncode


def build_dws_argv(
    arguments: Sequence[str],
    *,
    executable: str = "dws",
    json_output: bool = False,
) -> list[str]:
    """Build one validated DWS command, adding JSON output exactly once."""
    if not isinstance(executable, str) or not executable.strip():
        raise ValueError("dws_executable_required")
    if isinstance(arguments, (str, bytes)) or not isinstance(arguments, Sequence):
        raise TypeError("dws_arguments_must_be_a_sequence")
    if any(not isinstance(argument, str) for argument in arguments):
        raise TypeError("dws_arguments_must_be_strings")

    argv = [executable, *arguments]
    if json_output:
        format_indexes = [
            index for index, argument in enumerate(argv[1:], start=1)
            if argument in {"--format", "-f"}
        ]
        if format_indexes:
            if len(format_indexes) != 1 or format_indexes[0] + 1 >= len(argv):
                raise ValueError("dws_output_format_invalid")
            if argv[format_indexes[0] + 1] != "json":
                raise ValueError("dws_json_output_required")
        else:
            argv.extend(("--format", "json"))
    return argv


def run_dws_json(
    arguments: Sequence[str],
    *,
    executable: str = "dws",
    timeout: float = 60,
    cwd: str | None = None,
    env: Mapping[str, str] | None = None,
    input_text: str | None = None,
) -> Any:
    """Run a bounded JSON-mode DWS command without exposing provider output."""
    argv = build_dws_argv(arguments, executable=executable, json_output=True)
    options: dict[str, Any] = {
        "capture_output": True,
        "text": True,
        "timeout": timeout,
        "cwd": cwd,
        "env": dict(env) if env is not None else None,
        "check": False,
    }
    if input_text is not None:
        options["input"] = input_text
    completed = subprocess.run(argv, **options)
    raw = completed.stdout or completed.stderr
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        raise DwsInvocationError("dws_non_json_response", returncode=completed.returncode) from None
    if completed.returncode != 0:
        raise DwsInvocationError("dws_command_failed", returncode=completed.returncode)
    return payload


def run_dws_json_object(
    arguments: Sequence[str],
    *,
    executable: str = "dws",
    timeout: float = 60,
    invalid_json_reason: str = "dws_non_json_response",
    command_failure_reason: str = "dws_command_failed",
    invalid_object_reason: str = "dws_response_not_object",
    cwd: str | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Run a JSON command that requires an object response with caller-safe errors."""
    try:
        payload = run_dws_json(
            arguments,
            executable=executable,
            timeout=timeout,
            cwd=cwd,
            env=env,
        )
    except DwsInvocationError as exc:
        reason = (
            invalid_json_reason
            if exc.reason == "dws_non_json_response"
            else command_failure_reason
        )
        raise DwsInvocationError(reason, returncode=exc.returncode) from None

    if not isinstance(payload, dict):
        raise DwsInvocationError(invalid_object_reason)
    return payload


def run_dws_command(
    arguments: Sequence[str],
    *,
    executable: str = "dws",
    env: Mapping[str, str] | None = None,
    **kwargs: Any,
) -> subprocess.CompletedProcess[str]:
    """Run a non-JSON DWS command through the same subprocess boundary."""
    argv = build_dws_argv(arguments, executable=executable)
    if env is not None:
        kwargs["env"] = dict(env)
    return subprocess.run(argv, check=False, **kwargs)


def start_dws_stream(
    arguments: Sequence[str],
    *,
    executable: str = "dws",
    env: Mapping[str, str] | None = None,
    popen_factory: Callable[..., subprocess.Popen[str]] | None = None,
    **kwargs: Any,
) -> subprocess.Popen[str]:
    """Start a long-lived DWS stream while leaving its lifecycle to the caller."""
    argv = build_dws_argv(arguments, executable=executable)
    if env is not None:
        kwargs["env"] = dict(env)
    start = popen_factory or subprocess.Popen
    return start(argv, **kwargs)
