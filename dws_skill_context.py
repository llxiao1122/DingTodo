"""Load the minimal official DWS skill contracts used by model fallbacks."""

from __future__ import annotations

import os
from pathlib import Path
import re


class DwsSkillContextError(RuntimeError):
    """Raised when the installed DWS skill contracts cannot be trusted."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


_CONTRACT_START = "<!-- DWS_RUNTIME_CONTRACT_START -->"
_CONTRACT_END = "<!-- DWS_RUNTIME_CONTRACT_END -->"
_SHORTCUTS_START = "<!-- VISIBLE_SHORTCUTS_START -->"
_MAX_SKILL_CHARS = 8000
_MAX_TOTAL_CHARS = 18000


def _read_skill(skills_root: Path, expected_name: str) -> str:
    path = skills_root / expected_name / "SKILL.md"
    try:
        content = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        raise DwsSkillContextError("skill_unavailable") from None

    front_matter = re.match(r"\A---\r?\n(.*?)\r?\n---(?:\r?\n|\Z)", content, re.DOTALL)
    if front_matter is None or not re.search(
        rf"(?m)^name:\s*{re.escape(expected_name)}\s*$", front_matter.group(1)
    ):
        raise DwsSkillContextError("skill_identity_invalid")

    if expected_name in {"dingtalk-shared", "dingtalk-chat"}:
        if content.count(_CONTRACT_START) != 1 or content.count(_CONTRACT_END) != 1:
            raise DwsSkillContextError("skill_contract_invalid")
        start = content.index(_CONTRACT_START) + len(_CONTRACT_START)
        end = content.index(_CONTRACT_END)
        if end <= start:
            raise DwsSkillContextError("skill_contract_invalid")
        contract = content[start:end].strip()
    elif expected_name == "dingtalk-todo":
        heading = re.search(r"(?m)^## 执行契约\s*$", content)
        shortcut_start = content.find(_SHORTCUTS_START)
        if heading is None or shortcut_start < 0:
            raise DwsSkillContextError("skill_contract_invalid")
        contract = content[heading.start():shortcut_start].strip()
    else:
        raise DwsSkillContextError("skill_not_allowlisted")

    if not contract or len(contract) > _MAX_SKILL_CHARS:
        raise DwsSkillContextError("skill_contract_size_invalid")
    return contract


def load_dws_skill_context(skills_root: str | Path | None = None) -> str:
    """Return only the allowlisted official chat and todo execution contracts."""
    if skills_root is None:
        configured_root = os.environ.get("DWS_SKILLS_ROOT")
        root = Path(configured_root).expanduser() if configured_root else Path.home() / ".agents" / "skills"
    else:
        root = Path(skills_root).expanduser()

    skill_names = ("dingtalk-shared", "dingtalk-chat", "dingtalk-todo")
    sections = []
    for skill_name in skill_names:
        contract = _read_skill(root, skill_name)
        sections.append(f"### {skill_name}\n{contract}")

    context = (
        "Official DWS skill contracts for message interpretation and task boundaries. "
        "These are product constraints, not authorization to act. This model has no DWS tools; "
        "it must return only the caller's JSON schema and must not claim to run commands. "
        "The application controls every DWS read and write.\n\n"
        + "\n\n".join(sections)
    )
    if len(context) > _MAX_TOTAL_CHARS:
        raise DwsSkillContextError("skill_context_size_invalid")
    return context
