"""Render a systemd override while preserving the existing service arguments."""
from __future__ import annotations

import re
from pathlib import Path


def render(existing: str, bundle: Path) -> str:
    lines = existing.splitlines()
    directories = [line[len('WorkingDirectory='):] for line in lines if line.startswith('WorkingDirectory=')]
    starts = [line[len('ExecStart='):] for line in lines if line.startswith('ExecStart=') and line != 'ExecStart=']
    if len(directories) != 1 or len(starts) != 1:
        raise ValueError('unsupported_service_override')
    old = directories[0]
    bundle_path = str(bundle)
    if (not re.fullmatch(r'/[A-Za-z0-9_./-]+', old)
            or not re.fullmatch(r'/[A-Za-z0-9_./-]+', bundle_path)
            or '..' in Path(old).parts or '..' in Path(bundle_path).parts):
        raise ValueError('unsafe_service_paths')
    code, runtime = bundle_path + '/code', bundle_path + '/runtime'
    old_path_count = starts[0].count(old + '/')
    if old_path_count == 1:
        working_directory = code
        start = starts[0].replace(old + '/', code + '/', 1)
    elif old_path_count == 0:
        script_paths = re.findall(r'(?<!\S)(/[A-Za-z0-9_./-]+\.py)(?=\s|$)', starts[0])
        if len(script_paths) != 1:
            raise ValueError('unsafe_service_paths')
        script_path = Path(script_paths[0])
        releases_root = Path('/root/.hermes/releases')
        try:
            script_path.relative_to(releases_root)
        except ValueError:
            raise ValueError('unsafe_service_paths') from None
        if (not script_path.is_absolute() or str(script_path) != script_paths[0]
                or '..' in script_path.parts):
            raise ValueError('unsafe_service_paths')
        working_directory = old
        start = starts[0].replace(script_paths[0], code + '/' + script_path.name, 1)
    else:
        raise ValueError('unsafe_service_paths')
    return ('[Service]\nEnvironment="DINGTODO_ASSIGN_SELF_ONLY=1"\n'
            'Environment="PYTHONDONTWRITEBYTECODE=1"\n'
            f'Environment="HERMES_POLARDB_RUNTIME={runtime}"\n'
            f'WorkingDirectory={working_directory}\nExecStart=\nExecStart={start}\n')
