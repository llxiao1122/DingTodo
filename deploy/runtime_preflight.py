"""Read-only comparison of candidate database adapters and deployed runtime."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


def compare(candidate: Path, runtime: Path) -> dict:
    candidate = candidate.resolve(strict=True)
    runtime = runtime.resolve(strict=True)
    differences = []
    checked = 0
    for package in ('polardb_work_adapter', 'polardb_work_writer'):
        staged = candidate / 'writer_candidate' / package
        live = runtime / 'workflows' / package
        if staged.is_symlink() or live.is_symlink() or not staged.is_dir() or not live.is_dir():
            differences.append({'package': package, 'reason': 'package_missing'})
            continue
        if any(p.is_symlink() for p in staged.glob('*.py')) or any(p.is_symlink() for p in live.glob('*.py')):
            differences.append({'package': package, 'reason': 'symlink_forbidden'})
        staged_files = {p.name: p for p in staged.glob('*.py') if p.is_file() and not p.is_symlink()}
        live_files = {p.name: p for p in live.glob('*.py') if p.is_file() and not p.is_symlink()}
        if set(staged_files) != set(live_files):
            differences.append({'package': package, 'reason': 'file_set_mismatch'})
        for name in sorted(staged_files.keys() & live_files.keys()):
            checked += 1
            if hashlib.sha256(staged_files[name].read_bytes()).digest() != hashlib.sha256(live_files[name].read_bytes()).digest():
                differences.append({'package': package, 'file': name, 'reason': 'content_mismatch'})
    pymysql = runtime / 'pymysql' / '__init__.py'
    if not pymysql.is_file() or pymysql.is_symlink():
        differences.append({'package': 'pymysql', 'reason': 'dependency_missing'})
    return {'compatible': not differences, 'files_checked': checked, 'differences': differences}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--runtime', type=Path, required=True)
    args = parser.parse_args()
    result = compare(args.candidate, args.runtime)
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result['compatible'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
