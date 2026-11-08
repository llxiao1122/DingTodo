"""Read-only release preflight. A successful check is not a held deployment lock."""
from __future__ import annotations

import fcntl
import os
import subprocess
from pathlib import Path


ONESHOT_UNITS = ('hermes-dingtalk-daily-recovery.service', 'cifra-review-inbox.service')


def check(lock_path: Path) -> dict:
    result = {'ready': False, 'lock_available': False, 'units': {}, 'reasons': []}
    try:
        fd = os.open(lock_path, os.O_RDWR | os.O_NOFOLLOW)
    except OSError:
        result['reasons'].append('work_lock_unavailable')
        return result
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result['lock_available'] = True
        except BlockingIOError:
            result['reasons'].append('work_lock_held')
        for unit in ONESHOT_UNITS:
            proc = subprocess.run(['systemctl', 'show', unit, '--property=ActiveState', '--value'],
                                  capture_output=True, text=True, timeout=10, check=False)
            state = proc.stdout.strip()
            result['units'][unit] = state
            if proc.returncode or state != 'inactive':
                result['reasons'].append('unit_not_idle:' + unit)
        result['ready'] = not result['reasons']
        return result
    finally:
        if result['lock_available']:
            fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
