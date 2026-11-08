import fcntl
import subprocess
from unittest.mock import patch

from deploy.switch_preflight import check


def test_preflight_refuses_held_lock_or_running_oneshot(tmp_path):
    lock = tmp_path / 'work.lock'
    lock.touch()
    idle = subprocess.CompletedProcess([], 0, 'inactive\n', '')
    with patch('deploy.switch_preflight.subprocess.run', return_value=idle):
        assert check(lock)['ready']
        with lock.open('r+') as holder:
            fcntl.flock(holder, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = check(lock)
            assert not result['ready']
            assert 'work_lock_held' in result['reasons']
    active = subprocess.CompletedProcess([], 0, 'active\n', '')
    with patch('deploy.switch_preflight.subprocess.run', return_value=active):
        assert not check(lock)['ready']
