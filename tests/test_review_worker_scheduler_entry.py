import sys
from unittest.mock import patch

import review_inbox_worker


def test_scheduler_is_opt_in_and_inactive_scheduler_keeps_inbox_running(tmp_path, capsys):
    argv = ['worker', '--db', str(tmp_path / 'inbox.db'), '--profile', 'p',
            '--bot-open-id', 'bot', '--app-id', 'app', '--scheduler-owner-open-id', 'owner']
    with patch.object(sys, 'argv', argv), \
         patch('review_scheduler_runner.run', return_value={'status': 'scheduler_not_enabled'}) as scheduler, \
         patch.object(review_inbox_worker, 'run', return_value={'status': 'idle'}) as inbox:
        assert review_inbox_worker.main() == 0
        scheduler.assert_called_once()
        inbox.assert_called_once()
        assert 'idle' in capsys.readouterr().out


def test_scheduler_option_requires_app_identity(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv('DINGTODO_REVIEW_APP_ID', raising=False)
    argv = ['worker', '--db', str(tmp_path / 'inbox.db'), '--profile', 'p',
            '--bot-open-id', 'bot', '--scheduler-owner-open-id', 'owner']
    with patch.object(sys, 'argv', argv), patch.object(review_inbox_worker, 'run') as inbox:
        assert review_inbox_worker.main() == 2
        inbox.assert_not_called()
        assert 'cifra_app_id_not_configured' in capsys.readouterr().err
