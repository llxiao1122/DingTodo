from pathlib import Path

import pytest
from deploy.render_switch import render


def test_switch_keeps_service_arguments_and_identity():
    existing = '[Service]\nWorkingDirectory=/old/release\nExecStart=\nExecStart=/usr/bin/python3 /old/release/worker.py --profile exact --flag\n'
    output = render(existing, Path('/new/bundle'))
    assert 'ExecStart=/usr/bin/python3 /new/bundle/code/worker.py --profile exact --flag' in output
    assert 'WorkingDirectory=/new/bundle/code' in output
    assert 'HERMES_POLARDB_RUNTIME=/new/bundle/runtime' in output
    with pytest.raises(ValueError):
        render(existing, Path('/new/../escape'))
    with pytest.raises(ValueError):
        render(existing.replace('/old/release/worker.py', '/different/worker.py'), Path('/new/bundle'))


def test_switch_preserves_existing_working_directory_when_script_uses_another_release():
    existing = (
        '[Service]\n'
        'WorkingDirectory=/root/.hermes/releases/dingtodo-repair-20261104\n'
        'ExecStart=\n'
        'ExecStart=/usr/bin/python3 '
        '/root/.hermes/releases/dingtodo-dws-im-switch-5eecf23/polling_resident.py '
        '--state-dir /root/.hermes/dingtalk-listener-state --flag\n'
    )

    output = render(existing, Path('/root/.hermes/releases/dingtodo-dws-skills-037e005'))

    assert 'WorkingDirectory=/root/.hermes/releases/dingtodo-repair-20261104' in output
    assert (
        'ExecStart=/usr/bin/python3 '
        '/root/.hermes/releases/dingtodo-dws-skills-037e005/code/polling_resident.py '
        '--state-dir /root/.hermes/dingtalk-listener-state --flag'
    ) in output
    with pytest.raises(ValueError):
        render(existing.replace(
            '/root/.hermes/releases/dingtodo-dws-im-switch-5eecf23/polling_resident.py',
            '/tmp/local/polling_resident.py',
        ), Path('/root/.hermes/releases/dingtodo-dws-skills-037e005'))
