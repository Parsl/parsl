import logging
import os
import pathlib
import random
import shutil
import socket
import subprocess
import tempfile
import threading
import time

import pytest

from parsl.jobs.states import JobState
from parsl.launchers import SingleNodeLauncher
from parsl.providers import LocalProvider

logger = logging.getLogger(__name__)


def _run_tests(p: LocalProvider):
    status = _run(p, '/bin/true')
    assert status.message is None
    assert status.state == JobState.COMPLETED
    assert status.exit_code == 0
    assert status.stdout == ''
    assert status.stderr == ''

    status = _run(p, '/bin/true', np=2)
    assert status.message is None
    assert status.state == JobState.COMPLETED
    assert status.exit_code == 0
    assert status.stdout == ''
    assert status.stderr == ''

    status = _run(p, '/bin/false')
    assert status.state == JobState.FAILED
    assert status.exit_code != 0
    assert status.stdout == ''
    assert status.stderr == ''

    status = _run(p, '/bin/false', np=2)
    assert status.state == JobState.FAILED
    assert status.exit_code != 0
    assert status.stdout == ''
    assert status.stderr == ''

    status = _run(p, '/bin/echo -n magic')
    assert status.state == JobState.COMPLETED
    assert status.exit_code == 0
    assert status.stdout == 'magic'
    assert status.stderr == ''

    status = _run(p, '/bin/echo -n magic 1>&2')
    assert status.state == JobState.COMPLETED
    assert status.exit_code == 0
    assert status.stdout == ''
    assert status.stderr == 'magic'


@pytest.mark.local
def test_local_channel():
    with tempfile.TemporaryDirectory() as script_dir:
        script_dir = tempfile.mkdtemp()
        p = LocalProvider(launcher=SingleNodeLauncher(debug=False))
        p.script_dir = script_dir
        _run_tests(p)


def _run(p: LocalProvider, command: str, np: int = 1):
    id = p.submit(command, np)
    return _wait(p, id)


def _wait(p: LocalProvider, id: object):
    status = p.status([id])[0]
    while not status.terminal:
        time.sleep(0.1)
        status = p.status([id])[0]
    return status
