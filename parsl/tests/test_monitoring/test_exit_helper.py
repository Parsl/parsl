import multiprocessing
import signal

import psutil
import pytest

from parsl.multiprocessing import SpawnEvent, SpawnProcess, join_terminate_close_proc


def noop():
    pass


@pytest.mark.local
def test_end_process_already_exited():
    p = SpawnProcess(target=noop)
    p.start()
    p.join()
    join_terminate_close_proc(p)


def hang():
    while True:
        pass


@pytest.mark.local
def test_end_hung_process():
    """Test calling against a process that will not exit itself."""
    p = SpawnProcess(target=hang)
    p.start()
    pid = p.pid
    join_terminate_close_proc(p, timeout=1)
    assert not psutil.pid_exists(pid), "process should not exist any more"


def hang_no_sigint(e):
    def s(*args, **kwargs):
        e.set()
    signal.signal(signal.SIGTERM, s)
    while True:
        pass


@pytest.mark.local
def test_end_hung_process_no_sigint():
    """Test calling against a process that will not exit itself."""
    e = SpawnEvent()
    p = SpawnProcess(target=hang_no_sigint, args=(e,))
    p.start()
    pid = p.pid
    join_terminate_close_proc(p, timeout=2)
    assert not psutil.pid_exists(pid), "process should not exist any more"
    assert e.is_set(), "hung process should have set event on signal"
