import multiprocessing
import signal

import psutil
import pytest

from parsl.monitoring.monitoring import join_terminate_close_proc
from parsl.multiprocessing import ForkProcess


def noop():
    pass


@pytest.mark.local
def test_end_process_already_exited():
    p = ForkProcess(target=noop)
    p.start()
    p.join()
    join_terminate_close_proc(p)


def hang():
    while True:
        pass


@pytest.mark.local
def test_end_hung_process():
    """Test calling against a process that will not exit itself."""
    p = ForkProcess(target=hang)
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
    e = multiprocessing.Event()
    p = ForkProcess(target=hang_no_sigint, args=(e,))
    p.start()
    pid = p.pid
    join_terminate_close_proc(p, timeout=1)
    assert not psutil.pid_exists(pid), "process should not exist any more"
    assert e.is_set(), "hung process should have set event on signal"
