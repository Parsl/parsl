import os
import signal
import time

import pytest

import parsl
from parsl.tests.configs.htex_local_alternate import fresh_config

# This is a very generous upper bound on expected shutdown time of target
# process after receiving a signal, measured in seconds.
PERMITTED_SHUTDOWN_TIME_S = 60


@parsl.python_app
def simple_app():
    return True


@pytest.mark.local
def test_no_kills():
    """This tests that we can create a monitoring-enabled DFK and shut it down."""

    parsl.load(fresh_config())

    assert parsl.dfk().monitoring is not None, "This test requires monitoring"

    parsl.dfk().cleanup()


@pytest.mark.local
@pytest.mark.parametrize("sig", [signal.SIGINT, signal.SIGTERM, signal.SIGKILL, signal.SIGQUIT])
@pytest.mark.parametrize("process_attr", ["zmq_router_proc", "udp_router_proc", "dbm_proc", "filesystem_proc"])
def test_kill_monitoring_helper_process(sig, process_attr, try_assert):
    """This tests that we can kill a monitoring process and still have successful shutdown.
    SIGINT emulates some racy behaviour when ctrl-C is pressed: that
    monitoring processes receive a ctrl-C too, and so the other processes
    need to be tolerant to monitoring processes arbitrarily exiting.
    """

    parsl.load(fresh_config())

    dfk = parsl.dfk()

    assert dfk.monitoring is not None, "Monitoring required"

    target_proc = getattr(dfk.monitoring, process_attr)

    assert target_proc is not None, "prereq: target process must exist"
    assert target_proc.is_alive(), "prereq: target process must be alive"

    target_pid = target_proc.pid
    assert target_pid is not None, "prereq: target process must have a pid"

    os.kill(target_pid, sig)

    try_assert(lambda: not target_proc.is_alive(), timeout_ms=PERMITTED_SHUTDOWN_TIME_S * 1000)

    # now we have broken one piece of the monitoring system, do some app
    # execution and then shut down.

    simple_app().result()

    parsl.dfk().cleanup()
