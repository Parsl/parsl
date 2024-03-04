import os
import parsl
import pytest
import signal
import time


@parsl.python_app
def simple_app():
    return True


@pytest.mark.local
def test_no_kills():
    from parsl.tests.configs.htex_local_alternate import fresh_config
    """This tests that we can create a monitoring-enabled DFK and shut it down."""

    parsl.load(fresh_config())

    assert parsl.dfk().monitoring is not None, "This test requires monitoring"

    parsl.dfk().cleanup()
    parsl.clear()


@pytest.mark.local
@pytest.mark.parametrize("sig", [signal.SIGINT, signal.SIGTERM, signal.SIGKILL])  # are we expecting SIGKILL resilience here? Ideally yes
@pytest.mark.parametrize("process_attr", ["router_proc", "dbm_proc"])
def test_kill_monitoring_helper_process(sig, process_attr, try_assert):
    from parsl.tests.configs.htex_local_alternate import fresh_config
    """This tests that we can kill a monitoring process and still have successful shutdown.
    This emulates behaviour when ctrl-C is pressed: that all of the processes receive a
    termination signal  - SIGINT for ctrl-C - at once, and so specifically we should be
    tolerant to monitoring processes going away.
    """

    # This is a very generous upper bound on process shutdown times.
    expected_target_shutdown_time = 60

    parsl.load(fresh_config())

    dfk = parsl.dfk()

    assert dfk.monitoring is not None, "Monitoring required"

    target_proc = getattr(dfk.monitoring, process_attr)

    assert target_proc is not None, "prereq: target process must exist"
    assert target_proc.is_alive(), "prereq: target process must be alive"

    target_pid = target_proc.pid
    assert target_pid is not None, "prereq: target process must have a pid"

    os.kill(target_pid, sig)

    start_time = time.time()

    try_assert(lambda: not target_proc.is_alive(), timeout_ms = expected_target_shutdown_time * 1000)

    # now we have broken one piece of the monitoring system, do some app
    # execution and then shut down.

    simple_app().result()

    parsl.dfk().cleanup()
    parsl.clear()
