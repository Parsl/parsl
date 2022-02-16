import logging
import os
import parsl
import pytest
import signal
import time

logger = logging.getLogger(__name__)


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
def test_kill_router(sig, process_attr):
    from parsl.tests.configs.htex_local_alternate import fresh_config
    """This tests that we can kill a monitoring process and still have successful shutdown.
    This emulates behaviour when ctrl-C is pressed: that all of the processes receive a
    termination signal  - SIGINT for ctrl-C - at once, and so specifically we should be
    tolerant to monitoring processes going away.
    """

    # what is the time limit for the router shutting down?
    expected_router_shutdown_time = 60

    logger.info("Initialising parsl")
    parsl.load(fresh_config())
    logger.info("Initialised parsl")

    dfk = parsl.dfk()

    assert dfk.monitoring is not None, "Monitoring required"

    # TODO: there are two processes we need to check we can kill (or perhaps both as well)
    # monitoring.router_proc and monitoring.dbm_proc

    router_proc = getattr(dfk.monitoring, process_attr)

    assert router_proc is not None, "Monitoring router process required"
    assert router_proc.is_alive(), "Router must be alive"

    router_pid = router_proc.pid
    assert router_pid is not None, "Router must have a pid"

    logger.info(f"Sending {sig} to router")
    os.kill(router_pid, sig)

    logger.info("Waiting for router process to die, or timeout")
    start_time = time.time()
    while router_proc.is_alive() and start_time + expected_router_shutdown_time > time.time():
        logger.info("Wait loop")
        time.sleep(1)

    assert not router_proc.is_alive(), "Process must have died to continue"

    # now we have broken one piece of the monitoring system
    # let's run some apps that should generate some monitoring traffic

    logger.info("Invoking simple app")
    f = simple_app()

    logger.info("Invoked simple app, waiting for result")

    f.result()

    logger.info("Got simple app result")

    logger.info("Calling cleanup")
    parsl.dfk().cleanup()
    logger.info("Finished cleanup")

    parsl.clear()
