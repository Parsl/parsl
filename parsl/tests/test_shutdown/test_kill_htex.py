import logging
import os
import parsl
import pytest
import signal
import time

logger = logging.getLogger(__name__)

# TODO:
# should parametrically test both htex_local and htex_local_alternate
from parsl.tests.configs.htex_local import fresh_config


@parsl.python_app
def simple_app():
    return True


@pytest.mark.local
@pytest.mark.skip("not expected to pass - demonstrates hanging htex with missing interchange")
@pytest.mark.parametrize("sig", [signal.SIGINT, signal.SIGTERM, signal.SIGKILL])  # are we expecting SIGKILL resilience here? Ideally yes
def test_kill_router(sig):
    """This tests that we can kill a monitoring process and still have successful shutdown.
    This emulates behaviour when ctrl-C is pressed: that all of the processes receive a
    termination signal  - SIGINT for ctrl-C - at once, and so specifically we should be
    tolerant to monitoring processes going away.
    """

    # what is the time limit for the interchange shutting down?
    expected_shutdown_time = 60

    logger.info("Initialising parsl")
    parsl.load(fresh_config())
    logger.info("Initialised parsl")

    dfk = parsl.dfk()

    assert "htex_local" in dfk.executors.keys(), "htex required"

    proc = dfk.executors["htex_local"].interchange_proc

    assert proc is not None, "Interchange process required"
    assert proc.is_alive(), "Interchange must be alive"

    pid = proc.pid
    assert pid is not None, "Interchange must have a pid"

    time.sleep(5)
    logger.info(f"Sending {sig} to interchange pid {pid} - 1")
    os.kill(pid, sig)
    time.sleep(5)
    logger.info(f"Sending {sig} to interchange pid {pid} - 2")
    os.kill(pid, sig)
    time.sleep(5)
    logger.info(f"Sending {sig} to interchange pid {pid} - 3")
    os.kill(pid, sig)

    logger.info("Waiting for interchange process to die, or timeout")
    start_time = time.time()
    while proc.is_alive() and start_time + expected_shutdown_time > time.time():
        logger.info("Wait loop")
        time.sleep(1)

    assert not proc.is_alive(), "Interchange process must have died for test to continue"

    # now we have broken one piece of the monitoring system
    # let's run some apps that should generate some monitoring traffic

    logger.info("Invoking simple app")
    f = simple_app()

    logger.info("Invoked simple app, waiting for result")

    r = f.exception()

    assert isinstance(r, Exception), "simple app should have raised an exception"

    logger.info("Got simple app result")

    logger.info("Calling cleanup")
    parsl.dfk().cleanup()
    logger.info("Finished cleanup")

    parsl.clear()
