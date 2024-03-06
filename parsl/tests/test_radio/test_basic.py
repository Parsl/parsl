import logging
import os
import parsl
import pytest

logger = logging.getLogger(__name__)


@parsl.python_app
def this_app():
    # this delay needs to be several times the resource monitoring
    # period configured in the test configuration, so that some
    # messages are actually sent - there is no guarantee that any
    # (non-first) resource message will be sent at all for a short app.
    import time
    time.sleep(3)

    return 5


@pytest.mark.local
def test_diaspora_radio():
    # this is imported here rather than at module level because
    # it isn't available in a plain parsl install, so this module
    # would otherwise fail to import and break even a basic test
    # run.
    import sqlalchemy
    from sqlalchemy import text
    from parsl.tests.configs.htex_local_alternate import fresh_config

    if os.path.exists("runinfo/monitoring.db"):
        logger.info("Monitoring database already exists - deleting")
        os.remove("runinfo/monitoring.db")

    logger.info("loading parsl")
    parsl.load(fresh_config())

    logger.info("invoking and waiting for result")
    assert this_app().result() == 5

    logger.info("cleaning up parsl")
    parsl.dfk().cleanup()
    parsl.clear()

    logger.info("all done")
