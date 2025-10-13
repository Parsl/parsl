import logging
import os
import time

import pytest
from sqlalchemy import text

import parsl

logger = logging.getLogger(__name__)


@parsl.python_app
def this_app():
    # this delay needs to be several times the resource monitoring
    # period configured in the test configuration, so that some
    # messages are actually sent - there is no guarantee that any
    # (non-first) resource message will be sent at all for a short app.
    time.sleep(3)

    return 5


def fresh_config():
    from parsl import ThreadPoolExecutor
    from parsl.config import Config
    from parsl.monitoring import MonitoringHub

    return Config(executors=[ThreadPoolExecutor(label='threads', max_threads=4)],
                  monitoring=MonitoringHub(resource_monitoring_interval=3)
                  )


@pytest.mark.local
def test_row_counts():
    # this is imported here rather than at module level because
    # it isn't available in a plain parsl install, so this module
    # would otherwise fail to import and break even a basic test
    # run.
    import sqlalchemy

    if os.path.exists("runinfo/monitoring.db"):
        logger.info("Monitoring database already exists - deleting")
        os.remove("runinfo/monitoring.db")

    logger.info("Generating fresh config")
    c = fresh_config()
    logger.info("Loading parsl")
    parsl.load(c)

    logger.info("invoking and waiting for result")
    assert this_app().result() == 5

    logger.info("cleaning up parsl")
    parsl.dfk().cleanup()
    parsl.clear()

    # at this point, we should find one row in the monitoring database.

    logger.info("checking database content")
    engine = sqlalchemy.create_engine("sqlite:///runinfo/monitoring.db")
    with engine.begin() as connection:

        result = connection.execute(text("SELECT COUNT(*) FROM workflow"))
        (c, ) = result.first()
        assert c == 1

        result = connection.execute(text("SELECT COUNT(*) FROM task"))
        (c, ) = result.first()
        assert c == 1

        result = connection.execute(text("SELECT COUNT(*) FROM try"))
        (c, ) = result.first()
        assert c == 1

        result = connection.execute(text("SELECT COUNT(*) FROM status, try "
                                         "WHERE status.task_id = try.task_id "
                                         "AND status.task_status_name='exec_done' "
                                         "AND task_try_time_running is NULL"))
        (c, ) = result.first()
        assert c == 0

        # DIFF WRT ORIGINAL TEST: there is no concept of 'node' in local thread execution
        # result = connection.execute(text("SELECT COUNT(*) FROM node"))
        # (c, ) = result.first()
        # assert c == 2

        # DIFF WRT ORIGINAL TEST: there is no concept of block in local thread execution
        # There should be one block polling status
        # local provider has a status_polling_interval of 5s
        # result = connection.execute(text("SELECT COUNT(*) FROM block"))
        # (c, ) = result.first()
        # assert c >= 2

        # DIFF WRT ORIGINAL TEST: there is no resource monitoring with local thread executor
        # result = connection.execute(text("SELECT COUNT(*) FROM resource"))
        # (c, ) = result.first()
        # assert c >= 1

    logger.info("all done")


if __name__ == "__main__":
    test_row_counts()
