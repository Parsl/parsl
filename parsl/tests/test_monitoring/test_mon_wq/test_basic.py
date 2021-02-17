import logging
import os
import parsl
import pytest
import time

logger = logging.getLogger(__name__)


@parsl.python_app
def this_app():
    # this delay needs to be several times the resource monitoring
    # period configured in the test configuration, so that some
    # messages are actually sent - there is no guarantee that any
    # (non-first) resource message will be sent at all for a short app.
    time.sleep(3)

    return 5


@pytest.mark.local
def test_row_counts():
    # this is imported here rather than at module level because
    # it isn't available in a plain parsl install, so this module
    # would otherwise fail to import and break even a basic test
    # run.
    import sqlalchemy
    from parsl.tests.configs.workqueue_monitoring import fresh_config

    if os.path.exists("monitoring.db"):
        logger.info("Monitoring database already exists - deleting")
        os.remove("monitoring.db")

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
    engine = sqlalchemy.create_engine("sqlite:///monitoring.db")
    with engine.begin() as connection:

        result = connection.execute("SELECT COUNT(*) FROM workflow")
        (c, ) = result.first()
        assert c == 1

        result = connection.execute("SELECT COUNT(*) FROM task")
        (c, ) = result.first()
        assert c == 1

        result = connection.execute("SELECT COUNT(*) FROM try")
        (c, ) = result.first()
        assert c == 1

        result = connection.execute("SELECT COUNT(*) FROM status, try "
                                    "WHERE status.task_id = try.task_id "
                                    "AND status.task_status_name='exec_done' "
                                    "AND task_try_time_running is NULL")
        (c, ) = result.first()
        assert c == 0

        # workqueue doesn't populate the node table.
        # because parsl level code isn't running on a node persistently
        # instead, it is the workqueue worker doing that, which doesn't
        # report into parsl monitoring.
        # this is a feature downgrade from using htex that needs some
        # consideration

        # Two entries: one showing manager active, one inactive
        # result = connection.execute("SELECT COUNT(*) FROM node")
        # (c, ) = result.first()
        # assert c == 2

        # workqueue, at least when using providers, does have a loose
        # block concept: but it doesn't report anything into the block
        # table here, and if using wq external scaling thing, then there
        # wouldn't be parsl level blocks at all.
        # This needs some consideration.

        # There should be one block polling status
        # local provider has a status_polling_interval of 5s
        # result = connection.execute("SELECT COUNT(*) FROM block")
        # (c, ) = result.first()
        # assert c >= 2

        result = connection.execute("SELECT COUNT(*) FROM resource")
        (c, ) = result.first()
        assert c >= 1

    logger.info("all done")


if __name__ == "__main__":
    test_row_counts()
