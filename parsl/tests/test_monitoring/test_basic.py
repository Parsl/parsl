import logging
import os
import parsl
import pytest

logger = logging.getLogger(__name__)


@parsl.python_app
def this_app():
    import time
    time.sleep(7)
    return 5


@pytest.mark.local
def test_row_counts():
    # this is imported here rather than at module level because
    # it isn't available in a plain parsl install, so this module
    # would otherwise fail to import and break even a basic test
    # run.
    import sqlalchemy
    from parsl.tests.configs.htex_local_alternate import fresh_config

    if os.path.exists("monitoring.db"):
        logger.info("Monitoring database already exists - deleting")
        os.remove("monitoring.db")

    logger.info("loading parsl")
    parsl.load(fresh_config())

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

        # Two entries: one showing manager active, one inactive
        result = connection.execute("SELECT COUNT(*) FROM node")
        (c, ) = result.first()
        assert c == 2

        # There should be one block polling status
        # local provider has a status_polling_interval of 5s
        result = connection.execute("SELECT COUNT(*) FROM block")
        (c, ) = result.first()
        assert c == 1

    logger.info("all done")


if __name__ == "__main__":
    test_row_counts()
