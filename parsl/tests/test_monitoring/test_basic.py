
import logging
import os
import parsl
import pytest
import sqlalchemy

logger = logging.getLogger(__name__)

from parsl.tests.configs.htex_local_alternate import fresh_config


@parsl.python_app
def this_app():
    return 5


@pytest.mark.local
def test_row_counts():
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

    logger.info("all done")
