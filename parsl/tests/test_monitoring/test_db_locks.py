import logging
import os
import time

import pytest

import parsl

logger = logging.getLogger(__name__)


@parsl.python_app
def this_app():
    return 5


@pytest.mark.local
def test_row_counts():
    import sqlalchemy
    from sqlalchemy import text

    from parsl.tests.configs.htex_local_alternate import fresh_config
    if os.path.exists("runinfo/monitoring.db"):
        logger.info("Monitoring database already exists - deleting")
        os.remove("runinfo/monitoring.db")

    engine = sqlalchemy.create_engine("sqlite:///runinfo/monitoring.db")

    logger.info("loading parsl")
    parsl.load(fresh_config())

    # parsl.load() returns before all initialisation of monitoring
    # is complete, which means it isn't safe to take a read lock on
    # the database yet. This delay tries to work around that - some
    # better async behaviour might be nice, but what?
    #
    # Taking a read lock before monitoring is initialized will cause
    # a failure in the part of monitoring which creates tables, and
    # which is not protected against read locks at the time this test
    # was written.
    time.sleep(10)

    # to get an sqlite3 read lock that is held over a controllable
    # long time, create a transaction and perform a SELECT in it.
    # The lock will be held until the end of the transaction.
    # (see bottom of https://sqlite.org/lockingv3.html)

    logger.info("Getting a read lock on the monitoring database")
    with engine.begin() as readlock_connection:
        readlock_connection.execute(text("BEGIN TRANSACTION"))
        result = readlock_connection.execute(text("SELECT COUNT(*) FROM workflow"))
        (c, ) = result.first()
        assert c == 1
        # now readlock_connection should have a read lock that will
        # stay locked until the transaction is ended, or the with
        # block ends.

        logger.info("invoking and waiting for result")
        assert this_app().result() == 5

        # there is going to be some raciness here making sure that
        # the database manager actually tries to write while the
        # read lock is held. I'm not sure if there is a better way
        # to detect this other than a hopefully long-enough sleep.
        time.sleep(10)

    logger.info("cleaning up parsl")
    parsl.dfk().cleanup()

    # at this point, we should find data consistent with executing one
    # task in the database.

    logger.info("checking database content")
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

    logger.info("all done")
