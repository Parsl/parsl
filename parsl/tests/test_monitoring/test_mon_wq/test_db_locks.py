
import logging
import os
import parsl
import pytest
import time

logger = logging.getLogger(__name__)


@parsl.python_app
def this_app():
    return 5


@pytest.mark.local
def test_row_counts():
    import sqlalchemy
    from parsl.tests.configs.workqueue_monitoring import fresh_config

    if os.path.exists("monitoring.db"):
        logger.info("Monitoring database already exists - deleting")
        os.remove("monitoring.db")

    engine = sqlalchemy.create_engine("sqlite:///monitoring.db")

    logger.info("loading parsl")
    parsl.load(fresh_config())

    # parsl.load() returns before all initialisation of monitoring
    # is complete, which means it isn't safe to take a read lock on
    # the database yet. This delay tries to work around that - some
    # better async behaviour might be nice, but I'm not sure what.
    time.sleep(10)

    # to get an sqlite3 read lock that is held over a controllable
    # long time, create a transaction and perform a SELECT in it.
    # (see bottom of https://sqlite.org/lockingv3.html)

    # there's an awkward race here: parsl.load() returns before the
    # database might have been created, and so then the db manager will
    # crash (and if there is a retry loop there instead, I think it will
    # hang until after the read lock stuff below is finished? which might
    # be acceptable? if it's meant to be properly async and not blocking?)
    # ... in which case, initialise parsl *after taking the lock* would also
    # work (although the select statement to get that lock wouldn't be the same
    # because it wouldn't be able to select from the right table)

    logger.info("Getting a read lock on the monitoring database")
    with engine.begin() as readlock_connection:
        readlock_connection.execute("BEGIN TRANSACTION")
        result = readlock_connection.execute("SELECT COUNT(*) FROM workflow")
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
    parsl.clear()

    # at this point, we should find one row in the monitoring database.

    logger.info("checking database content")
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
