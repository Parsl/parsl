
import logging
import os

import pytest

import parsl

logger = logging.getLogger(__name__)


@parsl.python_app(cache=True)
def this_app(x):
    return x + 1


@pytest.mark.local
def test_hashsum():
    import sqlalchemy
    from sqlalchemy import text

    from parsl.tests.configs.htex_local_alternate import fresh_config

    if os.path.exists("runinfo/monitoring.db"):
        logger.info("Monitoring database already exists - deleting")
        os.remove("runinfo/monitoring.db")

    logger.info("loading parsl")
    parsl.load(fresh_config())

    logger.info("invoking and waiting for result (1/4)")
    f1 = this_app(4)
    assert f1.result() == 5

    logger.info("invoking and waiting for result (2/4)")
    f2 = this_app(17)
    assert f2.result() == 18

    logger.info("invoking and waiting for result (3/4)")
    f3 = this_app(4)
    assert f3.result() == 5

    logger.info("invoking and waiting for result (4/4)")
    f4 = this_app(4)
    assert f4.result() == 5

    assert f1.task_record['hashsum'] == f3.task_record['hashsum']
    assert f1.task_record['hashsum'] == f4.task_record['hashsum']
    assert f1.task_record['hashsum'] != f2.task_record['hashsum']

    logger.info("cleaning up parsl")
    parsl.dfk().cleanup()

    # at this point, we should find one row in the monitoring database.

    logger.info("checking database content")
    engine = sqlalchemy.create_engine("sqlite:///runinfo/monitoring.db")
    with engine.begin() as connection:

        # we should have three tasks, but with only two tries, because the
        # memo try should be missing
        result = connection.execute(text("SELECT COUNT(*) FROM task"))
        (task_count, ) = result.first()
        assert task_count == 4

        # this will check that the number of task rows for each hashsum matches the above app invocations
        result = connection.execute(text(f"SELECT COUNT(task_hashsum) FROM task WHERE task_hashsum='{f1.task_record['hashsum']}'"))
        (hashsum_count, ) = result.first()
        assert hashsum_count == 3

        result = connection.execute(text(f"SELECT COUNT(task_hashsum) FROM task WHERE task_hashsum='{f2.task_record['hashsum']}'"))
        (hashsum_count, ) = result.first()
        assert hashsum_count == 1

        result = connection.execute(text("SELECT COUNT(*) FROM status WHERE task_status_name='exec_done'"))
        (memo_count, ) = result.first()
        assert memo_count == 2

        result = connection.execute(text("SELECT COUNT(*) FROM status WHERE task_status_name='memo_done'"))
        (memo_count, ) = result.first()
        assert memo_count == 2

    logger.info("all done")
