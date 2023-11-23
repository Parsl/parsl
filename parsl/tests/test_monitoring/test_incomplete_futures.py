import logging
import os
import parsl
import pytest

from concurrent.futures import Future

logger = logging.getLogger(__name__)


@parsl.python_app
def this_app(inputs=()):
    return inputs[0]


@pytest.mark.local
def test_future_representation():
    import sqlalchemy
    from sqlalchemy import text
    from parsl.tests.configs.htex_local_alternate import fresh_config

    if os.path.exists("runinfo/monitoring.db"):
        logger.info("Monitoring database already exists - deleting")
        os.remove("runinfo/monitoring.db")

    logger.info("loading parsl")
    parsl.load(fresh_config())

    # this is arbitrary
    TOKEN = 54739

    # make a Future that has no result yet
    # invoke a task that depends on it
    # inspect and insert something about the monitoring recorded value of that Future
    # make the Future complete
    # inspect and insert something about the monitoring recorded value of that Future

    # potential race condition:
    # monitoring will take some time to update the record after submitting the task or completing the Future

    f1 = Future()

    f2 = this_app(inputs=[f1])

    f1.set_result(TOKEN)

    assert f2.result() == TOKEN

    # this cleanup gives a barrier that allows the monitoring code to store
    # everything it has in the database - without this, there's a race
    # condition that the task will not have arrived in the database yet.
    # A different approach for this test might be to poll the DB for a few
    # seconds, with the assumption "data will arrive in the DB within
    # 30 seconds, but probably much sooner".
    parsl.dfk().cleanup()
    parsl.clear()

    engine = sqlalchemy.create_engine("sqlite:///runinfo/monitoring.db")
    with engine.begin() as connection:
        result = connection.execute(text("SELECT COUNT(*) FROM task"))
        (task_count, ) = result.first()
        assert task_count == 1

        result = connection.execute(text("SELECT task_inputs FROM task"))
        (task_inputs, ) = result.first()
        assert task_inputs == "[" + repr(TOKEN) + "]"

    logger.info("all done")
