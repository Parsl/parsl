import logging
import os
import random
from concurrent.futures import Future

import pytest

import parsl


@parsl.python_app
def this_app(inputs=()):
    return inputs[0]


@pytest.mark.local
def test_future_representation(tmpd_cwd):
    import sqlalchemy
    from sqlalchemy import text

    from parsl.tests.configs.htex_local_alternate import fresh_config

    monitoring_db = str(tmpd_cwd / "monitoring.db")
    monitoring_url = "sqlite:///" + monitoring_db

    c = fresh_config()
    c.monitoring.logging_endpoint = monitoring_url
    c.run_dir = tmpd_cwd

    parsl.load(c)

    # we're going to pass this TOKEN into an app via a pre-requisite Future,
    # and then expect to see it appear in the monitoring database.
    TOKEN = random.randint(0, 1000000)

    # make a Future that has no result yet
    # invoke a task that depends on it
    # inspect and insert something about the monitoring recorded value of that Future
    # make the Future complete
    # inspect and insert something about the monitoring recorded value of that Future

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

    engine = sqlalchemy.create_engine(monitoring_url)
    with engine.begin() as connection:
        result = connection.execute(text("SELECT COUNT(*) FROM task"))
        (task_count, ) = result.first()
        assert task_count == 1

        result = connection.execute(text("SELECT task_inputs FROM task"))
        (task_inputs, ) = result.first()
        assert task_inputs == "[" + repr(TOKEN) + "]"
