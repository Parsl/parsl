import os
import parsl
import pytest
import time


@parsl.python_app
def this_app():
    # this delay needs to be several times the resource monitoring
    # period configured in the test configuration, so that some
    # messages are actually sent - there is no guarantee that any
    # (non-first) resource message will be sent at all for a short app.
    time.sleep(3)

    return 5


@pytest.mark.local
def test_row_counts(tmpd_cwd):
    # this is imported here rather than at module level because
    # it isn't available in a plain parsl install, so this module
    # would otherwise fail to import and break even a basic test
    # run.
    import sqlalchemy
    from sqlalchemy import text
    from parsl.tests.configs.htex_local_alternate import fresh_config

    db_url = f"sqlite:///{tmpd_cwd}/monitoring.db"

    c = fresh_config()
    c.monitoring.logging_endpoint = db_url

    with parsl.load(c):
        assert this_app().result() == 5

    parsl.clear()

    # at this point, we should find one row in the monitoring database.

    engine = sqlalchemy.create_engine(db_url)
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

        # Two entries: one showing manager active, one inactive
        result = connection.execute(text("SELECT COUNT(*) FROM node"))
        (c, ) = result.first()
        assert c == 2

        # There should be one block polling status
        # local provider has a status_polling_interval of 5s
        result = connection.execute(text("SELECT COUNT(*) FROM block"))
        (c, ) = result.first()
        assert c >= 2

        result = connection.execute(text("SELECT COUNT(*) FROM resource"))
        (c, ) = result.first()
        assert c >= 1
