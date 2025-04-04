import os
import time

import pytest

import parsl
from parsl import HighThroughputExecutor, ThreadPoolExecutor
from parsl.config import Config
from parsl.executors.status_handling import BlockProviderExecutor
from parsl.monitoring import MonitoringHub


@parsl.python_app
def this_app():
    # this delay needs to be several times the resource monitoring
    # period configured in the test configuration, so that some
    # messages are actually sent - there is no guarantee that any
    # (non-first) resource message will be sent at all for a short app.
    time.sleep(3)

    return 5


# The below fresh configs are for use in parametrization, and should return
# a configuration that is suitably configured for monitoring.

def thread_config():
    c = Config(executors=[ThreadPoolExecutor()],
               monitoring=MonitoringHub(hub_address="localhost",
                                        resource_monitoring_interval=0))
    return c


def htex_config():
    """This config will use htex's default htex-specific monitoring radio mode"""
    from parsl.tests.configs.htex_local_alternate import fresh_config
    return fresh_config()


def htex_udp_config():
    """This config will force UDP"""
    from parsl.tests.configs.htex_local_alternate import fresh_config
    c = fresh_config()
    assert len(c.executors) == 1

    assert c.executors[0].radio_mode == "htex", "precondition: htex has a radio mode attribute, configured for htex radio"
    c.executors[0].radio_mode = "udp"

    return c


def htex_filesystem_config():
    """This config will force filesystem radio"""
    from parsl.tests.configs.htex_local_alternate import fresh_config
    c = fresh_config()
    assert len(c.executors) == 1

    assert c.executors[0].radio_mode == "htex", "precondition: htex has a radio mode attribute, configured for htex radio"
    c.executors[0].radio_mode = "filesystem"

    return c


def workqueue_config():
    from parsl.tests.configs.workqueue_ex import fresh_config
    c = fresh_config()
    c.monitoring = MonitoringHub(
                        hub_address="localhost",
                        resource_monitoring_interval=1)
    return c


def taskvine_config():
    from parsl.executors.taskvine import TaskVineExecutor, TaskVineManagerConfig
    c = Config(executors=[TaskVineExecutor(manager_config=TaskVineManagerConfig(port=9000),
                                           worker_launch_method='provider')],
               strategy_period=0.5,

               monitoring=MonitoringHub(hub_address="localhost",
                                        resource_monitoring_interval=1))
    return c


def row_counts_parametrized(tmpd_cwd, fresh_config):
    # this is imported here rather than at module level because
    # it isn't available in a plain parsl install, so this module
    # would otherwise fail to import and break even a basic test
    # run.
    import sqlalchemy
    from sqlalchemy import text

    db_url = f"sqlite:///{tmpd_cwd}/monitoring.db"

    config = fresh_config()
    config.run_dir = tmpd_cwd
    config.monitoring.logging_endpoint = db_url

    with parsl.load(config):
        assert this_app().result() == 5

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

        if isinstance(config.executors[0], HighThroughputExecutor):
            # The node table is specific to the HighThroughputExecutor
            # Two entries: one showing manager active, one inactive
            result = connection.execute(text("SELECT COUNT(*) FROM node"))
            (c, ) = result.first()
            assert c == 4

        if isinstance(config.executors[0], BlockProviderExecutor):
            # This case assumes that a BlockProviderExecutor is actually being
            # used with blocks. It might not be (for example, Work Queue and
            # Task Vine can be configured to launch their own workers; and it
            # is a valid (although occasional) use of htex to launch executors
            # manually.
            # If you just added test cases like that and are wondering why this
            # assert is failing, that might be why.
            result = connection.execute(text("SELECT COUNT(*) FROM block"))
            (c, ) = result.first()
            assert c >= 2, "There should be at least two block statuses from a BlockProviderExecutor"

        result = connection.execute(text("SELECT COUNT(*) FROM resource"))
        (c, ) = result.first()
        if isinstance(config.executors[0], ThreadPoolExecutor):
            assert c == 0, "Thread pool executors should not be recording resources"
        else:
            assert c >= 1, "Task execution should have created some resource records"


@pytest.mark.local
@pytest.mark.parametrize("fresh_config", [thread_config, htex_config, htex_filesystem_config, htex_udp_config])
def test_row_counts_base(tmpd_cwd, fresh_config):
    row_counts_parametrized(tmpd_cwd, fresh_config)


@pytest.mark.workqueue
@pytest.mark.local
@pytest.mark.parametrize("fresh_config", [workqueue_config])
def test_row_counts_wq(tmpd_cwd, fresh_config):
    row_counts_parametrized(tmpd_cwd, fresh_config)


@pytest.mark.taskvine
@pytest.mark.local
@pytest.mark.parametrize("fresh_config", [taskvine_config])
def test_row_counts_tv(tmpd_cwd, fresh_config):
    row_counts_parametrized(tmpd_cwd, fresh_config)
