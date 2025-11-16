import os
import time

import pytest

import parsl
from parsl import HighThroughputExecutor, ThreadPoolExecutor
from parsl.config import Config
from parsl.executors.status_handling import BlockProviderExecutor
from parsl.monitoring import MonitoringHub
from parsl.monitoring.radios.filesystem import FilesystemRadio
from parsl.monitoring.radios.htex import HTEXRadio
from parsl.monitoring.radios.udp import UDPRadio


@parsl.python_app
def this_app():
    # this delay needs to be several times the resource monitoring
    # period configured in the test configuration, so that some
    # messages are actually sent - there is no guarantee that any
    # (non-first) resource message will be sent at all for a short app.
    time.sleep(3)

    return 5


def thread_config():
    c = Config(executors=[ThreadPoolExecutor(remote_monitoring_radio=UDPRadio(address="localhost", atexit_timeout=0))],
               monitoring=MonitoringHub(resource_monitoring_interval=0))
    return c


def academy_config():
    from parsl.tests.configs.htex_local_academy import fresh_config
    return fresh_config()


def academy_globus_config():
    from globus_compute_sdk import Executor

    from parsl.config import Config
    from parsl.executors import GlobusComputeExecutor
    from parsl.monitoring.radios.academy import AcademyRadio

    # laptop endpoint
    # endpoint_id = 'd23b9bc6-99d1-40c4-8c35-9effd8a2266c'
    # hetzner VM endpoint
    endpoint_id = '8c83fa8d-9e1f-4704-b2dc-b942e8d0d4fb'

    # endpoint_id = os.environ["GLOBUS_COMPUTE_ENDPOINT"]

    return Config(
        executors=[
            GlobusComputeExecutor(
                executor=Executor(endpoint_id=endpoint_id),
                label="globus_compute",
                remote_monitoring_radio=AcademyRadio()
            )
        ],
        monitoring=MonitoringHub(resource_monitoring_interval=1)
    )


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
@pytest.mark.parametrize("fresh_config", [thread_config, academy_config, academy_globus_config])
def test_row_counts_base(tmpd_cwd, fresh_config):
    row_counts_parametrized(tmpd_cwd, fresh_config)
