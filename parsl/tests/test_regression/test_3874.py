import shutil

import pytest

import parsl
from parsl.app.app import python_app
from parsl.config import Config
from parsl.executors import HighThroughputExecutor


@python_app
def noop():
    pass


@pytest.mark.local
def test_regression_3874(tmpd_cwd_session):
    # HTEX run 1

    rundir_1 = str(tmpd_cwd_session / "1")

    config = Config(executors=[HighThroughputExecutor()], strategy_period=0.5)
    config.run_dir = rundir_1

    with parsl.load(config):
        noop().result()

    # It is necessary to delete this rundir to exercise the bug. Otherwise,
    # the next run will be able to continue looking at this directory - the
    # bug manifests when it cannot.

    shutil.rmtree(rundir_1)

    # HTEX run 2
    # In the case of issue 3874, this run hangs (rather than failing) as the
    # JobStatusPoller fails to collect status of all of its managed tasks
    # every iteration, without converging towards failure.

    rundir_2 = str(tmpd_cwd_session / "2")

    config = Config(executors=[HighThroughputExecutor()], strategy_period=0.5)
    config.run_dir = rundir_2

    with parsl.load(config):
        noop().result()

    shutil.rmtree(rundir_2)
