import time

import pytest

import parsl
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import WrappedLauncher
from parsl.providers import LocalProvider


def local_config():
    return Config(
        max_idletime=1,
        strategy='htex_auto_scale',
        strategy_period=1,
        executors=[
            HighThroughputExecutor(
                label="htex_local",
                address="127.0.0.1",
                cores_per_worker=1,
                encrypted=True,
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=1,
                    min_blocks=0,
                    max_blocks=1,
                    # TODO: swap out the launcher during the test. make it use a much longer sleep (or infinite sleep)
                    # to give blocks that run but never complete themselves. then swap that for a launcher which
                    # launches the worker pool immediately.
                    launcher=WrappedLauncher(prepend="sleep inf ; "),
                ),
            )
        ],
    )


@parsl.python_app
def task():
    return 7


@pytest.mark.local
def test_regression(try_assert):
    # config means that we should start with one block and very rapidly scale
    # it down to 0 blocks. Because of 'sleep inf' in the WrappedLaucher, the
    # block will not ever register. This lack of registration is part of the
    # bug being tested for regression.

    # After that scaling has happened, we should see that we have one block
    # and it should be in a terminal state. We should also see htex reporting
    # no blocks connected.

    # Give 10 strategy periods for the above to happen: each step of scale up,
    # and scale down due to idleness isn't guaranteed to happen in exactly one
    # scaling step.

    htex = parsl.dfk().executors['htex_local']

    try_assert(lambda: len(htex.status_facade) == 1 and htex.status_facade['0'].terminal,
               timeout_ms=10000)

    assert htex.connected_blocks() == [], "No block should have connected to interchange"

    # Now we can reconfigure the launcher to let subsequent blocks launch ok,
    # and run a trivial task. That trivial task will scale up a new block and
    # run the task successfully.

    # Prior to issue #3568, the bug was that the scale in of the first
    # block earlier in the test case would have been treated as a failure,
    # and then the block error handler would have treated that failure as a
    # permanent htex failure, and so the task execution below would raise
    # a BadStateException rather than attempt to run the task.

    assert htex.provider.launcher.prepend != "", "Pre-req: prepend attribute should exist and be non-empty"
    htex.provider.launcher.prepend = ""
    assert task().result() == 7
