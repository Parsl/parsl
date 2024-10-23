import time

import pytest

import parsl
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import WrappedLauncher
from parsl.providers import LocalProvider


def local_config():
    # see the comments inside test_regression for reasoning about why each
    # of these parameters is set why it is.
    return Config(
        max_idletime=1,

        strategy='htex_auto_scale',
        strategy_period=1,

        executors=[
            HighThroughputExecutor(
                label="htex_local",
                encrypted=True,
                provider=LocalProvider(
                    init_blocks=1,
                    min_blocks=0,
                    max_blocks=1,
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
    # The above config means that we should start scaling out one initial
    # block, but then scale it back in after a second or so if the executor
    # is kept idle (which this test does using try_assert).

    # Because of 'sleep inf' in the WrappedLaucher, the block will not ever
    # register.

    # The bug being tested is about mistreatment of blocks which are scaled in
    # before they have a chance to register, and the above forces that to
    # happen.

    # After that scaling in has happened, we should see that we have one block
    # and it should be in a terminal state. The below try_assert waits for
    # that to become true.

    # At that time, we should also see htex reporting no blocks registered - as
    # mentioned above, that is a necessary part of the bug being tested here.

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
    # block earlier in the test case would have incorrectly been treated as a
    # failure, and then the block error handler would have treated that failure
    # as a permanent htex failure, and so the task execution below would raise
    # a BadStateException rather than attempt to run the task.

    assert htex.provider.launcher.prepend != "", "Pre-req: prepend attribute should exist and be non-empty"
    htex.provider.launcher.prepend = ""
    assert task().result() == 7
