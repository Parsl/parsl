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

    # So after a few seconds we should see
    # 0 blocks connected, 1 block marked as terminal
    # (post-fix that should be marked as SCALED_IN
    # but pre-fix it won't be... so for checking it
    # fails before the fix, we can't check SCALED_IN)
    # but we can check the historical blocks connected
    # which should show no blocks connected.

    # once we see that we can swap in the new launcher
    # and try running an app.

    assert htex.provider.launcher.prepend != "", "Pre-req: prepend attribute should exist and be non-empty"
    htex.provider.launcher.prepend = ""
    assert task().result() == 7
