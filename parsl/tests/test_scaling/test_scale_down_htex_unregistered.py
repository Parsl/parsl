import logging
import time

import pytest

import parsl
from parsl import File, python_app
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.jobs.states import TERMINAL_STATES, JobState
from parsl.launchers import SingleNodeLauncher
from parsl.providers import LocalProvider

logger = logging.getLogger(__name__)

_max_blocks = 1
_min_blocks = 0


def local_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                heartbeat_period=1,
                heartbeat_threshold=2,
                poll_period=100,
                label="htex_local",
                address="127.0.0.1",
                max_workers_per_node=1,
                encrypted=True,
                launch_cmd="sleep inf",
                provider=LocalProvider(
                    init_blocks=1,
                    max_blocks=_max_blocks,
                    min_blocks=_min_blocks,
                    launcher=SingleNodeLauncher(),
                ),
            )
        ],
        max_idletime=0.5,
        strategy='htex_auto_scale',
        strategy_period=0.1
    )


# see issue #1885 for details of failures of this test.
# at the time of issue #1885 this test was failing frequently
# in CI.
@pytest.mark.local
def test_scaledown_with_register(try_assert):
    dfk = parsl.dfk()
    htex = dfk.executors['htex_local']

    num_managers = len(htex.connected_managers())
    assert num_managers == 0, "Expected 0 managers at start"

    try_assert(lambda: len(htex.status()),
               fail_msg="Expected 1 block at start")

    s = htex.status()
    assert s['0'].state == JobState.RUNNING, "Expected block to be in RUNNING"

    def check_zero_blocks():
        s = htex.status()
        return len(s) == 1 and s['0'].state in TERMINAL_STATES

    try_assert(
        check_zero_blocks,
        fail_msg="Expected 0 blocks after idle scaledown",
        timeout_ms=15000,
    )
