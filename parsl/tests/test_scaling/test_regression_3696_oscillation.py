import math
from unittest.mock import MagicMock

import pytest

from parsl.executors.high_throughput.executor import HighThroughputExecutor
from parsl.jobs.states import JobState, JobStatus
from parsl.jobs.strategy import Strategy


@pytest.mark.local
def test_htex_strategy_does_not_oscillate():
    """Check for oscillations in htex scaling.
    In issue 3696, with a large number of workers per block
    and a smaller number of active tasks, the htex scaling
    strategy oscillates between 0 and 1 active block, rather
    than converging to 1 active block.

    The choices of 14 tasks and 48 workers per node are taken
    from issue #3696.
    """

    s = Strategy(strategy='htex_auto_scale', max_idletime=math.inf)

    provider = MagicMock()
    executor = MagicMock(spec=HighThroughputExecutor)

    statuses = {}

    executor.provider = provider
    executor.outstanding = 14
    executor.status_facade = statuses
    executor.workers_per_node = 48

    provider.parallelism = 1
    provider.init_blocks = 0
    provider.min_blocks = 0
    provider.max_blocks = 2
    provider.nodes_per_block = 1

    def f(n):
        for _ in range(n):
            statuses[len(statuses)] = JobStatus(state=JobState.PENDING)

    executor.scale_out_facade.side_effect = f

    s.add_executors([executor])

    # In issue #3696, this first strategise does initial and load based
    # scale outs, because 14 > 48*0
    s.strategize([executor])
    executor.scale_out_facade.assert_called()
    executor.scale_in_facade.assert_not_called()

    # In issue #3696, this second strategize does a scale in, because 14 < 48*1
    s.strategize([executor])
    executor.scale_in_facade.assert_not_called()  # this assert fails due to issue #3696

    # Now check scale in happens with 0 load
    executor.outstanding = 0
    s.strategize([executor])
    executor.scale_in_facade.assert_called()
