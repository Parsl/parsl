import math
from unittest.mock import MagicMock

import pytest

from parsl.executors.high_throughput.executor import HighThroughputExecutor
from parsl.jobs.states import JobState, JobStatus
from parsl.jobs.strategy import Strategy


# the parameterize tuple consists of:
# Input:
#  * number of tasks to mock the load as
#  * number of workers per node
# Expected output:
#  * the number of blocks we should expect to be launched
#    in this situation
#
# This test will configure an executor, then run strategize
# a few times, asserting that it converges to the correct
# number of blocks without oscillating.
@pytest.mark.local
@pytest.mark.parametrize("ns", [(14, 48, 1),    # values from issue #3696

                                (1, 1, 1),      # one task needs one block

                                (100, 1, 20),   # many one-task blocks, hitting hard-coded max blocks

                                (47, 48, 1),    # some edge cases around #3696 values
                                (48, 48, 1),    # "
                                (49, 48, 2),    # "
                                (149, 50, 3)])  # "
def test_htex_strategy_does_not_oscillate(ns):
    """Check for oscillations in htex scaling.
    In issue 3696, with a large number of workers per block
    and a smaller number of active tasks, the htex scaling
    strategy oscillates between 0 and 1 active block, rather
    than converging to 1 active block.
    """

    n_tasks, n_workers, n_blocks = ns

    s = Strategy(strategy='htex_auto_scale', max_idletime=0)

    provider = MagicMock()
    executor = MagicMock(spec=HighThroughputExecutor)

    statuses = {}

    executor.provider = provider
    executor.outstanding = n_tasks
    executor.status_facade = statuses
    executor.workers_per_node = n_workers

    provider.parallelism = 1
    provider.init_blocks = 0
    provider.min_blocks = 0
    provider.max_blocks = 20
    provider.nodes_per_block = 1

    def scale_out(n):
        for _ in range(n):
            statuses[len(statuses)] = JobStatus(state=JobState.PENDING)

    executor.scale_out_facade.side_effect = scale_out

    def scale_in(n, max_idletime=None):
        # find n PENDING jobs and set them to CANCELLED
        for k in statuses:
            if n == 0:
                return
            if statuses[k].state == JobState.PENDING:
                statuses[k].state = JobState.CANCELLED
                n -= 1

    executor.scale_in_facade.side_effect = scale_in

    s.add_executors([executor])

    # In issue #3696, this first strategise does initial and load based
    # scale outs, because n_tasks > n_workers*0
    s.strategize([executor])

    executor.scale_out_facade.assert_called()
    assert len(statuses) == n_blocks, "Should have launched n_blocks"
    assert len([k for k in statuses if statuses[k].state == JobState.PENDING]) == n_blocks
    # there might be several calls to scale_out_facade inside strategy,
    # but the end effect should be that exactly one block is scaled out.

    executor.scale_in_facade.assert_not_called()

    # In issue #3696, this second strategize does a scale in, because n_tasks < n_workers*1
    s.strategize([executor])

    # assert that there should still be n_blocks pending blocks
    assert len([k for k in statuses if statuses[k].state == JobState.PENDING]) == n_blocks
    # this assert fails due to issue #3696

    # Now check scale in happens with 0 load
    executor.outstanding = 0
    s.strategize([executor])
    executor.scale_in_facade.assert_called()
    assert len([k for k in statuses if statuses[k].state == JobState.PENDING]) == 0
