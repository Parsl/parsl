import random
import threading

import pytest

import parsl
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SimpleLauncher
from parsl.providers import LocalProvider

# we need some blocks, but it doesn't matter too much how many, as long
# as they can all start up and get registered within the try_assert
# timeout later on.
BLOCK_COUNT = 3

# the try_assert timeout for the above number of blocks to get started
PERMITTED_STARTUP_TIME_S = 30


class AccumulatingLocalProvider(LocalProvider):
    def __init__(self, *args, **kwargs):
        # Use a list for submitted job IDs because if there are multiple
        # submissions returning the same job ID, this test should count
        # those...
        self.submit_job_ids = []

        # ... but there's no requirement, I think, that cancel must be called
        # only once per job id. What matters here is that each job ID is
        # cancelled at least once.
        self.cancel_job_ids = set()

        super().__init__(*args, **kwargs)

    def submit(self, *args, **kwargs):
        job_id = super().submit(*args, **kwargs)
        self.submit_job_ids.append(job_id)
        return job_id

    def cancel(self, job_ids):
        self.cancel_job_ids.update(job_ids)
        return super().cancel(job_ids)


@pytest.mark.local
def test_shutdown_scalein_blocks(tmpd_cwd, try_assert):
    """
    This test scales up several blocks, and then checks that they are all
    scaled in at DFK shutdown.
    """
    accumulating_provider = AccumulatingLocalProvider(
        init_blocks=BLOCK_COUNT,
        min_blocks=0,
        max_blocks=0,
        launcher=SimpleLauncher(),
    )

    htex = HighThroughputExecutor(
               label="htex_local",
               cores_per_worker=1,
               provider=accumulating_provider
           )

    config = Config(
        executors=[htex],
        strategy='none',
        strategy_period=0.1,
        run_dir=str(tmpd_cwd)
    )

    with parsl.load(config):
        # this will wait for everything to be scaled out fully
        try_assert(lambda: len(htex.connected_managers()) == BLOCK_COUNT, timeout_ms=PERMITTED_STARTUP_TIME_S * 1000)

    assert len(accumulating_provider.submit_job_ids) == BLOCK_COUNT, f"Exactly {BLOCK_COUNT} blocks should have been launched"
    assert len(accumulating_provider.cancel_job_ids) == BLOCK_COUNT, f"Exactly {BLOCK_COUNT} blocks should have been scaled in"
