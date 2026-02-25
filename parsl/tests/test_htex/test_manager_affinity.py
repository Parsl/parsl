import logging
import random
import pytest

import parsl
from parsl.app.app import python_app
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.executors.high_throughput.manager_selector import RandomManagerSelector
from parsl.providers import LocalProvider
from parsl.usage_tracking.levels import LEVEL_1


logger = logging.getLogger(__name__)


@python_app
def fake_task(parsl_resource_specification=None):
    import time
    import os
    time.sleep(1)
    # return the worker pid. with only one worker per manager, this will
    # be enough to identify the manager/pool.
    return os.getpid()


@pytest.mark.local
def test_priority_queue(try_assert):
    provider = LocalProvider(
        init_blocks=5,
        max_blocks=5,
        min_blocks=5,
    )

    htex = HighThroughputExecutor(
        label="htex_local",
        max_workers_per_node=1, # needed for correctness
        provider=provider,
        worker_debug=True,  # needed to instrospect interchange logs
    )

    config = Config(
        executors=[htex],
        strategy="htex_auto_scale",
        usage_tracking=LEVEL_1,
    )

    with parsl.load(config):
        futures = []

        # Submit tasks with mixed priorities
        # Test fallback behavior with a guaranteed-unsorted priorities
        for i in range(100):
            # 10 different test affinities
            affinity = random.randint(1,10)
            spec = {'manager_affinity': affinity}
            futures.append((affinity, fake_task(parsl_resource_specification=spec)))

        # wait for the interchange to have received all tasks
        # (which happens asynchronously to the main thread, and is otherwise
        # a race condition which can cause this test to fail)

        n = len(futures)

        def interchange_logs_task_count():
            with open(htex.worker_logdir + "/interchange.log", "r") as f:
                lines = f.readlines() 
                for line in lines:
                    if f"Put task {n} onto pending_task_queue" in line:
                        return True
            return False

        try_assert(interchange_logs_task_count)

        affinities = {}
        for (affinity, fut) in futures:
            p = fut.result()
            if affinity in affinities:
                assert p == affinities[affinity]
            else:
                affinities[affinity] = p

        logger.info("affinities table: %r", RuntimeError(repr(affinities)))
