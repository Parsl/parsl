from random import randint

import pytest

import parsl
from parsl.app.app import python_app
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.executors.high_throughput.manager_selector import RandomManagerSelector
from parsl.providers import LocalProvider
from parsl.usage_tracking.levels import LEVEL_1


@python_app
def fake_task(parsl_resource_specification=None):
    import time
    return time.time()


@pytest.mark.local
def test_priority_queue(try_assert):
    provider = LocalProvider(
        init_blocks=0,
        max_blocks=0,
        min_blocks=0,
    )

    htex = HighThroughputExecutor(
        label="htex_local",
        max_workers_per_node=1,
        manager_selector=RandomManagerSelector(),
        provider=provider,
        worker_debug=True,  # needed to instrospect interchange logs
    )

    config = Config(
        executors=[htex],
        strategy="htex_auto_scale",
        usage_tracking=LEVEL_1,
    )

    with parsl.load(config):
        futures = {}

        # Submit tasks with mixed priorities
        # Test fallback behavior with a guaranteed-unsorted priorities
        priorities = [randint(2, 9) for _ in range(randint(1, 10))]
        priorities.insert(0, 10)
        priorities.extend((1, 10, 1))
        for i, priority in enumerate(priorities):
            spec = {'priority': priority}
            futures[(priority, i)] = fake_task(parsl_resource_specification=spec)

        # wait for the interchange to have received all tasks
        # (which happens asynchronously to the main thread, and is otherwise
        # a race condition which can cause this test to fail)

        n = len(priorities)

        def interchange_logs_task_count():
            with open(htex.worker_logdir + "/interchange.log", "r") as f:
                lines = f.readlines()
                for line in lines:
                    if f"Put task {n} onto pending_task_queue" in line:
                        return True
            return False

        try_assert(interchange_logs_task_count)

        provider.max_blocks = 1
        htex.scale_out_facade(1)  # don't wait for the JSP to catch up

        # Wait for completion
        results = {
            key: future.result() for key, future in futures.items()
        }

        # Sort by finish time
        sorted_by_completion = sorted(results.items(), key=lambda item: item[1])
        execution_order = [key for key, _ in sorted_by_completion]

        # check priority queue functionality
        priorities_only = [p for (p, i) in execution_order]
        assert priorities_only == sorted(priorities_only), "Priority execution order failed"

        # check FIFO fallback
        from collections import defaultdict
        seen = defaultdict(list)
        for (priority, idx) in execution_order:
            seen[priority].append(idx)

        for priority, indices in seen.items():
            assert indices == sorted(indices), f"FIFO fallback violated for priority {priority}"
