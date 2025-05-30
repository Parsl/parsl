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
def test_priority_queue():
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
    )

    config = Config(
        executors=[htex],
        strategy="htex_auto_scale",
        usage_tracking=LEVEL_1,
    )

    with parsl.load(config):
        futures = {}

        # Submit tasks with mixed priorities
        # Priorities: [10, 10, 5, 5, 1, 1] to test fallback behavior
        for i, priority in enumerate([10, 10, 5, 5, 1, 1]):
            spec = {'priority': priority}
            futures[(priority, i)] = fake_task(parsl_resource_specification=spec)

        provider.max_blocks = 1

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
