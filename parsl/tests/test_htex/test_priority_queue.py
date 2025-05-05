import pytest

import parsl
from parsl.app.app import bash_app, python_app
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.executors.high_throughput.manager_selector import (
    ManagerSelector,
    RandomManagerSelector,
)
from parsl.launchers import WrappedLauncher
from parsl.providers import LocalProvider
from parsl.usage_tracking.levels import LEVEL_1


@parsl.python_app
def fake_task(parsl_resource_specification={'priority': 1}):
    import time
    return time.time()


@pytest.mark.local
def test_priority_queue():
    p = LocalProvider(
        init_blocks=0,
        max_blocks=0,
        min_blocks=0,
    )

    htex = HighThroughputExecutor(
        label="htex_local",
        max_workers_per_node=1,
        manager_selector=RandomManagerSelector(),
        provider=p,
    )

    config = Config(
        executors=[htex],
        strategy="htex_auto_scale",
        usage_tracking=LEVEL_1,
    )

    with parsl.load(config):
        futures = {}
        for priority in range(10, 0, -1):
            spec = {'priority': priority}
            futures[priority] = fake_task(parsl_resource_specification=spec)

        p.max_blocks = 1
        results = {priority: future.result() for priority, future in futures.items()}
        sorted_results = dict(sorted(results.items(), key=lambda item: item[1]))
        sorted_priorities = list(sorted_results.keys())
        assert sorted_priorities == sorted(sorted_priorities)