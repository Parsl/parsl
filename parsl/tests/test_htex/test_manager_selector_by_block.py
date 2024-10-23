import time

import pytest

import parsl
from parsl.app.app import bash_app, python_app
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.executors.high_throughput.manager_selector import (
    BlockIdManagerSelector,
    ManagerSelector,
)
from parsl.launchers import WrappedLauncher
from parsl.providers import LocalProvider
from parsl.usage_tracking.levels import LEVEL_1

BLOCK_COUNT = 2


@parsl.python_app
def get_worker_pid():
    import os
    return os.environ.get('PARSL_WORKER_BLOCK_ID')


@pytest.mark.local
def test_block_id_selection(try_assert):
    htex = HighThroughputExecutor(
        label="htex_local",
        max_workers_per_node=1,
        manager_selector=BlockIdManagerSelector(),
        provider=LocalProvider(
            init_blocks=BLOCK_COUNT,
            max_blocks=BLOCK_COUNT,
            min_blocks=BLOCK_COUNT,
        ),
    )

    config = Config(
        executors=[htex],
        usage_tracking=LEVEL_1,
    )

    with parsl.load(config):
        blockids = []
        try_assert(lambda: len(htex.connected_managers()) == BLOCK_COUNT, timeout_ms=20000)
        for i in range(10):
            future = get_worker_pid()
            blockids.append(future.result())

        assert all(blockid == "1" for blockid in blockids)
