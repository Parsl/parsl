import time

import pytest

import parsl
from parsl.app.app import bash_app, python_app
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.configs.local_threads import config
from parsl.executors import HighThroughputExecutor
from parsl.executors.high_throughput.manager_selector import (
    BlockIdManagerSelector,
    ManagerSelector,
)
from parsl.launchers import WrappedLauncher
from parsl.providers import LocalProvider
from parsl.usage_tracking.levels import LEVEL_1


def local_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label="htex_local",
                max_workers_per_node=1,
                manager_selector=BlockIdManagerSelector(),
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=2,
                    max_blocks=2,
                    min_blocks=2,
                ),
            )
        ],
        usage_tracking=LEVEL_1,
    )


@parsl.python_app
def get_worker_pid():
    import os
    return os.environ.get('PARSL_WORKER_BLOCK_ID')


@pytest.mark.local
def test_block_id_selection(try_assert):
    blockids = []
    try_assert(lambda: (get_worker_pid().result() == "1"), timeout_ms=20000)
    for i in range(10):
        future = get_worker_pid()
        blockids.append(future.result())

    assert all(blockid == "1" for blockid in blockids)
