"""Tests related to Parsl workers being able to access their worker ID"""

from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl import python_app
import pytest


def local_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label="htex_Local",
                worker_debug=True,
                max_workers=4,
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=1,
                    max_blocks=1,
                ),
            )
        ],
        strategy='none',
    )


@python_app
def get_worker_info():
    import os
    rank = int(os.environ['PARSL_WORKER_RANK'])
    size = int(os.environ['PARSL_WORKER_COUNT'])
    pool_id = os.environ['PARSL_WORKER_POOL_ID']
    return rank, size, pool_id


@pytest.mark.local
def test_htex():
    worker_info = [get_worker_info() for _ in range(4)]
    worker_ids, worker_size, pool_info = zip(*[r.result() for r in worker_info])
    assert len(set(worker_info)) > 1  # Tasks should run on >1 worker
    assert len(set(worker_size)) == 1  # All workers have same pool size
    assert len(set(pool_info)) == 1  # All from the same pool
