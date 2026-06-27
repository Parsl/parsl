"""Tests related to assigning workers to specific compute units"""

import os

import pytest

from parsl import python_app
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider


def local_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label="htex_Local",
                worker_debug=True,
                max_workers_per_node=2,
                cpu_affinity='block',
                available_accelerators=2,
                encrypted=True,
                provider=LocalProvider(
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
    from time import sleep
    rank = int(os.environ['PARSL_WORKER_RANK'])
    aff = os.sched_getaffinity(0)
    device = os.environ.get('CUDA_VISIBLE_DEVICES')
    sleep(1.0)
    return rank, (aff, device)


@pytest.mark.local
@pytest.mark.skipif('sched_getaffinity' not in dir(os), reason='System does not support sched_setaffinity')
@pytest.mark.skipif(os.cpu_count() == 1, reason='Must have a more than one CPU')
def test_htex():
    worker_info = [get_worker_info() for _ in range(4)]
    worker_affinity = dict([r.result() for r in worker_info])
    assert worker_affinity[0] != worker_affinity[1]
    assert worker_affinity[0][1] == "0"  # Make sure it is pinned to the correct CUDA device
