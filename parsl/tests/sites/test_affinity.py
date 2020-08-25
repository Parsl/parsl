"""Tests related to Parsl workers being able to access their worker ID"""

from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl import python_app
import pytest
import os

local_config = Config(
    executors=[
        HighThroughputExecutor(
            label="htex_Local",
            worker_debug=True,
            max_workers=2,
            cpu_affinity='block',
            provider=LocalProvider(
                channel=LocalChannel(),
                init_blocks=1,
                max_blocks=1,
            ),
        )
    ],
    strategy=None,
)


@python_app
def get_worker_info():
    from time import sleep
    import os
    rank = int(os.environ['PARSL_WORKER_RANK'])
    aff = os.sched_getaffinity(0)
    sleep(1.0)
    return rank, aff


@pytest.mark.local
@pytest.mark.skipif('sched_getaffinity' not in dir(os), reason='System does not support sched_setaffinity')
@pytest.mark.skipif(os.cpu_count() == 1, reason='Must have a more than one CPU')
def test_htex():
    worker_info = [get_worker_info() for _ in range(4)]
    worker_affinity = dict([r.result() for r in worker_info])
    assert worker_affinity[0] != worker_affinity[1]
