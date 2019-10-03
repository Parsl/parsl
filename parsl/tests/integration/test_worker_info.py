"""Tests related to Parsl workers being able to access their worker ID"""

from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl import python_app

local_config = Config(
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
    strategy=None,
)


@python_app
def get_worker_info():
    from time import sleep
    import os
    sleep(2)
    return int(os.environ['PARSL_WORKER_ID'])


def test_htex():
    # Submit several jobs, make sure they get >1 worker id
    worker_ids = [get_worker_info() for _ in range(4)]
    worker_ids = [r.result() for r in worker_ids]
    assert len(set(worker_ids)) > 1
