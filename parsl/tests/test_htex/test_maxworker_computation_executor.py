import random

import pytest

import parsl
from parsl.config import Config
from parsl.executors.high_throughput.executor import HighThroughputExecutor
from parsl.providers import LocalProvider


@pytest.mark.local
def test_max_workers_per_node():

    w = random.randint(1, 8)
    h = HighThroughputExecutor(max_workers_per_node=w,
                               provider=LocalProvider(max_blocks=0, init_blocks=0, min_blocks=0)
                               )
    c = Config(executors=[h])

    with parsl.load(c):
        # tests the parameter was wired all the way through
        assert h.max_workers_per_node == w

        # tests that the calculation chose the same number
        # in the absence of other restrictions
        assert h.workers_per_node == w


@pytest.mark.local
def test_cores():

    p = LocalProvider(max_blocks=0, init_blocks=0, min_blocks=0)
    p.cores_per_node = 8

    h = HighThroughputExecutor(max_workers_per_node=16,
                               provider=p,
                               cores_per_worker=4
                               )
    c = Config(executors=[h])

    with parsl.load(c):
        # tests the parameter was wired all the way through
        assert h.cores_per_worker == 4

        # 8 cores divided out at 4-per-worker
        assert h.workers_per_node == 2


@pytest.mark.local
def test_mem():

    p = LocalProvider(max_blocks=0, init_blocks=0, min_blocks=0)
    p.cores_per_node = 8
    p.mem_per_node = 16

    h = HighThroughputExecutor(max_workers_per_node=16,
                               provider=p,
                               cores_per_worker=4,
                               mem_per_worker=16
                               )
    c = Config(executors=[h])

    with parsl.load(c):
        # tests the parameter was wired all the way through
        assert h.mem_per_worker == 16

        # one worker gets all the memory
        assert h.workers_per_node == 1


@pytest.mark.local
def test_accelerators():

    p = LocalProvider(max_blocks=0, init_blocks=0, min_blocks=0)
    p.cores_per_node = 16
    p.mem_per_node = 16

    h = HighThroughputExecutor(max_workers_per_node=16,
                               provider=p,
                               cores_per_worker=4,
                               mem_per_worker=2,
                               available_accelerators=3
                               )
    c = Config(executors=[h])

    with parsl.load(c):

        # number of accelerators is worker constraining limit
        assert h.workers_per_node == 3
