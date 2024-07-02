import logging

import pytest

import parsl
from parsl import Config
from parsl.executors import HighThroughputExecutor
from parsl.executors.errors import BadStateException
from parsl.jobs.states import JobState, JobStatus
from parsl.providers import LocalProvider


def local_config():
    """Config to simulate failing blocks without connecting"""
    return Config(
        executors=[
            HighThroughputExecutor(
                label="HTEX",
                heartbeat_period=1,
                heartbeat_threshold=2,
                poll_period=100,
                max_workers_per_node=1,
                provider=LocalProvider(
                    worker_init="conda deactivate; export PATH=''; which python; exit 0",
                    init_blocks=0,
                    max_blocks=2,
                    min_blocks=0,
                ),
            )
        ],
        max_idletime=0.5,
        strategy='htex_auto_scale',
    )


@parsl.python_app
def double(x):
    return x * 2


@pytest.mark.local
def test_disconnected_blocks():
    """Test reporting of blocks that fail to connect from HTEX"""
    dfk = parsl.dfk()
    executor = dfk.executors["HTEX"]

    connected_blocks = executor.connected_blocks()
    assert not connected_blocks, "Expected 0 blocks"

    future = double(5)
    with pytest.raises(BadStateException):
        future.result()

    assert isinstance(future.exception(), BadStateException)
    exception_body = str(future.exception())
    assert "EXIT CODE: 0" in exception_body

    status_dict = executor.status()
    assert len(status_dict) == 1, "Expected only 1 block"
    for status in status_dict.values():
        assert isinstance(status, JobStatus)
        assert status.state == JobState.MISSING

    connected_blocks = executor.connected_blocks()
    assert not connected_blocks, "Expected 0 blocks"
