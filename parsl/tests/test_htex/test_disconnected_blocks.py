import logging

import pytest

import parsl
from parsl import Config
from parsl.executors import HighThroughputExecutor
from parsl.executors.errors import BadStateException
from parsl.jobs.states import JobState, JobStatus
from parsl.providers import LocalProvider

logger = logging.getLogger(__name__)


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
    logger.info("BENC: 1")
    dfk = parsl.dfk()
    logger.info("BENC: 2")
    executor = dfk.executors["HTEX"]
    logger.info("BENC: 3")

    connected_blocks = executor.connected_blocks()
    logger.info("BENC: 4")
    assert not connected_blocks, "Expected 0 blocks"
    logger.info("BENC: 5")

    future = double(5)
    logger.info("BENC: 6")
    with pytest.raises(BadStateException):
        logger.info("BENC: 7")
        future.result()
        logger.info("BENC: 8")
    logger.info("BENC: 9")

    assert isinstance(future.exception(), BadStateException)
    logger.info("BENC: 10")
    exception_body = str(future.exception())
    assert "EXIT CODE: 0" in exception_body
    logger.info("BENC: 11")

    status_dict = executor.status()
    logger.info("BENC: 12")
    assert len(status_dict) == 1, "Expected only 1 block"
    for status in status_dict.values():
        assert isinstance(status, JobStatus)
        assert status.state == JobState.MISSING

    logger.info("BENC: 13")
    connected_blocks = executor.connected_blocks()
    logger.info("BENC: 14")
    assert not connected_blocks, "Expected 0 blocks"
