import pytest

import parsl
from parsl import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider


def local_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label="HTEX",
                heartbeat_period=1,
                heartbeat_threshold=2,
                poll_period=100,
                address="127.0.0.1",
                max_workers_per_node=1,
                provider=LocalProvider(
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
def test_get_connected_blocks():
    """Test reporting of connected blocks from HTEX"""
    dfk = parsl.dfk()
    executor = dfk.executors["HTEX"]

    connected_blocks = executor.connected_blocks()
    assert not connected_blocks, "Expected 0 blocks"

    blocking_task = double(5).result()
    assert blocking_task == 10

    connected_blocks = executor.connected_blocks()
    assert len(connected_blocks) == 1, "Expected 1 block"

    executor.scale_in(1)

    connected_blocks = executor.connected_blocks()
    assert len(connected_blocks) == 1, "Expected 1 block"

    blocking_task = double(5).result()
    assert blocking_task == 10

    # With the first block scaled_in there should be reporting
    # 2 blocks including the older one.
    connected_blocks = executor.connected_blocks()
    assert len(connected_blocks) == 2, "Expected 2 blocks"
