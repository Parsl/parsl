import pytest
import parsl
import time
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider
from parsl.launchers import SimpleLauncher
from parsl.config import Config


_init_blocks = 1
_min_blocks = 3


def local_setup():
    config = Config(
        executors=[
            HighThroughputExecutor(
                label="HTEX",
                heartbeat_period=1,
                heartbeat_threshold=2,
                poll_period=100,
                address="127.0.0.1",
                max_workers_per_node=1,
                provider=LocalProvider(
                    init_blocks=_init_blocks,
                    max_blocks=4,
                    min_blocks=_min_blocks,
                    launcher=SimpleLauncher()
                ),
            )
        ],
        max_idletime=0.5,
        strategy='simple',
    )
    parsl.load(config)


def local_teardown():
    parsl.dfk().cleanup()
    parsl.clear()


@pytest.mark.local
def test_scaling_out():
    # status_polling_interval for LocalProvider is 5s
    # sleeping >10 will scale out to min blocks

    assert len(parsl.dfk().executors["HTEX"].connected_managers()) != _min_blocks

    time.sleep(15)

    assert len(parsl.dfk().executors["HTEX"].connected_managers()) == _min_blocks
