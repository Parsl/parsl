import os

from pytest import mark

import parsl
from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl import python_app


@python_app()
def test_function():
    return os.getpid()


@mark.local
def test_spawn_method():
    local_config = Config(
        executors=[
            HighThroughputExecutor(
                label="fork",
                worker_debug=True,
                max_workers=2,
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=1,
                    max_blocks=1,
                ),
                start_method="spawn"
            )
        ],
        strategy=None,
    )

    parsl.load(local_config)

    # Get the PID for the child function as a way of making sure it launches
    future = test_function()
    remote_pid = future.result()
    assert remote_pid != os.getpid()
