import logging
import time

import pytest

import parsl
from parsl import File, python_app
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SingleNodeLauncher
from parsl.providers import LocalProvider

logger = logging.getLogger(__name__)

_max_blocks = 5
_min_blocks = 2


def local_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                heartbeat_period=1,
                heartbeat_threshold=2,
                poll_period=100,
                label="htex_local",
                address="127.0.0.1",
                max_workers_per_node=1,
                encrypted=True,
                provider=LocalProvider(
                    init_blocks=0,
                    max_blocks=_max_blocks,
                    min_blocks=_min_blocks,
                    launcher=SingleNodeLauncher(),
                ),
            )
        ],
        max_idletime=0.5,
        strategy='simple',
    )


@python_app
def waiting_app(ident: int, inputs=(), outputs=()):
    import pathlib
    import time

    # Approximate an Event by writing to files; the test logic will poll this file
    with open(outputs[0], "a") as f:
        f.write(f"Ready: {ident}\n")

    # Similarly, use Event approximation (file check!) by polling.
    may_finish_file = pathlib.Path(inputs[0])
    while not may_finish_file.exists():
        time.sleep(0.01)


# see issue #1885 for details of failures of this test.
# at the time of issue #1885 this test was failing frequently
# in CI.
@pytest.mark.local
def test_scale_out(tmpd_cwd, try_assert):
    dfk = parsl.dfk()

    num_managers = len(dfk.executors['htex_local'].connected_managers())

    assert num_managers == 0, "Expected 0 managers at start"
    assert dfk.executors['htex_local'].outstanding == 0, "Expected 0 tasks at start"

    ntasks = 10
    ready_path = tmpd_cwd / "workers_ready"
    finish_path = tmpd_cwd / "workers_may_continue"
    ready_path.touch()
    inputs = [File(finish_path)]
    outputs = [File(ready_path)]

    futs = [waiting_app(i, outputs=outputs, inputs=inputs) for i in range(ntasks)]

    while ready_path.read_text().count("\n") < _max_blocks:
        time.sleep(0.5)

    assert len(dfk.executors['htex_local'].connected_managers()) == _max_blocks

    finish_path.touch()  # Approximation of Event, via files
    [x.result() for x in futs]

    assert dfk.executors['htex_local'].outstanding == 0

    def assert_kernel():
        return len(dfk.executors['htex_local'].connected_managers()) == _min_blocks

    try_assert(
        assert_kernel,
        fail_msg=f"Expected {_min_blocks} managers when no tasks (min_blocks)",
        timeout_ms=15000,
    )
