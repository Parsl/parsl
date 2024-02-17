import logging
import time

import pytest

import parsl

from parsl import File, python_app
from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.launchers import SingleNodeLauncher
from parsl.config import Config
from parsl.executors import HighThroughputExecutor

logger = logging.getLogger(__name__)

_max_blocks = 5
_min_blocks = 0


def local_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                heartbeat_period=1,
                heartbeat_threshold=2,
                poll_period=100,
                label="htex_local",
                address="127.0.0.1",
                max_workers=1,
                encrypted=True,
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=0,
                    max_blocks=_max_blocks,
                    min_blocks=_min_blocks,
                    launcher=SingleNodeLauncher(),
                ),
            )
        ],
        max_idletime=0.5,
        strategy='htex_auto_scale',
    )


@python_app
def sleep_TODO_events():
    import time
    time.sleep(20)


@python_app
def waiting_app(ident: int, inputs=()):
    import pathlib
    import time

    # Approximate an Event by writing to files; the test logic will poll this file
    with open(inputs[0], "a") as f:
        f.write(f"Ready: {ident}\n")

    # Similarly, use Event approximation (file check!) by polling.
    may_finish_file = pathlib.Path(inputs[1])
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

    ntasks = _max_blocks * 2
    ready_path = tmpd_cwd / "workers_ready"
    finish_path = tmpd_cwd / "workers_may_continue"
    ready_path.touch()
    inputs = [File(str(ready_path)), File(str(finish_path))]

    futs = [waiting_app(i, inputs=inputs) for i in range(ntasks)]

    while ready_path.read_text().count("\n") < _max_blocks:
        time.sleep(0.5)

    assert len(dfk.executors['htex_local'].connected_managers()) == _max_blocks

    finish_path.touch()  # Approximation of Event, via files
    [x.result() for x in futs]

    assert dfk.executors['htex_local'].outstanding == 0

    # now we can launch one "long" task - and what should happen is that the connected_managers count "eventually" (?) converges to 1 and stays there.

    fut = sleep_TODO_events()

    def check_one_block():
        return len(dfk.executors['htex_local'].connected_managers()) == 1

    try_assert(
        check_one_block,
        fail_msg="Expected 1 managers during a single long task",
        timeout_ms=15000,
    )

    # the task should not have finished by the time we end up with 1 manager
    assert not fut.done()

    # but now I want to test that we don't immediately converge to
    # min_blocks but that, for example, there is one strategy pass or
    # something like that?

    # this will give some strategy passes... is there an event driven way
    # of doing this?

    time.sleep(10)

    # this interacts with the sleep in the task... it needs to be the right
    # size to still be inside the task even after we've waited for the
    # partial scale down to happen, so that the following assertion can fire
    # properly

    assert check_one_block()

    fut.result()

    # now we should expect min_blocks scale down

    def check_min_blocks():
        return len(dfk.executors['htex_local'].connected_managers()) == _min_blocks

    try_assert(
        check_min_blocks,
        fail_msg=f"Expected {_min_blocks} managers when no tasks (min_blocks)",
        timeout_ms=15000,
    )
