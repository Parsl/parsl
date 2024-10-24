from threading import Event

import pytest

import parsl
from parsl import File, python_app
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SingleNodeLauncher
from parsl.providers import LocalProvider

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
        strategy='htex_auto_scale',
        strategy_period=0.1
    )


@python_app
def waiting_app(ident: int, outputs=(), inputs=()):
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

    ntasks = _max_blocks * 2
    ready_path = tmpd_cwd / "workers_ready"
    finish_path = tmpd_cwd / "stage1_workers_may_continue"
    ready_path.touch()
    inputs = [File(finish_path)]
    outputs = [File(ready_path)]

    futs = [waiting_app(i, outputs=outputs, inputs=inputs) for i in range(ntasks)]

    try_assert(lambda: ready_path.read_text().count("\n") == _max_blocks, "Wait for _max_blocks tasks to be running", timeout_ms=15000)

    # This should be true immediately, because the previous try_assert should
    # wait until there are max_blocks tasks running, and his test should be
    # configured to use 1 worker per block.
    assert len(dfk.executors['htex_local'].connected_managers()) == _max_blocks

    finish_path.touch()  # Approximation of Event, via files
    [x.result() for x in futs]

    assert dfk.executors['htex_local'].outstanding == 0

    # now we can launch one "long" task -
    # and what should happen is that the connected_managers count "eventually" (?) converges to 1 and stays there.

    finish_path = tmpd_cwd / "stage2_workers_may_continue"

    fut = waiting_app(0, outputs=outputs, inputs=[File(finish_path)])

    def check_one_block():
        return len(dfk.executors['htex_local'].connected_managers()) == 1

    try_assert(
        check_one_block,
        fail_msg="Expected 1 managers during a single long task",
    )

    # the task should not have finished by the time we end up with 1 manager
    assert not fut.done()

    # This section wait for the strategy to run again, with the above single
    # task outstanding, and check that the strategy has not scaled up or
    # down more on those subsequent iterations.

    # It does this by hooking the callback of the job status poller, and
    # waiting until it has run.

    old_cb = dfk.job_status_poller.callback

    strategy_iterated = Event()

    def hook_cb(*args, **kwargs):
        r = old_cb(*args, **kwargs)
        strategy_iterated.set()
        return r

    dfk.job_status_poller.callback = hook_cb

    # hack strategies to run more frequently. this allo
    # dfk.job_status_poller.

    try_assert(
        strategy_iterated.is_set,
        fail_msg="Expected strategy to have run within this period",
    )

    assert check_one_block()

    finish_path.touch()  # now we can end the single stage-2 task

    fut.result()

    # now we should expect min_blocks scale down

    def check_min_blocks():
        return len(dfk.executors['htex_local'].connected_managers()) == _min_blocks

    try_assert(
        check_min_blocks,
        fail_msg=f"Expected {_min_blocks} managers when no tasks (min_blocks)",
    )
