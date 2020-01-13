import pickle

import pytest

from parsl import python_app, DataFlowKernel
from parsl.utils import time_limited_open


def run_checkpointed(n=2, mode="task_exit"):
    """ This test runs n apps that will fail with Division by zero error,
    followed by 1 app that will succeed. The checkpoint should only have 1 task.
    """

    from parsl.tests.configs.local_threads import config
    config["globals"]["checkpointMode"] = mode
    dfk = DataFlowKernel(config=config)

    @python_app(data_flow_kernel=dfk, cache=True)
    def cached_rand(x):
        import random
        return random.randint(0, 10000)

    @python_app(data_flow_kernel=dfk, cache=True)
    def cached_failing(x):
        5 / 0
        return 1

    items = []
    for i in range(0, n):
        x = cached_failing(0)
        items.append(x)
        try:
            x.result()
        except Exception:
            print("Ignoring failure of task")
            pass

    x = cached_rand(1)
    print(x.result())
    rundir = dfk.rundir
    # Call cleanup *only* for dfk_exit to ensure that a checkpoint is written
    # at all
    if mode == "dfk_exit":
        dfk.cleanup()
    return rundir


@pytest.mark.local
@pytest.mark.skip('hangs intermittently in pytest')
def test_regression_239():
    """Ensure failed tasks are not cached with task_exit mode. Tests #239
    Also tests task_exit behavior.
    """

    rundir = run_checkpointed()
    with time_limited_open("{}/checkpoint/tasks.pkl".format(rundir), 'rb', seconds=2) as f:
        tasks = []
        try:
            while f:
                tasks.append(pickle.load(f))
                print
        except EOFError:
            pass
        print("Tasks from cache : ", tasks)
        assert len(tasks) == 1, "Expected {} checkpoint events, got {}".format(1, len(tasks))


@pytest.mark.local
@pytest.mark.skip('hangs intermittently in pytest')
def test_checkpointing_at_dfk_exit():
    """Ensure failed tasks are not cached with dfk_exit mode. Tests #239
    """

    rundir = run_checkpointed(mode="dfk_exit")
    with time_limited_open("{}/checkpoint/tasks.pkl".format(rundir), 'rb', seconds=2) as f:
        tasks = []
        try:
            while f:
                tasks.append(pickle.load(f))
                print
        except EOFError:
            pass
        print("Tasks from cache : ", tasks)
        assert len(tasks) == 1, "Expected {} checkpoint events, got {}".format(1, len(tasks))


if __name__ == "__main__":
    test_regression_239()
    test_checkpointing_at_dfk_exit()
