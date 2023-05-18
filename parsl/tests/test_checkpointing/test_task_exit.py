import argparse
import pickle

import pytest

import parsl
from parsl.app.app import python_app
from parsl.utils import time_limited_open
from parsl.tests.configs.local_threads_checkpoint_task_exit import config


def local_setup():
    global dfk
    dfk = parsl.load(config)


def local_teardown():
    parsl.dfk().cleanup
    parsl.clear()


@python_app(cache=True)
def slow_double(x, sleep_dur=1):
    import time
    time.sleep(sleep_dur)
    return x * 2


@pytest.mark.local
def test_at_task_exit(n=2):
    """Test checkpointing at task_exit behavior
    """

    d = {}

    print("Launching: ", n)
    for i in range(0, n):
        d[i] = slow_double(i)
    print("Done launching")

    for i in range(0, n):
        d[i].result()

    # There are two potential race conditions here which
    # might be useful to be aware of if debugging this test.

    #  i) .result() returning does not necessarily mean that
    #     a checkpoint that has been written: it means that the
    #     AppFuture has had its result written. In the DFK
    #     implementation at time of writing, .result() returning
    #     does not indicate that a checkpoint has been written,
    #     it seems like.

    # ii) time_limited_open has a specific time limit in it.
    #     While this limit might seem generous at time of writing,
    #     it should be remembered that this is still a race.

    with time_limited_open("{}/checkpoint/tasks.pkl".format(dfk.run_dir), 'rb', seconds=5) as f:
        tasks = []
        try:
            while f:
                tasks.append(pickle.load(f))
        except EOFError:
            pass

        assert len(tasks) == n, "Expected {} checkpoint events, got {}".format(n, len(tasks))
