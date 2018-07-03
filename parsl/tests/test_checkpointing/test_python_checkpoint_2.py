import argparse
import os
import time

import pytest

import parsl
from parsl.app.app import App
from parsl.tests.test_checkpointing.test_python_checkpoint_1 import \
    test_initial_checkpoint_write
from parsl.tests.configs.local_threads_checkpoint import config

rundir = test_initial_checkpoint_write()
config.checkpoint_files = [os.path.join(rundir, 'checkpoint')]

parsl.clear()
parsl.load(config)


@App('python', cache=True)
def slow_double(x, sleep_dur=1):
    import time
    time.sleep(sleep_dur)
    return x * 2


@pytest.mark.local
def test_loading_checkpoint(n=2):
    """2. Load the memoization table from previous checkpoint
    """

    d = {}

    start = time.time()
    print("Launching : ", n)
    for i in range(0, n):
        d[i] = slow_double(i)
    print("Done launching")

    for i in range(0, n):
        d[i].result()
    print("Done sleeping")

    delta = time.time() - start
    assert delta < 1, "Took longer than a second, restore from checkpoint failed"


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_initial_checkpoint_write(n=4)
