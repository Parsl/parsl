import argparse
import os
import time

import pytest

import parsl
from parsl.app.app import App
import parsl.tests.test_checkpointing.test_python_checkpoint_1 as test1
from parsl.tests.configs.local_threads import config
from parsl.tests.configs.local_threads_checkpoint import fresh_config


@App('python', cache=True)
def slow_double(x, sleep_dur=1):
    import time
    time.sleep(sleep_dur)
    return x * 2


@pytest.mark.local
def test_loading_checkpoint(n=2):
    """Load memoization table from previous checkpoint
    """

    parsl.load(config)
    rundir = test1.test_initial_checkpoint_write()
    parsl.clear()

    local_config = fresh_config()
    local_config.checkpoint_files = [os.path.join(rundir, 'checkpoint')]
    parsl.load(local_config)

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
    assert delta < 1, "Took longer than a second ({}), assuming restore from checkpoint failed".format(delta)
    parsl.clear()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_loading_checkpoint()
