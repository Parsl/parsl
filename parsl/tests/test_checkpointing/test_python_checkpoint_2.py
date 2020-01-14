import argparse
import os
import pytest
import parsl
from parsl import python_app

from parsl.tests.configs.local_threads import config
from parsl.tests.configs.local_threads_checkpoint import fresh_config


@python_app(cache=True)
def random_app(i):
    import random
    return random.randint(i, 100000)


def launch_n_random(n=2):
    """1. Launch a few apps and write the checkpoint once a few have completed
    """
    d = [random_app(i) for i in range(0, n)]
    return [i.result() for i in d]


@pytest.mark.local
def test_loading_checkpoint(n=2):
    """Load memoization table from previous checkpoint
    """
    config.checkpoint_mode = 'task_exit'
    parsl.load(config)
    results = launch_n_random(n)
    rundir = parsl.dfk().run_dir
    parsl.dfk().cleanup()
    parsl.clear()

    local_config = fresh_config()
    local_config.checkpoint_files = [os.path.join(rundir, 'checkpoint')]
    parsl.load(local_config)

    relaunched = launch_n_random(n)
    assert len(relaunched) == len(results) == n, "Expected all results to have n items"

    for i in range(n):
        assert relaunched[i] == results[i], "Expected relaunched to contain cached results from first run"
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
