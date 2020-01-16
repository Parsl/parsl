import argparse
import os
import pytest

import parsl
from parsl import python_app
from parsl.tests.configs.local_threads import config


@python_app(cache=True)
def random_app(i):
    import random
    return random.randint(i, 100000)


def launch_n_random(n=2):
    """1. Launch a few apps and write the checkpoint once a few have completed
    """

    d = [random_app(i) for i in range(0, n)]
    print("Done launching")

    # Block till done
    return [i.result() for i in d]


@pytest.mark.local
def test_initial_checkpoint_write(n=2):
    """1. Launch a few apps and write the checkpoint once a few have completed
    """
    parsl.load(config)
    results = launch_n_random(n)

    cpt_dir = parsl.dfk().checkpoint()

    cptpath = cpt_dir + '/dfk.pkl'
    print("Path exists : ", os.path.exists(cptpath))
    assert os.path.exists(
        cptpath), "DFK checkpoint missing: {0}".format(cptpath)

    cptpath = cpt_dir + '/tasks.pkl'
    print("Path exists : ", os.path.exists(cptpath))
    assert os.path.exists(
        cptpath), "Tasks checkpoint missing: {0}".format(cptpath)

    run_dir = parsl.dfk().run_dir
    parsl.clear()

    return run_dir, results


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
