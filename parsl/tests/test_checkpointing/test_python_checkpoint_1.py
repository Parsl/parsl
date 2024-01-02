import os
import pytest

import parsl
from parsl import python_app
from parsl.tests.configs.local_threads import fresh_config


def local_config():
    config = fresh_config()
    config.checkpoint_mode = "manual"
    return config


@python_app(cache=True)
def random_app(i):
    import random
    return random.randint(i, 100000)


def launch_n_random(n=2):
    """1. Launch a few apps and write the checkpoint once a few have completed
    """

    d = [random_app(i) for i in range(0, n)]

    # Block till done
    return [i.result() for i in d]


@pytest.mark.local
def test_initial_checkpoint_write(n=2):
    """1. Launch a few apps and write the checkpoint once a few have completed
    """
    launch_n_random(n)

    cpt_dir = parsl.dfk().checkpoint()

    cptpath = cpt_dir + '/dfk.pkl'
    assert os.path.exists(cptpath), f"DFK checkpoint missing: {cptpath}"

    cptpath = cpt_dir + '/tasks.pkl'
    assert os.path.exists(cptpath), f"Tasks checkpoint missing: {cptpath}"
