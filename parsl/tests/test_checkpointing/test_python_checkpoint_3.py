import os

import pytest

import parsl
from parsl.app.app import App
from parsl.tests.configs.local_threads import config

parsl.clear()
dfk = parsl.load(config)


@App('python')
def slow_double(x, sleep_dur=1, cache=True):
    import time
    time.sleep(sleep_dur)
    return x * 2

@pytest.mark.local
def test_checkpointing():
    """Testing code snippet from documentation
    """

    N = 5  # Number of calls to slow_double
    d = []  # List to store the futures
    for i in range(0, N):
        d.append(slow_double(i))

    # Wait for the results
    [i.result() for i in d]

    cpt_dir = dfk.checkpoint()
    print(cpt_dir)  # Prints the checkpoint dir

    assert os.path.exists(cpt_dir), "Checkpoint dir does not exist"
