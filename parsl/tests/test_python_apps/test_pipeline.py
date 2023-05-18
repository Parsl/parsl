import argparse

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.local_threads import config


@python_app
def increment(x):
    return x + 1


@python_app
def slow_increment(x, dur):
    import time
    time.sleep(dur)
    return x + 1


def test_increment(depth=2):
    """Test simple pipeline A->B...->N
    """
    futs = {0: 0}
    for i in range(1, depth):
        futs[i] = increment(futs[i - 1])

    print([futs[i].result() for i in futs if not isinstance(futs[i], int)])


def test_increment_slow(depth=2):
    """Test simple pipeline A->B...->N with delay
    """
    futs = {0: 0}
    for i in range(1, depth):
        futs[i] = slow_increment(futs[i - 1], 0.5)

    print(futs[i])
    print([futs[i].result() for i in futs if not isinstance(futs[i], int)])
