import argparse
import random

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.local_threads import config


@python_app
def map_one(x, dur):
    import time
    time.sleep(dur)
    return x * 2


@python_app
def map_two(x, dur):
    import time
    time.sleep(dur)
    return x * 5


@python_app
def add_two(x, y, dur):
    import time
    time.sleep(dur)
    return x + y


def test_func_1(width=2):

    fu_1 = []
    for i in range(1, width + 1):
        fu = map_one(i, random.randint(0, 5) / 10)
        fu_1.extend([fu])

    fu_2 = []
    for fu in fu_1:
        fu = map_two(fu, 0)
        fu_2.extend([fu])

    assert sum([i.result() for i in fu_2]) == sum(
        range(1, width + 1)) * 10, "Sums do not match"
    return fu_2


def test_func_2(width=2):

    fu_1 = []
    for i in range(1, width + 1):
        fu = map_one(i, random.randint(0, 5))
        fu_1.extend([fu])

    fu_2 = []
    for i in range(0, width + 1, 2)[0:-1]:
        fu = add_two(fu_1[i], fu_1[i + 1], 0)
        fu_2.extend([fu])

    assert sum([i.result() for i in fu_2]) == sum(
        range(1, width + 1)) * 2, "Sums do not match"
    return fu_2
