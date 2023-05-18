import argparse
import time

import pytest

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.local_threads import config


local_config = config


@python_app
def double(x):
    return x * 2


def plain_double(x):
    return x * 2


@pytest.mark.skip('not asserting anything')
def test_plain(n=2):
    start = time.time()
    x = []
    for i in range(0, n):
        x.extend([plain_double(i)])

    print(sum(x))

    ttc = time.time() - start
    print("Total time : ", ttc)

    return ttc


@pytest.mark.skip('not asserting anything')
def test_parallel(n=2):
    start = time.time()
    x = []
    for i in range(0, n):
        x.extend([double(i)])

    print(sum([i.result() for i in x]))

    ttc = time.time() - start
    print("Total time : ", ttc)

    return ttc


@pytest.mark.skip('not asserting anything')
def test_parallel2(n=2):
    start = time.time()
    x = []
    for i in range(0, n):
        x.extend([double(i)])

    ttc = time.time() - start
    print("Total time : ", ttc)

    return ttc
