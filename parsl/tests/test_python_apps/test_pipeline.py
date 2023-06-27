import argparse

import pytest

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.local_threads import config


@python_app
def increment(x):
    return x + 1


@python_app
def slow_increment(x):
    import time
    time.sleep(0.001)
    return x + 1


@pytest.mark.parametrize("depth", (2, 3))
def test_increment(depth):
    """Test simple pipeline A->B...->N"""
    futs = [increment(0)]
    futs.extend(increment(futs[i - 1]) for i in range(1, depth))
    assert sum(f.result() for f in futs) == sum(range(1, depth + 1))


@pytest.mark.parametrize("depth", (2, 3))
def test_increment_slow(depth):
    """Test simple pipeline A->B...->N with delay"""
    futs = [slow_increment(0)]
    futs.extend(slow_increment(futs[i - 1]) for i in range(1, depth))
    assert sum(f.result() for f in futs) == sum(range(1, depth + 1))
