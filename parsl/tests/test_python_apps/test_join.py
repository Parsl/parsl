import pytest
import time

from parsl import join_app, python_app

from parsl.tests.configs.local_threads import fresh_config

local_config = fresh_config()

RESULT_CONSTANT = 3


@python_app
def inner_app():
    time.sleep(1)
    return RESULT_CONSTANT


@join_app
def outer_app():
    fut = inner_app()
    return fut


@python_app
def add_one(n):
    return n + 1


@python_app
def combine(*args):
    """Wait for an arbitrary list of futures and return them as a list"""
    return args


@join_app
def outer_make_a_dag(n):
    futs = []
    for _ in range(n):
        futs.append(inner_app())
    return combine(*futs)


@python_app
def gen_random():
    import random
    return random.randrange(10)


@python_app
def count(el):
    return len(el)


@pytest.mark.local
def test_result_flow():
    f = outer_app()
    res = f.result()
    assert res == RESULT_CONSTANT


@pytest.mark.local
def test_dependency_on_joined():
    g = add_one(outer_app())
    res = g.result()
    assert res == RESULT_CONSTANT + 1


@pytest.mark.skip('test needs implementing properly')
def test_inner_dag():
    """BENC: note, this test needs turning into a proper pytest test - at the
    moment it always fails because i always raise a value error"""
    f = count(outer_make_a_dag(gen_random()))
    g = count(outer_make_a_dag(gen_random()))
    raise ValueError("BENC: f = {}, g = {}".format(f.result(), g.result()))
