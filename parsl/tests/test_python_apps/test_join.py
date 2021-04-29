import pytest
import time

from parsl import join_app, python_app

from parsl.tests.configs.local_threads import fresh_config

local_config = fresh_config()

RESULT_CONSTANT = 3


@python_app(cache=True)
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
    return list(args)


@join_app
def outer_make_a_dag_combine(n):
    futs = []
    for _ in range(n):
        futs.append(inner_app())
    return combine(*futs)


@join_app
def outer_make_a_dag_multi(n):
    futs = []
    for _ in range(n):
        futs.append(inner_app())
    return futs


def test_result_flow():
    f = outer_app()
    res = f.result()
    assert res == RESULT_CONSTANT


@join_app
def join_wrong_type_app():
    return 3


def test_wrong_type():
    f = join_wrong_type_app()
    with pytest.raises(TypeError):
        f.result()


def test_dependency_on_joined():
    g = add_one(outer_app())
    res = g.result()
    assert res == RESULT_CONSTANT + 1


def test_combine():
    f = outer_make_a_dag_combine(inner_app())
    res = f.result()
    assert res == [RESULT_CONSTANT] * RESULT_CONSTANT


def test_multiple_return():
    f = outer_make_a_dag_multi(inner_app())
    res = f.result()
    assert res == [RESULT_CONSTANT] * RESULT_CONSTANT
