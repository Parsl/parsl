import pytest
import time

from parsl import join_app, python_app
from parsl.dataflow.errors import JoinError

from parsl.tests.configs.local_threads import fresh_config as local_config

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


class InnerError(RuntimeError):
    pass


@python_app
def inner_error(s="X"):
    raise InnerError("Error " + s)


@join_app
def outer_error():
    return inner_error()


@join_app
def outer_two_errors():
    return [inner_error("A"), inner_error("B")]


@join_app
def outer_one_error_one_result():
    return [inner_error("A"), inner_app()]


def test_error():
    f = outer_error()
    e = f.exception()
    assert isinstance(e, JoinError)
    assert isinstance(e.dependent_exceptions_tids[0][0], InnerError)


def test_two_errors():
    f = outer_two_errors()
    e = f.exception()
    assert isinstance(e, JoinError)
    assert len(e.dependent_exceptions_tids) == 2

    de0 = e.dependent_exceptions_tids[0][0]
    assert isinstance(de0, InnerError)
    assert de0.args[0] == "Error A"

    de1 = e.dependent_exceptions_tids[1][0]
    assert isinstance(de1, InnerError)
    assert de1.args[0] == "Error B"


def test_one_error_one_result():
    f = outer_one_error_one_result()
    e = f.exception()

    assert isinstance(e, JoinError)
    assert len(e.dependent_exceptions_tids) == 1

    de0 = e.dependent_exceptions_tids[0][0]
    assert isinstance(de0, InnerError)
    assert de0.args[0] == "Error A"
