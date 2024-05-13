import pytest

from parsl import join_app, python_app
from parsl.dataflow.errors import JoinError

RESULT_CONSTANT = 3


@python_app
def inner_app():
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
    return combine(*(inner_app() for _ in range(n)))


@join_app
def outer_make_a_dag_multi(n):
    return [inner_app() for _ in range(n)]


def test_result_flow():
    f = outer_app()
    assert f.result() == RESULT_CONSTANT


@join_app
def join_wrong_type_app():
    return 3


def test_wrong_type():
    f = join_wrong_type_app()
    with pytest.raises(TypeError):
        f.result()


def test_dependency_on_joined():
    g = add_one(outer_app())
    assert g.result() == RESULT_CONSTANT + 1


def test_combine():
    f = outer_make_a_dag_combine(inner_app())
    assert f.result() == [RESULT_CONSTANT] * RESULT_CONSTANT


def test_multiple_return():
    f = outer_make_a_dag_multi(inner_app())
    assert f.result() == [RESULT_CONSTANT] * RESULT_CONSTANT


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

    assert len(e.dependent_exceptions_tids) == 1
    assert isinstance(e.dependent_exceptions_tids[0][0], InnerError)
    assert e.dependent_exceptions_tids[0][1].startswith("task ")


def test_two_errors():
    f = outer_two_errors()
    e = f.exception()
    assert isinstance(e, JoinError)
    assert len(e.dependent_exceptions_tids) == 2

    de0 = e.dependent_exceptions_tids[0][0]
    assert isinstance(de0, InnerError)
    assert de0.args[0] == "Error A"
    assert e.dependent_exceptions_tids[0][1].startswith("task ")

    de1 = e.dependent_exceptions_tids[1][0]
    assert isinstance(de1, InnerError)
    assert de1.args[0] == "Error B"
    assert e.dependent_exceptions_tids[1][1].startswith("task ")


def test_one_error_one_result():
    f = outer_one_error_one_result()
    e = f.exception()

    assert isinstance(e, JoinError)
    assert len(e.dependent_exceptions_tids) == 1

    de0 = e.dependent_exceptions_tids[0][0]
    assert isinstance(de0, InnerError)
    assert de0.args[0] == "Error A"
    assert e.dependent_exceptions_tids[0][1].startswith("task ")


@join_app
def app_no_futures():
    return []


def test_no_futures():
    # tests that a list of futures that contains no futures will
    # complete - regression test for issue #2792
    f = app_no_futures()
    assert f.result() == []
