from concurrent.futures import Future
from typing import TypeVar

import pytest

from parsl import python_app

T = TypeVar('T')


@python_app
def returns_a_dict() -> dict:
    return {"a": "X", "b": "Y"}


@python_app
def returns_a_list() -> list:
    return ["X", "Y"]


@python_app
def returns_a_tuple() -> tuple:
    return ("X", "Y")


@python_app
def returns_a_class() -> type:
    from dataclasses import dataclass

    @dataclass
    class MyClass:
        a: str = "X"
        b: str = "Y"

    return MyClass


class MyOuterClass():
    def __init__(self):
        self.q = "A"
        self.r = "B"


@python_app
def returns_a_class_instance() -> object:
    return MyOuterClass()


def test_returns_a_dict():

    # precondition that returns_a_dict behaves
    # correctly
    assert returns_a_dict().result()["a"] == "X"

    # check that the deferred __getitem__ functionality works,
    # allowing [] to be used on an AppFuture
    assert returns_a_dict()["a"].result() == "X"


def test_returns_a_list():

    # precondition that returns_a_list behaves
    # correctly
    assert returns_a_list().result()[0] == "X"

    # check that the deferred __getitem__ functionality works,
    # allowing [] to be used on an AppFuture
    assert returns_a_list()[0].result() == "X"


def test_returns_a_tuple():

    # precondition that returns_a_tuple behaves
    # correctly
    assert returns_a_tuple().result()[0] == "X"

    # check that the deferred __getitem__ functionality works,
    # allowing [] to be used on an AppFuture
    assert returns_a_tuple()[0].result() == "X"


def test_lifted_getitem_on_dict_bad_key():
    assert isinstance(returns_a_dict()["invalid"].exception(), KeyError)


def test_returns_a_class_instance():
    # precondition
    assert returns_a_class_instance().result().q == "A"

    # test of commuting . and result()
    assert returns_a_class_instance().q.result() == "A"


def test_returns_a_class_instance_no_underscores():
    # test that _underscore attribute references are not lifted
    f = returns_a_class_instance()
    with pytest.raises(AttributeError):
        f._nosuchattribute.result()
    f.exception()  # wait for f to complete before the test ends


def test_returns_a_class():

    # precondition that returns_a_class behaves
    # correctly
    assert returns_a_class().result().a == "X"

    # check that the deferred __getattr__ functionality works,
    # allowing [] to be used on an AppFuture
    assert returns_a_class().a.result() == "X"

    # when the result is not indexable, a sensible error should
    # appear in the appropriate future


@python_app
def passthrough(v: T) -> T:
    return v


def test_lifted_getitem_ordering():
    # this should test that lifting getitem has the correct execution
    # order: that it does not defer the execution of following code

    f_prereq = Future()

    f_top = passthrough(f_prereq)

    f_a = f_top['a']

    # lifted ['a'] should not defer execution here (so it should not
    # implicitly call result() on f_top). If it does, this test will
    # hang at this point, waiting for f_top to get a value, which
    # will not happen until f_prereq gets a value..
    # which doesn't happen until:

    f_prereq.set_result({"a": "X"})

    # now at this point it should be safe to wait for f_a to get a result
    # while passthrough and lifted getitem run...

    assert f_a.result() == "X"
