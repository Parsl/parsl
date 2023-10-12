from parsl import python_app


@python_app
def returns_a_dict():
    return {"a": "X", "b": "Y"}


@python_app
def returns_a_list():
    return ["X", "Y"]


@python_app
def returns_a_tuple():
    return ("X", "Y")


@python_app
def returns_a_class():
    from dataclasses import dataclass

    @dataclass
    class MyClass:
        a: str = "X"
        b: str = "Y"

    return MyClass


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


def test_returns_a_class():

    # precondition that returns_a_class behaves
    # correctly
    assert returns_a_class().result().a == "X"

    # check that the deferred __getitem__ functionality works,
    # allowing [] to be used on an AppFuture
    assert returns_a_class().a.result() == "X"

    # when the result is not indexable, a sensible error should
    # appear in the appropriate future
