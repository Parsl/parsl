from parsl import python_app
from concurrent.futures import Future


@python_app
def returns_a_dict():
    return {"a": "X", "b": "Y"}


@python_app
def returns_a_list():
    return [5, 6, 7, 8, 9]


@python_app
def passthrough(v):
    return v


def test_lifted_getitem_on_dict():
    # check that the deferred __getitem__ functionality works,
    # allowing [] to be used on an AppFuture
    assert returns_a_dict()["a"].result() == "X"


def test_lifted_getitem_on_dict_bad_key():
    assert isinstance(returns_a_dict()["invalid"].exception(), KeyError)


def test_lifted_getitem_on_list():
    assert returns_a_list()[2].result() == 7


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
