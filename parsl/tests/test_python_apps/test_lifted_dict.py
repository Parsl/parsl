from parsl import python_app


@python_app
def returns_a_dict():
    return {"a": "X", "b": "Y"}


def test_returns_a_dict():

    # precondition that returns_a_dict behaves
    # correctly
    assert returns_a_dict().result()["a"] == "X"

    # check that the deferred __getitem__ functionality works,
    # allowing [] to be used on an AppFuture
    assert returns_a_dict()["a"].result() == "X"

    # other things to test: when the result is a sequence, so that
    # [] is a position

    # when the result is not indexable, a sensible error should
    # appear in the appropriate future
