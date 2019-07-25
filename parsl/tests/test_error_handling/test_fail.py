import pytest

from parsl.app.app import python_app


@python_app
def always_fail():
    raise ValueError("This ValueError should propagate to the app caller in fut.result()")


def test_simple():
    with pytest.raises(ValueError):
        fut = always_fail()
        fut.result()
