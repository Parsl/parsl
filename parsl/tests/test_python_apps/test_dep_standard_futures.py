from concurrent.futures import Future

import parsl
from parsl.dataflow.errors import DependencyError


@parsl.python_app
def copy_app(v):
    return v


def test_future_result_dependency():

    plain_fut = Future()

    parsl_fut = copy_app(plain_fut)

    assert not parsl_fut.done()

    message = "Test"

    plain_fut.set_result(message)

    assert parsl_fut.result() == message


def test_future_fail_dependency():

    plain_fut = Future()

    parsl_fut = copy_app(plain_fut)

    assert not parsl_fut.done()

    plain_fut.set_exception(ValueError("Plain failure"))

    ex = parsl_fut.exception()

    # check that what we got is a dependency error...
    assert isinstance(ex, DependencyError)

    # and that the dependency error string mentions the dependency
    # Future, plain_fut, somewhere in its str

    assert repr(plain_fut) in str(ex)
    assert len(ex.dependent_exceptions_tids) == 1
    assert isinstance(ex.dependent_exceptions_tids[0][0], ValueError)
    assert ex.dependent_exceptions_tids[0][1].startswith("<Future ")
