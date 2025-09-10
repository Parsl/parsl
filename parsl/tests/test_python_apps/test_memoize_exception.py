import parsl
from parsl.app.app import python_app


@python_app(cache=True)
def raise_exception_cache(x, cache=True):
    raise RuntimeError("exception from raise_exception_cache")


@python_app(cache=False)
def raise_exception_nocache(x, cache=True):
    raise RuntimeError("exception from raise_exception_nocache")


def test_python_memoization(n=2):
    """Test Python memoization of exceptions, with cache=True"""
    x = raise_exception_cache(0)

    # wait for x to be done
    x.exception()

    for i in range(0, n):
        fut = raise_exception_cache(0)

        # check that we get back the same exception object, rather than
        # a new one from a second invocation of raise_exception().
        assert fut.exception() is x.exception(), "Memoized exception should have been memoized"


def test_python_no_memoization(n=2):
    """Test Python non-memoization of exceptions, with cache=False"""
    x = raise_exception_nocache(0)

    # wait for x to be done
    x.exception()

    for i in range(0, n):
        fut = raise_exception_nocache(0)

        # check that we get back a different exception object each time
        assert fut.exception() is not x.exception(), "Memoized exception should have been memoized"
