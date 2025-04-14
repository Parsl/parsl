import argparse

import parsl
from parsl.app.app import python_app


@python_app(cache=True)
def raise_exception(x, cache=True):
    raise RuntimeError("exception from raise_exception")


def test_python_memoization(n=2):
    """Testing python memoization with exceptions."""
    x = raise_exception(0)

    # wait for x to be done
    x.exception()

    for i in range(0, n):
        foo = raise_exception(0)
        print(foo.exception())
        assert foo.exception() == x.exception(), "Memoized exceptions were not used"
