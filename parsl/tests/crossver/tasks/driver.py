import pathlib
import sys

import parsl

from .. import util


@parsl.python_app
def add(a, b):
    return a + b


@parsl.python_app
def fail(message):
    raise ValueError(message)


with util.load(pathlib.Path(sys.argv[1]), heartbeat_threshold=10):
    sums = [add(i, i) for i in range(10)]
    failure = fail("intentional crossver failure")

    assert [f.result(timeout=60) for f in sums] == [2 * i for i in range(10)]

    exc = failure.exception(timeout=60)
    assert isinstance(exc, ValueError), repr(exc)
    assert "intentional crossver failure" in str(exc), repr(exc)
