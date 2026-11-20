import pathlib
import sys

import parsl

from .. import util


@parsl.python_app
def add(a, b):
    return a + b


with util.load(pathlib.Path(sys.argv[1]), heartbeat_threshold=10):
    exc = add(1, 2).exception(timeout=60)
    assert "is incompatible with the interchange" in str(exc), repr(exc)
    assert "is too old for this interchange" in str(exc), repr(exc)
