import argparse

import pytest

import parsl
from parsl.app.app import python_app
from parsl.config import Config
from parsl.dataflow.memoization import BasicMemoizer
from parsl.executors.threads import ThreadPoolExecutor


def local_config():
    return Config(
        executors=[
            ThreadPoolExecutor(max_threads=4),
        ],
        memoizer=BasicMemoizer(memoize=False)
    )


@python_app(cache=True)
def random_uuid(x):
    import uuid
    return str(uuid.uuid4())


@pytest.mark.local
def test_python_memoization(n=2):
    """Testing python memoization disable via DFK call
    """
    x = random_uuid(0)

    # Force x to completion before running other tests that will
    # potentially re-use (or not-re-use) the result of x. Otherwise,
    # results might not be reused because x is not completed, rather
    # than because app-caching is disabled.

    x.result()

    for i in range(0, n):
        foo = random_uuid(0)
        assert foo.result() != x.result(
        ), "Memoized results were used when memoization was disabled"
