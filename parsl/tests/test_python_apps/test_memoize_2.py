import argparse

import pytest

import parsl
from parsl.app.app import python_app
from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor


def local_config():
    return Config(
        executors=[
            ThreadPoolExecutor(max_threads=4),
        ],
        app_cache=False
    )


@python_app
def random_uuid(x):
    import uuid
    return str(uuid.uuid4())


@pytest.mark.local
def test_python_memoization(n=2):
    """Testing python memoization disable via DFK call
    """
    x = random_uuid(0)

    for i in range(0, n):
        foo = random_uuid(0)
        assert foo.result() != x.result(
        ), "Memoized results were used when memoization was disabled"
