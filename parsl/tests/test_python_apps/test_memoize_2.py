import argparse

import pytest

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.local_threads_no_cache import fresh_config as local_config


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
