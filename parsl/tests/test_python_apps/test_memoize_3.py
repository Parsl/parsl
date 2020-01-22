import argparse

import pytest

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.local_threads_no_cache import config

local_config = config


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


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_python_memoization(n=4)
