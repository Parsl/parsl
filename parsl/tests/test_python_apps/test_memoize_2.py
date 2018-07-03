import argparse

import pytest

import parsl
from parsl.app.app import App
from parsl.dataflow.dflow import DataFlowKernel
from parsl.tests.configs.local_threads_no_cache import config

parsl.clear()
dfk = DataFlowKernel(config)


@App('python', dfk)
def random_uuid(x):
    import uuid
    return str(uuid.uuid4())


@pytest.mark.local
def test_python_memoization(n=4):
    """Testing python memoization disable via config
    """
    x = random_uuid(0)

    for i in range(0, n):
        foo = random_uuid(0)
        assert foo.result() != x.result(
        ), "Memoized results were used when memoization was disabled"


dfk.cleanup()
parsl.clear()
dfk = DataFlowKernel(config)


@App('python', dfk)
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
