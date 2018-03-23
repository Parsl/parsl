"""Testing python apps
"""

import parsl
from parsl import *
import argparse

# parsl.set_stream_logger()
config = {
    "sites": [
        {"site": "Local_Threads",
         "auth": {"channel": None},
         "execution": {
             "executor": "threads",
             "provider": None,
             "maxThreads": 4,
         }
         }],
    "globals": {"appCache": False}
}
dfk = DataFlowKernel(config=config)


@App('python', dfk)
def random_uuid(x):
    import uuid
    return str(uuid.uuid4())


def test_python_memoization(n=4):
    """Testing python memoization disable via config
    """
    x = random_uuid(0)

    for i in range(0, n):
        foo = random_uuid(0)
        assert foo.result() != x.result(
        ), "Memoized results were used when memoization was disabled"


dfk.cleanup()
workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers], appCache=False)


@App('python', dfk)
def random_uuid(x):
    import uuid
    return str(uuid.uuid4())


def test_python_memoization(n=4):
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
