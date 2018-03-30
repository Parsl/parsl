"""Testing dockerized apps
"""
import parsl
from parsl import *
parsl.set_stream_logger()
import time
import argparse

from localDockerIPP import localDockerIPP as config

dfk = DataFlowKernel(config=config)


@App('python', dfk)
def double(x):
    return x * 2


@App('python', dfk)
def platform_app(stdout=None):
    import time
    import platform
    time.sleep(0)
    return "Hello from {0}".format(platform.uname())


def test_simple(n=10):

    start = time.time()
    x = double(n)
    print("Result : ", x.result())
    assert x.result() == n * \
        2, "Expected double to return:{0} instead got:{1}".format(
            n * 2, x.result())
    print("Duration : {0}s".format(time.time() - start))
    print("[TEST STATUS] test_parallel_for [SUCCESS]")
    return True


def test_platform():

    items = []

    for i in range(0, 10):
        items.append(platform_app())

    for i in items:
        print(i.result())


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_simple(int(args.count))
    x = test_platform()
    # x = test_parallel_for(int(args.count))

    # x = test_stdout()
    # raise_error(0)
