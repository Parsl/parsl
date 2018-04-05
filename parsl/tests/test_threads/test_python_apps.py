"""Testing python apps
"""
import parsl
from parsl import *
from nose.tools import nottest
import os
import time
import argparse

from nose.tools import assert_raises

# parsl.set_stream_logger()
workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers])


@App('python', dfk)
def double(x):
    return x * 5


@App('python', dfk)
def echo(x, string, stdout=None):
    print(string)
    return x * 5


@App('python', dfk)
def custom_exception():
    from globus_sdk import GlobusError
    raise GlobusError('foobar')


def test_parallel_for(n=2):

    d = {}
    start = time.time()
    for i in range(0, n):
        d[i] = double(i)
        # time.sleep(0.01)

    print("Exception : ", d[0].exception())
    assert len(
        d.keys()) == n, "Only {0}/{1} keys in dict".format(len(d.keys()), n)

    [d[i].result() for i in d]
    print("Duration : {0}s".format(time.time() - start))
    print("[TEST STATUS] test_parallel_for [SUCCESS]")
    return d


@nottest
def test_stdout():

    string = "Hello World!"
    fu = echo(10, string, stdout='std.out')
    fu.result()

    assert os.path.exists('std.out'), "STDOUT was not captured to 'std.out'"

    with open('std.out', 'r') as f:
        assert f.read() == string, "String did not match output file"
    print("[TEST STATUS] test_stdout [SUCCESS]")


def test_custom_exception():
    from globus_sdk import GlobusError

    def wrapper():
        x = custom_exception()
        return x.result()
    assert_raises(GlobusError, wrapper)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_parallel_for(int(args.count))
    x = test_stdout()
    # raise_error(0)
