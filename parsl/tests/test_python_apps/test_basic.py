import argparse
import os
import time

import pytest

import parsl
from parsl.app.app import App
from parsl.tests.configs.local_ipp import config

parsl.set_stream_logger()


@App('python')
def double(x):
    return x * 2


@App('python')
def echo(x, string, stdout=None):
    print(string)
    return x * 5


@App('python')
def import_echo(x, string, stdout=None):
    import time
    time.sleep(0)
    print(string)
    return x * 5


@App('python')
def custom_exception():
    from globus_sdk import GlobusError
    raise GlobusError('foobar')


def test_simple(n=2):
    start = time.time()
    x = double(n)
    print("Result : ", x.result())
    assert x.result() == n * \
        2, "Expected double to return:{0} instead got:{1}".format(
            n * 2, x.result())
    print("Duration : {0}s".format(time.time() - start))
    print("[TEST STATUS] test_parallel_for [SUCCESS]")
    return True


def test_imports(n=2):
    start = time.time()
    x = import_echo(n, "hello world")
    print("Result : ", x.result())
    assert x.result() == n * \
        5, "Expected double to return:{0} instead got:{1}".format(
            n * 2, x.result())
    print("Duration : {0}s".format(time.time() - start))
    print("[TEST STATUS] test_parallel_for [SUCCESS]")
    return True


def test_parallel_for(n=2):
    d = {}
    start = time.time()
    for i in range(0, n):
        d[i] = double(i)
        # time.sleep(0.01)

    assert len(
        d.keys()) == n, "Only {0}/{1} keys in dict".format(len(d.keys()), n)

    [d[i].result() for i in d]
    print("Duration : {0}s".format(time.time() - start))
    print("[TEST STATUS] test_parallel_for [SUCCESS]")
    return d


@pytest.mark.skip('broken')
def test_stdout():
    """This one does not work as we don't catch stdout and stderr for python apps.
    """
    string = "Hello World!"
    fu = echo(10, string, stdout='std.out')
    fu.result()

    assert os.path.exists('std.out'), "STDOUT was not captured to 'std.out'"

    with open('std.out', 'r') as f:
        assert f.read() == string, "String did not match output file"
    print("[TEST STATUS] test_stdout [SUCCESS]")


def test_custom_exception():
    from globus_sdk import GlobusError

    with pytest.raises(GlobusError):
        x = custom_exception()
        x.result()


def demonstrate_custom_exception():
    x = custom_exception()
    print(x.result())


if __name__ == '__main__':
    parsl.clear()
    parsl.load(config)

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    # demonstrate_custom_exception()
    x = test_simple(int(args.count))
    # x = test_imports()
    # x = test_parallel_for()
    # x = test_parallel_for(int(args.count))
    # x = test_stdout()
