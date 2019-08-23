import argparse
import time
import pytest

import parsl
from llex_local import config
# parsl.set_stream_logger()

from parsl.app.app import python_app  # , bash_app
# parsl.load(config)


@python_app
def double(x):
    return x * 2


@python_app
def import_echo(x, string, stdout=None):
    import time
    time.sleep(0)
    print(string)
    return x * 5


@python_app
def math_error(x):
    return x / 0

@pytest.mark.skip('manual run only')
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


@pytest.mark.skip('manual run only')
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


@pytest.mark.skip('manual run only')
def test_failure(n=2):
    error = None
    try:
        math_error(10).result()
    except ZeroDivisionError as e:
        error = e
        pass
    assert isinstance(error, ZeroDivisionError), "Expected ZeroDivisionError, got {}".format(error)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sitespec", default=None)
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.sitespec:
        config = None
        try:
            exec("import parsl; from {} import config".format(args.sitespec))
            parsl.load(config)
        except Exception:
            print("Failed to load the requested config : ", args.sitespec)
            exit(0)

    if args.debug:
        parsl.set_stream_logger()

    x = test_simple(int(args.count))
    x = test_imports()
    x = test_failure()
