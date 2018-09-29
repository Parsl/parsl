import argparse
import time

import parsl

# from parsl.configs.local_threads import config
# from parsl.configs.local_ipp import config
from mpix_local import config

# parsl.set_stream_logger()
from parsl.app.app import App
parsl.load(config)


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
def platform(sleep=10, stdout=None):
    import platform
    import time
    time.sleep(sleep)
    return platform.uname()


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


def test_platform(n=2):
    # sync
    x = platform(sleep=0)
    print(x.result())

    d = []
    for i in range(0, n):
        x = platform(sleep=1)
        d.append(x)

    print(set([i.result()for i in d]))

    return True


def test_parallel_for(n=2):
    d = {}

    start = time.time()
    print("Priming ...")
    double(10).result()
    delta = time.time() - start
    print("Priming done in {} seconds".format(delta))

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
        except Exception as e:
            print("Failed to load the requested config : ", args.sitespec)
            exit(0)

    # if args.debug:
    #    parsl.set_stream_logger()

    # x = test_simple(int(args.count))
    # x = test_imports()
    x = test_parallel_for(int(args.count))
    # x = test_parallel_for(int(args.count))
    # x = test_stdout()
    # x = test_platform()
