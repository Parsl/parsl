#!/usr/bin/env python3

import argparse
import time

import parsl

# from parsl.tests.configs.htex_local import config
# from htex_local import config
# from parsl.configs.local_threads import config
# from parsl.configs.local_ipp import config

# parsl.set_stream_logger()
# config.executors[0].provider.tasks_per_node = 4
# parsl.load(config)
from parsl.app.app import python_app  # , bash_app


@python_app
def double(x):
    return x * 2


@python_app
def echo(x, string, stdout=None):
    print(string)
    return x * 5


@python_app
def import_echo(x, string, stdout=None):
    # from time import sleep
    # sleep(0)
    print(string)
    return x * 5


@python_app
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


def test_platform(n=2, sleep=1):

    dfk = parsl.dfk()
    # sync
    x = platform(sleep=0)
    print(x.result())

    name = list(dfk.executors.keys())[0]
    print("Trying to get executor : ", name)

    print("Executor : ", dfk.executors[name])
    print("Connected   : ", dfk.executors[name].connected_workers)
    print("Outstanding : ", dfk.executors[name].outstanding)
    d = []
    for i in range(0, n):
        x = platform(sleep=sleep)
        d.append(x)

    print("Connected   : ", dfk.executors[name].connected_workers)
    print("Outstanding : ", dfk.executors[name].outstanding)

    print(set([i.result()for i in d]))

    print("Connected   : ", dfk.executors[name].connected_workers)
    print("Outstanding : ", dfk.executors[name].outstanding)

    return True


def test_parallel_for(n=2, sleep=1):
    d = {}

    start = time.time()
    print("Priming ...")
    double(10).result()
    delta = time.time() - start
    print("Priming done in {} seconds".format(delta))

    start = time.time()
    for i in range(0, n):
        d[i] = platform(sleep=sleep)
        # d[i] = double(i)
        # time.sleep(0.01)
    [d[i].result() for i in d]
    delta = time.time() - start
    print("Time to complete {} tasks: {:8.3f} s".format(n, delta))
    print("Throughput : {:8.3f} Tasks/s".format(n / delta))
    return d


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sleep", default="0")
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    parser.add_argument("-f", "--fileconfig", required=True)

    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    config = None
    exec("from {} import config".format(args.fileconfig))
    parsl.load(config)
    # x = test_simple(int(args.count))
    # x = test_imports()
    x = test_parallel_for(int(args.count), float(args.sleep))
    # x = test_platform(int(args.count), int(args.sleep))
    # x = test_parallel_for(int(args.count))
    # x = test_stdout()
    # x = test_platform()
