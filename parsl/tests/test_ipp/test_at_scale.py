"""Testing bash apps
"""
import parsl
from parsl import *
import time
import argparse

# parsl.set_stream_logger()
workers = IPyParallelExecutor()
print("Using ipyparallel workers")
# workers = ThreadPoolExecutor(max_workers=4)
print("Using threads pool 4")
dfk = DataFlowKernel(workers)


@App('python', dfk)
def double(x):
    return x * 2


def plain_double(x):
    return x * 2


def test_plain(n=10):
    start = time.time()
    x = []
    for i in range(0, n):
        x.extend([plain_double(i)])

    print(sum(x))

    ttc = time.time() - start
    print("Total time : ", ttc)

    return ttc


def test_parallel(n=10):
    start = time.time()
    x = []
    for i in range(0, n):
        x.extend([double(i)])

    print(sum([i.result() for i in x]))

    ttc = time.time() - start
    print("Total time : ", ttc)

    return ttc


def test_parallel2(n=10):
    start = time.time()
    x = []
    for i in range(0, n):
        x.extend([double(i)])

    ttc = time.time() - start
    print("Total time : ", ttc)

    return ttc


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_plain(int(args.count))
    # x = test_parallel(int(args.count))
    x = test_parallel2(int(args.count))

    # x = test_stdout()
    # raise_error(0)
