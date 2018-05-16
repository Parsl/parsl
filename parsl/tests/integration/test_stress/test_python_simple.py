"""Testing bash apps
"""
import parsl
from parsl import *

import time
import argparse

workers = ThreadPoolExecutor(max_workers=100)
dfk = DataFlowKernel(executors=[workers])


@App('python', dfk)
def increment(x):
    return x + 1


def test_stress(count=1000):
    """Threaded app RTT stress test"""

    start = time.time()
    x = []
    for i in range(int(count)):
        fu = increment(i)
        x.append(fu)
    end = time.time()
    print("Launched {0} tasks in {1} s".format(count, end - start))

    [fu.result() for fu in x]
    end = time.time()
    print("Completed {0} tasks in {1} s".format(count, end - start))


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="100",
                        help="width of the pipeline")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    test_stress(count=int(args.count))
