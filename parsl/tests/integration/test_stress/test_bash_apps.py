"""Testing bash apps
"""
import parsl
from parsl import DataFlowKernel, ThreadPoolExecutor, bash_app


import time
import argparse


@bash_app
def sleep_foo(sleepdur, stdout=None):
    return """sleep {0}"""


if __name__ == '__main__':
    print("Parsl version: ", parsl.__version__)
    workers = ThreadPoolExecutor(max_threads=10)
    dfk = DataFlowKernel(executors=[workers])

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    fus = {}

    start = time.time()
    for i in range(int(args.count)):

        fus[i] = sleep_foo(5)

    print([(key, fus[key].result()) for key in fus])
    end = time.time()
    print("Total time : ", end - start)
