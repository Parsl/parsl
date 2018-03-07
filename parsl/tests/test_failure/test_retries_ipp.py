#!/usr/bin/env python3
import argparse

import parsl
from parsl import *

# from parsl.configs.local import localThreads as config
from parsl.configs.local import localIPP as config
config["globals"]["lazy_fail"] = True

parsl.set_stream_logger()

dfk = DataFlowKernel(config=config, failRetries=2)


@App('python', dfk)
def sleep_then_fail(inputs=[], sleep_dur=0.1):
    import time
    import math
    time.sleep(sleep_dur)
    math.ceil("Trigger TypeError")
    return 0


@App('python', dfk)
def sleep(sleep_dur=0.1):
    import time
    time.sleep(sleep_dur)
    return 0


def test_fail_nowait(numtasks=10):
    ''' Test retries on tasks with no dependencies. Threads
    '''
    fus = []
    for i in range(0, numtasks):
        fu = sleep_then_fail(sleep_dur=0.1)
        fus.extend([fu])

    try:
        [x.result() for x in fus]
    except Exception as e:
        # assert isinstance(e, TypeError), "Expected a TypeError, got {}".format(e)
        pass

    print("Done")


def test_fail_delayed(numtasks=10):
    ''' Test retries on tasks with dependencies. Threads.

    This is testing retry behavior when AppFutures are created
    with no parent.
    '''

    x = sleep()
    fus = []
    for i in range(0, numtasks):
        fu = sleep_then_fail(inputs=[x], sleep_dur=0.5)
        fus.extend([fu])

    try:
        [x.result() for x in fus]
    except Exception as e:
        # assert isinstance(e, TypeError), "Expected a TypeError, got {}".format(e)
        pass

    print("Done")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    test_fail_nowait(numtasks=int(args.count))
    test_fail_delayed(numtasks=int(args.count))
