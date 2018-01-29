''' Testing python apps
'''
import parsl
from parsl import *
from nose.tools import nottest
import os
import time
import shutil
import argparse

time.sleep(1)
parsl.set_stream_logger()
last_checkpoint = os.path.abspath('runinfo/{0}'.format(sorted(os.listdir('runinfo/'))[-1]))
while True:
    if not os.path.exists(last_checkpoint):
        print("Waiting for path")
        sleep(0.1)
    else:
        print("Path exists to checkpoint")
        break


config = {
    "sites": [
        {"site": "Local_Threads",
         "auth": {"channel": None},
         "execution": {
             "executor": "threads",
              "provider": None,
              "maxThreads": 2,
          }
         }],
    "globals": {"lazyErrors": True,
                "memoize": True,
                "checkpoint": True,
                }
}
dfk = DataFlowKernel(config=config, checkpointFiles=[last_checkpoint])


@App('python', dfk)
def slow_double(x, sleep_dur=1):
    import time
    time.sleep(sleep_dur)
    return x * 2


def test_initial_checkpoint_write(n=4):
    """ 2. Load the memoization table from previous checkpoint
    """

    d = {}

    start = time.time()
    print("Launching : ", n)
    for i in range(0, n):
        d[i] = slow_double(i)
    print("Done launching")

    for i in range(0, n):
        d[i].result()
    print("Done sleeping")

    delta = time.time() - start
    assert delta < 1, "Took longer than a second, restore from checkpoint failed"


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10", help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true', help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_initial_checkpoint_write(n=4)
