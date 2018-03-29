import parsl
from parsl import *
import os
import time
import argparse

# parsl.set_stream_logger()
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
                }
}

dfk = DataFlowKernel(config=config)


@App('python', dfk, cache=True)
def slow_double(x, sleep_dur=1):
    import time
    time.sleep(sleep_dur)
    return x * 2


def test_initial_checkpoint_write(n=4):
    """1. Launch a few apps and write the checkpoint once a few have completed
    """

    d = {}
    time.time()
    print("Launching : ", n)
    for i in range(0, n):
        d[i] = slow_double(i)
    print("Done launching")

    for i in range(0, n):
        d[i].result()
    print("Done sleeping")
    cpt_dir = dfk.checkpoint()

    cptpath = cpt_dir + '/dfk.pkl'
    print("Path exists : ", os.path.exists(cptpath))
    assert os.path.exists(
        cptpath), "DFK checkpoint missing: {0}".format(cptpath)

    cptpath = cpt_dir + '/tasks.pkl'
    print("Path exists : ", os.path.exists(cptpath))
    assert os.path.exists(
        cptpath), "Tasks checkpoint missing: {0}".format(cptpath)

    return


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_initial_checkpoint_write(n=4)
