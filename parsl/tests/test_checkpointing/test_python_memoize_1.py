''' Testing python apps
'''

import parsl
from parsl import *
from nose.tools import nottest
import os
import time
import shutil
import argparse

#parsl.set_stream_logger()
config = {
    "sites" : [
        { "site" : "Local_Threads",
          "auth" : { "channel" : None },
          "execution" : {
              "executor" : "threads",
              "provider" : None,
              "maxThreads" : 4,
          }
        }],
    "globals" : {"lazyErrors" : True,
                 "checkpoint" : True,
    }
}

dfk = DataFlowKernel(config=config)

@App('python', dfk)
def slow_double (x, sleep_dur=3):
    import time
    time.sleep(sleep_dur)
    return x*2

def test_initial_checkpoint_write (n=4):
    """ Launch a few apps and check if the results are reused
    """

    d = {}
    print("Launching : ", n)
    for i in range(0,n):
        d[i] = slow_double(i)

    print("Waiting for results from round1")
    [d[i].result() for i in d]

    x = {}
    start = time.time()
    for i in range(0,n):
        x[i] = slow_double(i)
    end = time.time()
    delta = end-start
    assert delta < 0.1  , "Memoized results were not used"

if __name__ == '__main__' :

    parser   = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10", help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true', help="Count of apps to launch")
    args   = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_initial_checkpoint_write (n=4)
