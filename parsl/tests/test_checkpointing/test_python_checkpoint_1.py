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
              "maxThreads" : 2,
          }
        }],
    "globals" : {"lazyErrors" : True,
                 "checkpoint" : True,
    }
}

dfk = DataFlowKernel(config=config)

@App('python', dfk)
def slow_double (x, sleep_dur=1):
    import time
    time.sleep(sleep_dur)
    return x*2

def test_initial_checkpoint_write (n=4):
    """ 1. Launch a few apps and write the checkpoint once a few have completed
    """

    d = {}
    start = time.time()
    print("Launching : ", n)
    for i in range(0,n):
        d[i] = slow_double(i)
    print("Done launching")

    for i in range(0,n):
        d[i].result()
    print("Done sleeping")
    cpt_dir = dfk.checkpoint()
    assert not os.path.exists(cpt_dir+'/dfk') , "DFK checkpoint missing"
    assert not os.path.exists(cpt_dir+'/tasks') , "Tasks checkpoint missing"
    return

if __name__ == '__main__' :

    parser   = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10", help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true', help="Count of apps to launch")
    args   = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_initial_checkpoint_write (n=4)
