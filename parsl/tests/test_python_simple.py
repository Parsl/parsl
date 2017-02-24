''' Testing bash apps
'''
import parsl
from parsl import *

import os
import time
import shutil
import argparse
import random

parsl.set_stream_logger()

workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(workers)

@App('python', dfk)
def increment(x):
    return x+1

@App('python', dfk)
def slow_increment(x, dur):
    import time
    time.sleep(dur)
    return x+1

def test_increment(depth=5):
    futs = {0:0}
    for i in range(1,depth):
        futs[i] = increment( futs[i-1] )

    x = sum([ futs[i].result() for i in futs if type(futs[i]) != int ])
    assert x == sum(range(1,depth)), "[TEST] increment [FAILED]"
    print("[TEST] increment [SUCCESS]")


def test_slow_increment(depth=5):
    futs = {0:0}
    for i in range(1,depth):
        futs[i]  = slow_increment( futs[i-1], random.randint(0,100)/1000)

    x = sum([futs[i].result()    for i in futs if type(futs[i]) != int ])

    assert x == sum(range(1,depth)), "[TEST] slow_increment [FAILED]"
    print("[TEST] slow_increment [SUCCESS]")

if __name__ == '__main__' :

    parser   = argparse.ArgumentParser()
    parser.add_argument("-w", "--width", default="5", help="width of the pipeline")
    parser.add_argument("-d", "--debug", action='store_true', help="Count of apps to launch")
    args   = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    test_increment(depth=int(args.width))
    test_slow_increment(depth=int(args.width))

    
