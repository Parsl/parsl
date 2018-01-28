''' Testing bash apps
'''
import parsl
from parsl import *

from nose.tools import nottest
print("Parsl version: ", parsl.__version__)

import os
import time
import shutil
import argparse

#parsl.set_stream_logger()
workers = ThreadPoolExecutor(max_workers=10)

#workers = ProcessPoolExecutor(max_workers=4)
dfk = DataFlowKernel(workers)

@App('bash', dfk)
def sleep_foo(sleepdur, stdout=None):
    cmd_line = '''sleep {0}
    '''



if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10", help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true', help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    fus = {}

    start = time.time()
    for i in range(int(args.count)):

        fus[i] = sleep_foo(5)

    print ([(key, fus[key].result()) for key in fus])
    end = time.time()
    print ("Total time : ", end-start)
