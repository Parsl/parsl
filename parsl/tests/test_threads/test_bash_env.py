''' Testing bash apps
'''
import parsl
from parsl import *

from nose.tools import nottest
print("Parsl version: ", parsl.__version__)

import os
import time
import argparse

workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(workers)

@App('bash', dfk)
def env_bashing(dep, inputs=[], outputs=[], stderr='std.err', stdout='std.out'):
    cmd_line = '''# dump env to stdout
    env
    export PYTHONPATH=/usr/lib/python2.7
    echo "PYTHONPATH =  $PYTHONPATH"
    PATHTHO
    '''

def test_bashing():

    t1 = env_bashing (None, stdout='std1.out')
    t2 = env_bashing (t1,   stdout='std2.out')
    t2.result()
    print(t2)
    
if __name__ == '__main__' :

    parser   = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10", help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true', help="Count of apps to launch")
    args   = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    y = test_bashing()
    #raise_error(0)


