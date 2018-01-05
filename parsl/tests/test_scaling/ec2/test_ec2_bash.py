import parsl
from parsl import *
#from nose.tools import nottest
import os
import time
import shutil
import argparse

parsl.set_stream_logger()

from ec2 import singleNode as config
dfk = DataFlowKernel(config=config)

@App("bash", dfk)
def bash_app(seq_run_id):
    return '''uname -a;
echo {0}
aws s3 cp logfile s3://mybucket/{0}
    '''


def test_python_remote(count=10):
    ''' Run with no delay
    '''
    fus = []
    for i in range(0, count):
        fu = python_app_slow(0)
        fus.extend([fu])

    for fu in fus:
        print(fu.result())

def test_python_remote_slow(count=20):

    fus = []
    for i in range(0, count):
        fu = python_app_slow(count)
        fus.extend([fu])

    for fu in fus:
        print(fu.result())


if __name__ == "__main__" :


    test_python_remote()
    dfk.cleanup()
    #test_python_remote_slow()
