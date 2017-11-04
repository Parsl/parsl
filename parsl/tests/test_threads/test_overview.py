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
workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers])

@App('python', dfk)
def app_double(x):
    return x*2

@App('python', dfk)
def app_sum(inputs=[]):
    return sum(inputs)


def test_1 (N = 100) :
    ''' Testing code snippet from the documentation
    '''

    # Create a list of integers
    items = range(0,N)

    # Map Phase : Apply an *app* function to each item in list
    mapped_results = []
    for i in items:
        x = app_double(i)
        mapped_results.append(x)

    total = app_sum(inputs=mapped_results)

    print(total.result())

    assert total.result() != sum(items), "Sum is wrong {0} != {1}".format(total.result(), sum(items))

if __name__ == "__main__" :
    test_1(10)
    test_1(100)
