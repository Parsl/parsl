import os
import sys
import argparse

import parsl
from parsl import *

from parsl.app.app_factory import AppFactoryFactory, AppFactory

parsl.set_stream_logger()
workers = ThreadPoolExecutor(max_workers=4)

@App('bash', workers)
def test(stderr='std.err', stdout='std.out'):
    cmd_line = "echo 'Hello world'"


def test2(stderr='std.err', stdout='std.out'):
    cmd_line = "echo 'Hello world'"

def test3(x):
    return x*2

if __name__ == '__main__' :

    appff = AppFactoryFactory('main')
    app_f = appff.make('bash', workers, test2, walltime=60)
    assert type(app_f) == AppFactory , "AppFactoryFactory made the wrong type"

    app_f_2 = appff.make('python', workers, test3, walltime=60)
    assert type(app_f_2) == AppFactory , "AppFactoryFactory made the wrong type"

    #for i in range(0,100):
    #    x, _ = app_f_2(10)

    #app_f_2(20)
    #print("Name: ", app_f.__repr__())

    #print("Test type : ", type(app_f))

    #futs = {}
    #for i in range(0, 4):
    #    futs[i], _ = test()


