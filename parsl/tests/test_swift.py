#!/usr/bin/env python3.5
import sys

import parsl
from parsl import *

parsl.set_stream_logger()

from parsl.executors.swift_t import *


def foo(x, y):
    return x*y

def slow_foo(x, y):
    import time
    time.sleep(x)
    return x*y

def bad_foo(x, y):
    time.sleep(x)
    return x*y


def test_simple():
    print("Start")
    tex = TurbineExecutor()
    x = tex.submit(foo, 5, 10)
    print("Got : ", x)
    print("X result : ", x.result())
    assert x.result() == 50, "X != 50"
    print("done")

def test_except():
    print("Start")
    tex = TurbineExecutor()
    x = tex.submit(bad_foo, 5, 10)
    print("Got : ", x)

    print("X exception : ", x.exception())
    print("X result : ", x.result())

    print("done")

if __name__ == "__main__":


    #test_simple()
    test_except()
    exit(0)
    futs = {}
    for i in range(0, 1):
        futs[i] = tex.submit(slow_foo, 3, 10)


    x.result(timeout=10)
    for x in range(0, 10):
        print(futs)
        time.sleep(4)

    print("Done")

