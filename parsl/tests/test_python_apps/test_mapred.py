import argparse

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.local_threads import config


@python_app
def fan_out(x, dur):
    import time
    time.sleep(dur)
    return x * 2


@python_app
def accumulate(inputs=[]):
    return sum(inputs)


@python_app
def accumulate_t(*args):
    return sum(args)


def test_mapred_type1(width=2):
    """MapReduce test with the reduce stage taking futures in inputs=[]
    """

    futs = []
    for i in range(1, width + 1):
        fu = fan_out(i, 1)
        futs.extend([fu])

    print("Fan out : ", futs)

    red = accumulate(inputs=futs)
    # print([(i, i.done()) for i in futs])
    r = sum([x * 2 for x in range(1, width + 1)])
    assert r == red.result(), "[TEST] MapRed type1 expected %s, got %s" % (
        r, red.result())


def test_mapred_type2(width=2):
    """MapReduce test with the reduce stage taking futures on the args
    """

    futs = []
    for i in range(1, width + 1):
        fu = fan_out(i, 0.1)
        futs.extend([fu])

    print("Fan out : ", futs)

    red = accumulate_t(*futs)

    # print([(i, i.done()) for i in futs])
    r = sum([x * 2 for x in range(1, width + 1)])
    assert r == red.result(), "[TEST] MapRed type2 expected %s, got %s" % (
        r, red.result())
