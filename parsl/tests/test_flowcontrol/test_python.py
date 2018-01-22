from parsl import *
import parsl
import libsubmit

print(parsl.__version__)
print(libsubmit.__version__)

#parsl.set_stream_logger()

from .local import localIPP
dfk = DataFlowKernel(config=localIPP)

@App("python", dfk)
def python_app():
    import platform
    return "Hello from {0}".format(platform.uname())


def test_python():
    ''' Testing basic scaling|Python 0 -> 1 block '''

    import os
    results = {}
    for i in range(0,2):
        results[i] = python_app()

    print("Waiting ....")
    print(results[0].result())

