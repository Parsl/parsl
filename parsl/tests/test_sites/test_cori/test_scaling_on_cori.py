import parsl
from parsl import *
import os

os.environ['CORI_USERNAME'] = 'yadunand'
from cori import singleNode as config
parsl.set_stream_logger()
dfk = DataFlowKernel(config=config)


@App("python", dfk)
def python_app_slow(duration):
    import platform
    import time
    time.sleep(duration)
    return "Hello from {0}".format(platform.uname())


def test_python_remote(count=10):
    """ Run with no delay
    """
    fus = []
    for i in range(0, count):
        fu = python_app_slow(0)
        fus.extend([fu])

    for fu in fus:
        print(fu.result())


def test_python_remote_slow(count=10):

    fus = []
    for i in range(0, count):
        fu = python_app_slow(count / 10)
        fus.extend([fu])

    for fu in fus:
        print(fu.result())


if __name__ == "__main__":

    test_python_remote()
    test_python_remote_slow()
