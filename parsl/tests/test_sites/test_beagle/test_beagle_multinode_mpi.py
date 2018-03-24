import parsl
from parsl import *
import os

os.environ['CORI_USERNAME'] = 'yadunand'
from cori import multiNodeMPI as config
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


def test_python_remote_slow(count=20):

    fus = []
    for i in range(0, count):
        fu = python_app_slow(count)
        fus.extend([fu])

    for fu in fus:
        print(fu.result())


@App("bash", dfk)
def bash_mpi_app(stdout=None, stderr=None):
    return """ls -thor
mpi_hello
    """


if __name__ == "__main__":

    items = []
    for i in range(0, 4):
        x = bash_mpi_app(stdout="parsl.{0}.out".format(i),
                         stderr="parsl.{0}.err".format(i))
        items.extend([x])

    for i in items:
        print(i.result())
