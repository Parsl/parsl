from parsl import *
import parsl
import libsubmit

print(parsl.__version__)
print(libsubmit.__version__)

from .local import localIPP
dfk = DataFlowKernel(config=localIPP)


@App("python", dfk)
def python_app():
    import platform
    return "Hello from {0}".format(platform.uname())


def test_python(N=2):
    """Testing basic scaling|Python 0 -> 1 block """

    results = {}
    for i in range(0, N):
        results[i] = python_app()

    print("Waiting ....")
    for i in range(0, N):
        print(results[0].result())


if __name__ == '__main__':

    parsl.set_stream_logger()
    test_python()
