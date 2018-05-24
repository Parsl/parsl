import pytest

from parsl.app.app import App
from parsl.dataflow.dflow import DataFlowKernel
from parsl.tests.configs.local_ipp import config

config.executors[0].init_blocks = 0
dfk = DataFlowKernel(config=config)


@App("python", dfk)
def python_app():
    import platform
    return "Hello from {0}".format(platform.uname())


@pytest.mark.local
def test_python(N=2):
    """Testing basic scaling|Python 0 -> 1 block """

    results = {}
    for i in range(0, N):
        results[i] = python_app()

    print("Waiting ....")
    for i in range(0, N):
        print(results[0].result())


if __name__ == '__main__':

    test_python()
