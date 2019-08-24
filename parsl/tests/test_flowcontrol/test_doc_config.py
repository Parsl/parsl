import pytest

from parsl.app.app import App

from parsl.tests.configs.midway_ipp import config


local_config = config


@App("python")
def python_app():
    import os
    import time
    import platform
    time.sleep(20)
    return "Hello from {0}:{1}".format(os.getpid(), platform.uname())


@pytest.mark.local
def test_python(N=5):
    ''' Testing basic scaling|Python 0 -> 1 block on SSH.Midway  '''

    results = {}
    for i in range(0, N):
        results[i] = python_app()

    print("Waiting ....")
    for i in range(0, N):
        print(results[0].result())


if __name__ == '__main__':

    test_python()
