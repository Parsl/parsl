import pytest

import parsl
from parsl.app.app import bash_app, python_app


def local_setup():
    from parsl.tests.configs.cori_ipp_multinode import config
    parsl.load(config)


def local_teardown():
    parsl.clear()


@python_app
def python_app_slow(duration):
    import platform
    import time
    time.sleep(duration)
    return "Hello from {0}".format(platform.uname())


@pytest.mark.skip('not asserting anything')
def test_python_remote(count=10):
    """Run with no delay"""

    fus = []
    for i in range(0, count):
        fu = python_app_slow(0)
        fus.extend([fu])

    for fu in fus:
        print(fu.result())


@pytest.mark.skip('not asserting anything')
def test_python_remote_slow(count=20):
    fus = []
    for i in range(0, count):
        fu = python_app_slow(count)
        fus.extend([fu])

    for fu in fus:
        print(fu.result())


@bash_app
def bash_mpi_app(stdout=None, stderr=None):
    return """ls -thor
mpi_hello
    """
