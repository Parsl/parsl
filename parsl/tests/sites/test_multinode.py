import argparse

import pytest

import parsl
from parsl.app.app import App
from parsl.tests.configs.cori_ipp_multinode import config
from parsl.tests.conftest import load_dfk

parsl.clear()
parsl.load(config)
parsl.set_stream_logger()


@App("python")
def python_app_slow(duration):
    import platform
    import time
    time.sleep(duration)
    return "Hello from {0}".format(platform.uname())


@pytest.mark.skip('not asserting anything')
def test_python_remote(count=10):
    """Run with no delay
    """
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", default='local',
                        help="Path to configuration file to run")
    args = parser.parse_args()

    load_dfk(args.config)
    test_python_remote()
    test_python_remote_slow()
