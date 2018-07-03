import argparse

import pytest

import parsl
from parsl.dataflow.dflow import DataFlowKernel
from parsl.app.app import App
from parsl.tests.conftest import load_dfk
from parsl.tests.configs.local_threads_ipp import config

parsl.clear()
dfk = DataFlowKernel(config=config)
parsl.set_stream_logger()

import logging
logger = logging.getLogger(__name__)


@App("python", dfk, executors=['local_threads'])
def python_app_2():
    import os
    import threading
    import time
    time.sleep(1)
    return "Hello from PID[{}] TID[{}]".format(os.getpid(), threading.current_thread())


@App("python", dfk, executors=['local_ipp'])
def python_app_1():
    import os
    import threading
    import time
    time.sleep(1)
    return "Hello from PID[{}] TID[{}]".format(os.getpid(), threading.current_thread())


@App("bash", dfk)
def bash_app(stdout=None, stderr=None):
    return 'echo "Hello from $(uname -a)" ; sleep 2'


@pytest.mark.local
def test_python(N=2):
    """Testing basic python functionality."""

    r1 = {}
    r2 = {}
    for i in range(0, N):
        r1[i] = python_app_1()
        r2[i] = python_app_2()
    print("Waiting ....")

    for x in r1:
        print("python_app_1 : ", r1[x].result())
    for x in r2:
        print("python_app_2 : ", r2[x].result())

    return


@pytest.mark.local
def test_bash():
    """Testing basic bash functionality."""

    import os
    fname = os.path.basename(__file__)

    x = bash_app(stdout="{0}.out".format(fname))
    print("Waiting ....")
    print(x.result())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--num", default=10,
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    parser.add_argument("-c", "--config", default='local',
                        help="Path to configuration file to run")
    args = parser.parse_args()
    load_dfk(args.config)
    if args.debug:
        parsl.set_stream_logger()

    test_python()
    test_bash()
