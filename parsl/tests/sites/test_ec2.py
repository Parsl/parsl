import parsl
import pytest

from parsl.app.app import python_app
from parsl.tests.configs.ec2_single_node import config

import logging
logger = logging.getLogger(__name__)

local_config = config


@python_app(executors=['ec2_single_node'])
def python_app_2():
    import os
    import threading
    import time
    time.sleep(1)
    return "Hello from PID[{}] TID[{}]".format(os.getpid(), threading.current_thread())


@python_app(executors=['ec2_single_node'])
def python_app_1():
    import os
    import threading
    import time
    time.sleep(1)
    return "Hello from PID[{}] TID[{}]".format(os.getpid(), threading.current_thread())


@parsl.bash_app
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
    parsl.set_stream_logger()

    # this config is imported inside the main block for the same
    # reason that it has local standup/teardown code at the top
    # of this file. For that same reason, set_stream_logger
    # is called before this import.

    from parsl.tests.configs.ec2_single_node import config
    parsl.load(config)
    test_python()
    test_bash()
