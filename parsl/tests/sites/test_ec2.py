import parsl

if __name__ == "__main__":
    parsl.set_stream_logger()
    # initialise logging before importing config, to get logging
    # from config declaration # as it looks like the AWS initializer
    # is doing more than just creating data structures

from parsl.app.app import App
from parsl.tests.configs.ec2_single_node import config

import logging
logger = logging.getLogger(__name__)


@App("python", executors=['ec2_single_node'])
def python_app_2():
    import os
    import threading
    import time
    time.sleep(1)
    return "Hello from PID[{}] TID[{}]".format(os.getpid(), threading.current_thread())


@App("python", executors=['ec2_single_node'])
def python_app_1():
    import os
    import threading
    import time
    time.sleep(1)
    return "Hello from PID[{}] TID[{}]".format(os.getpid(), threading.current_thread())


@App("bash")
def bash_app(stdout=None, stderr=None):
    return 'echo "Hello from $(uname -a)" ; sleep 2'


# @pytest.mark.local
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


local_config = config


# @pytest.mark.local
def test_bash():
    """Testing basic bash functionality."""

    import os
    fname = os.path.basename(__file__)

    x = bash_app(stdout="{0}.out".format(fname))
    print("Waiting ....")
    print(x.result())


if __name__ == "__main__":
    parsl.load(config)
    test_python()
    test_bash()
