import argparse

from parsl import *
import parsl
import libsubmit

print(parsl.__version__)
print(libsubmit.__version__)

parsl.set_stream_logger()

from local_multi import threads_ipp as config
dfk = DataFlowKernel(config=config)


@App("python", dfk, sites=['Local_threads'])
def python_app_2():
    import os
    import threading
    import time
    time.sleep(1)
    return "Hello from PID[{}] TID[{}]".format(os.getpid(), threading.current_thread())


@App("python", dfk, sites=['Local_IPP'])
def python_app_1():
    import os
    import threading
    import time
    time.sleep(1)
    return "Hello from PID[{}] TID[{}]".format(os.getpid(), threading.current_thread())


@App("bash", dfk)
def bash_app(stdout=None, stderr=None):
    return 'echo "Hello from $(uname -a)" ; sleep 2'


def test_python(N=5):
    ''' Testing basic python functionality '''

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


def test_bash():
    ''' Testing basic bash functionality '''

    import os
    fname = os.path.basename(__file__)

    x = bash_app(stdout="{0}.out".format(fname))
    print("Waiting ....")
    print(x.result())


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    test_python()
    # test_bash()
