from parsl import *
import parsl
import libsubmit
import os
import time
# parsl.set_stream_logger()
print(parsl.__version__)
print(libsubmit.__version__)

os.environ['MIDWAY_USERNAME'] = 'yadunand'
from midway import multiNode as config
parsl.set_stream_logger()
dfk = DataFlowKernel(config=config)


@App("python", dfk)
def python_app(sleep_duration=0.5):
    import platform
    import time
    time.sleep(sleep_duration)
    return "Hello from {0}".format(platform.uname())


@App("bash", dfk)
def bash_app(stdout=None, stderr=None):
    return 'echo "Hello from $(uname -a)" ; sleep 2'


def test_python(N=2000, sleep_duration=0.5):
    results = {}

    start = time.time()
    for i in range(0, N):
        results[i] = python_app(sleep_duration=sleep_duration)
    end = time.time()
    print("Launched {} tasks in : {}. Task rate: {} Tasks/sec".format(N,
                                                                      end - start, float(N) / (end - start)))
    print("Waiting ....")

    start = time.time()
    x = [results[i].result() for i in results]
    end = time.time()
    print("Completed all tasks in :", end - start)
    print("Ideal time : {}*{} = {} / parallelism".format(N,
                                                         sleep_duration, N * sleep_duration))
    print("Unique items : ")
    for item in set(x):
        print(item)


def test_bash():
    import os
    fname = os.path.basename(__file__)

    x = bash_app(stdout="{0}.out".format(fname))
    print("Waiting ....")
    print(x.result())


if __name__ == "__main__":

    test_python()
    # test_bash()
