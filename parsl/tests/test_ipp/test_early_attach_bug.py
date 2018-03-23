"""Testing early attach behavior with LoadBalanced view

Test setup:

Start the ipcontroller and 1 ipengine, and run this script.
The time to finish the 10 apps should be ~10s.

In the second run, start the parsl script, and as soon as the run starts,
start additional ipengines. The time to finish the 10 apps should still be ~10s.

This shows that the LoadBalanced View simply routes tasks to the available engines
at the time the apps were submitted to it. It is not capable of rebalancing the apps
among the engine once it has been sent to the the engine's queue.


"""
import parsl
from parsl import *

print("Parsl version: ", parsl.__version__)

import time

# parsl.set_stream_logger()

workers = IPyParallelExecutor()
dfk = DataFlowKernel(workers)


@App('python', dfk)
def sleep_double(x):
    import time
    time.sleep(1)
    return x * 2


if __name__ == "__main__":

    print("Starting launch")

    jobs = {}
    for i in range(0, 20):
        jobs[i] = sleep_double(i)

    start = time.time()
    for i in range(0, 10):
        print(jobs[i].result())
    print("Time to finish : ", time.time() - start)
