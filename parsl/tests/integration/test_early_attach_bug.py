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
from parsl import DataFlowKernel, python_app
import time

from parsl.tests.configs.local_ipp import config


@python_app
def sleep_double(x):
    import time
    time.sleep(1)
    return x * 2


def test_z_cleanup():
    dfk = DataFlowKernel(config=config)
    dfk.cleanup()
    pass


if __name__ == "__main__":

    print("Starting launch")

    jobs = {}
    for i in range(0, 20):
        jobs[i] = sleep_double(i)

    start = time.time()
    for i in range(0, 10):
        print(jobs[i].result())
    print("Time to finish : ", time.time() - start)
