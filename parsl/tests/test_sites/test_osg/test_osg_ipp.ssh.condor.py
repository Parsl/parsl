from parsl import *
import parsl
import libsubmit
#libsubmit.set_stream_logger()
#parsl.set_stream_logger()
import os

os.environ["OSG_USERNAME"] = "yadunand"
from osg import multiNode as config
dfk = DataFlowKernel(config=config)

@App("python", dfk)
def test(duration=0):
    import platform
    import time
    time.sleep(duration)
    return "Hello from {0}".format(platform.uname())


if __name__ == "__main__":

    results = {}
    print("Launching tasks...")
    for i in range(0,10):
        results[i] = test(20)

    print("Waiting ....")

    for key in results:
        print(results[key].result())

