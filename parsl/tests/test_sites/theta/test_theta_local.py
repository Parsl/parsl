from parsl import *
import parsl
import libsubmit
import os
parsl.set_stream_logger()
os.environ["THETA_USERNAME"] = "yadunand"
from theta import multiNode as config
dfk = DataFlowKernel(config=config)

@App("python", dfk)
def test(dur=1):
    import platform
    import time
    time.sleep(dur)
    return "Hello from {0}".format(platform.uname())


def test_all (dur = 1):
    results = {}
    for i in range(0,5):
        results[i] = test(dur=dur)

    print("Waiting ....")
    print(results[0].result())


if __name__ == "__main__": 

    test_all(dur=1)
