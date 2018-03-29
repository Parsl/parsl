from parsl import *
import parsl
import os
parsl.set_stream_logger()
os.environ["THETA_USERNAME"] = "yadunand"
from theta import multiNode as config
dfk = DataFlowKernel(config=config)


@App("python", dfk)
def platform_info(dur=1):
    import platform
    import time
    time.sleep(dur)
    return "Hello from {0}".format(platform.uname())


def test_all(N=20, dur=1):
    """ Testing local:cobalt:aprun, with checks on number of nodes
    """
    results = {}
    for i in range(0, 5):
        results[i] = platform_info(dur=dur)

    print("Waiting ....")
    uniq_set = set([results[rid].result() for rid in results])
    print("Unique nodes : ", len(uniq_set))


if __name__ == "__main__":

    test_all(dur=1)
