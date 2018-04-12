''' Testing python apps
'''
from parsl import *

from parsl.configs.local import localThreads as config
dfk = DataFlowKernel(config=config)


@App('python', dfk)
def worker_identify(x, sleep_dur=0.2):
    import time
    import threading

    time.sleep(sleep_dur)
    return threading.current_thread()


def test_parallel_for(n=8):

    d = []
    for i in range(0, n):
        d.extend([worker_identify(i)])

    thread_count = len(set([item.result() for item in d]))
    expected = config["sites"][0]["execution"]["maxThreads"]

    assert thread_count == expected, "Got {} Expected {} threads".format(
        thread_count, expected)

    return d


if __name__ == "__main__":

    print(config)
    test_parallel_for()
