''' Testing python apps
'''

from parsl import *
from parsl.configs.local import localThreads

# parsl.set_stream_logger()

dfk = DataFlowKernel(config=localThreads)


@App('python', dfk)
def worker_identify(x, sleep_dur=0.2):
    import time
    import os
    import threading
    time.sleep(sleep_dur)
    return {"pid": os.getpid(),
            "tid": threading.current_thread()}


def test_parallel_for(n=8):

    d = []
    for i in range(0, n):
        d.extend([worker_identify(i)])

    [item.result() for item in d]

    thread_count = len(set([item.result()['tid'] for item in d]))
    process_count = len(set([item.result()['pid'] for item in d]))
    assert thread_count == localThreads["sites"][0]["execution"]["maxThreads"], "Wrong number of threads"
    assert process_count == 1, "More processes than allowed"
    return d


if __name__ == "__main__":

    test_parallel_for()
