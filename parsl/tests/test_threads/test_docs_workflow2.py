"""Testing bash apps
"""
import parsl
from parsl import *

print("Parsl version: ", parsl.__version__)

import time

# parsl.set_stream_logger()
workers = ThreadPoolExecutor(max_workers=8)
dfk = DataFlowKernel(executors=[workers])


@App('python', dfk)
def wait_sleep_double(x, fu_1, fu_2):
    import time
    time.sleep(2)   # Sleep for 2 seconds
    return x * 2


def test_parallel(N=10):
    """Parallel workflow example from docs on Composing a workflow
    """

    # Launch two apps, which will execute in parallel, since they don't have to
    # wait on any futures
    start = time.time()
    doubled_x = wait_sleep_double(N, None, None)
    doubled_y = wait_sleep_double(N, None, None)

    # The third depends on the first two :
    #    doubled_x   doubled_y     (2 s)
    #           \     /
    #           doublex_z          (2 s)
    doubled_z = wait_sleep_double(N, doubled_x, doubled_y)

    # doubled_z will be done in ~4s
    print(doubled_z.result())
    time.time()
    delta = time.time() - start

    assert doubled_z.result() == N * \
        2, "Expected doubled_z = N*2 = {0}".format(N * 2)
    assert delta > 4 and delta < 5, "Time delta exceeded expected 4 < duration < 5"


if __name__ == "__main__":

    test_parallel()
