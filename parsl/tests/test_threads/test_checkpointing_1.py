import os
from parsl import *


def test_1():
    """Checkpointing example from docs.
    """
    from parsl.configs.local import localThreads as config
    dfk = DataFlowKernel(config=config)

    @App('python', dfk, cache=True)
    def slow_double(x, sleep_dur=1):
        import time
        time.sleep(sleep_dur)
        return x * 2

    N = 5   # Number of calls to slow_double
    d = []  # List to store the futures
    for i in range(0, N):
        d.append(slow_double(i))

    # Wait for the results
    [i.result() for i in d]

    cpt_dir = dfk.checkpoint()
    print(cpt_dir)  # Prints the checkpoint dir

    # Testing component
    assert os.path.exists(cpt_dir), "Checkpoint dir does not exist"
    dfk.cleanup()
