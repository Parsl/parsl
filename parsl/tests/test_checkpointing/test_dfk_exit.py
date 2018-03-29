import parsl
from parsl import *
import argparse

config = {
    "sites": [
        {"site": "Local_Threads",
         "auth": {"channel": None},
         "execution": {
             "executor": "threads",
             "provider": None,
             "maxThreads": 2,
         }
        }],
    "globals": {"lazyErrors": True,
                "memoize": True,
                "checkpointMode": "dfk_exit",
    }
}
dfk = DataFlowKernel(config=config)


@App('python', dfk, cache=True)
def slow_double(x, sleep_dur=1):
    import time
    time.sleep(sleep_dur)
    return x * 2


def test_at_task_exit(n=4):
    """Test checkpointing at task_exit behavior
    """

    d = {}

    print("Launching : ", n)
    for i in range(0, n):
        d[i] = slow_double(i)
    print("Done launching")

    for i in range(0, n):
        d[i].result()
    print("Done sleeping")

    dfk.cleanup()
    print("Rundir : ", dfk.rundir)
    with open("{}/parsl.log".format(dfk.rundir), 'r') as f:
        expected_msg = "Done checkpointing {} tasks".format(n)
        lines = [line for line in f.readlines() if expected_msg in line]
        assert len(lines) == 1, "Expected {} checkpoint events, got {}".format(1, len(lines))


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_at_task_exit(n=4)
