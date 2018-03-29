import parsl
from parsl import *
import time
import argparse
from dateutil.parser import parse
import datetime
from nose.tools import nottest
# parsl.set_stream_logger()

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
                "checkpointMode": "periodic",
                "checkpointPeriod": "00:00:05",
    }
}
dfk = DataFlowKernel(config=config)


@App('python', dfk, cache=True)
def slow_double(x, sleep_dur=1):
    import time
    time.sleep(sleep_dur)
    return x * 2


def tstamp_to_seconds(line):
    print("Parsing line : ", line)
    parsed = parse(line, fuzzy=True)
    epoch = datetime.datetime.utcfromtimestamp(0)
    f = (parsed - epoch).total_seconds()
    return f


@nottest
def test_periodic(n=4):
    """Test checkpointing with task_periodic behavior
    """

    d = {}

    print("Launching : ", n)
    for i in range(0, n):
        d[i] = slow_double(i)
    print("Done launching")

    for i in range(0, n):
        d[i].result()
    print("Done sleeping")

    time.sleep(10)
    dfk.cleanup()

    # Here we will check if the loglines came back with 5 seconds deltas
    print("Rundir : ", dfk.rundir)

    with open("{}/parsl.log".format(dfk.rundir), 'r') as f:
        expected_msg = "] check".format(n)
        lines = [line for line in f.readlines() if expected_msg in line.lower()]
        deltas = [tstamp_to_seconds(line) for line in lines]
        assert int(deltas[1] - deltas[0]) == 5, "Delta between checkpoints exceeded period "
        assert int(deltas[2] - deltas[1]) == 5, "Delta between checkpoints exceeded period "


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_periodic(n=4)
