import argparse
import datetime
import time

import pytest
from dateutil.parser import parse

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.local_threads_checkpoint_periodic import config


def local_setup():
    global dfk
    dfk = parsl.load(config)


def local_teardown():
    # explicit clear without dfk.cleanup here, because the
    # test does that already
    parsl.clear()


@python_app(cache=True)
def slow_double(x, sleep_dur=1):
    import time
    time.sleep(sleep_dur)
    return x * 2


def tstamp_to_seconds(line):
    print("Parsing line: ", line)
    parsed = parse(line[0:23], fuzzy=True)
    epoch = datetime.datetime.utcfromtimestamp(0)
    f = (parsed - epoch).total_seconds()
    return f


@pytest.mark.local
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

    time.sleep(16)
    dfk.cleanup()

    # Here we will check if the loglines came back with 5 seconds deltas
    print("Rundir: ", dfk.run_dir)

    with open("{}/parsl.log".format(dfk.run_dir), 'r') as f:
        log_lines = f.readlines()
        expected_msg = "]  Done checkpointing"
        expected_msg2 = "]  No tasks checkpointed in this pass"

        lines = [line for line in log_lines if expected_msg in line or expected_msg2 in line]
        assert len(lines) >= 3, "Insufficient checkpoint lines in logfile"
        deltas = [tstamp_to_seconds(line) for line in lines]
        assert deltas[1] - deltas[0] < 5.5, "Delta between checkpoints exceeded period"
        assert deltas[2] - deltas[1] < 5.5, "Delta between checkpoints exceeded period"


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
