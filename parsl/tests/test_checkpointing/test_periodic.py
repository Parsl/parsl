import pytest

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.local_threads_checkpoint_periodic import fresh_config


def local_setup():
    parsl.load(fresh_config())


@python_app(cache=True)
def slow_double(x, sleep_dur=1):
    import time
    time.sleep(sleep_dur)
    return x * 2


def tstamp_to_seconds(line):
    f = line.partition(" ")[0]
    return float(f)


@pytest.mark.local
def test_periodic():
    """Test checkpointing with task_periodic behavior
    """
    h, m, s = map(int, parsl.dfk().config.checkpoint_period.split(":"))
    assert h == 0, "Verify test setup"
    assert m == 0, "Verify test setup"
    assert s > 0, "Verify test setup"
    sleep_for = s + 1
    with parsl.dfk():
        futs = [slow_double(sleep_for) for _ in range(4)]
        [f.result() for f in futs]
        run_dir = parsl.dfk().run_dir

    # Here we will check if the loglines came back with 5 seconds deltas
    with open("{}/parsl.log".format(run_dir)) as f:
        log_lines = f.readlines()
    expected_msg = " Done checkpointing"
    expected_msg2 = " No tasks checkpointed in this pass"

    lines = [line for line in log_lines if expected_msg in line or expected_msg2 in line]
    assert len(lines) >= 3, "Insufficient checkpoint lines in logfile"
    deltas = [tstamp_to_seconds(line) for line in lines]
    assert deltas[1] - deltas[0] < 5.5, "Delta between checkpoints exceeded period"
    assert deltas[2] - deltas[1] < 5.5, "Delta between checkpoints exceeded period"
