import pytest

import parsl


@parsl.python_app
def noop():
    pass


@parsl.python_app
def fail():
    raise RuntimeError()


@pytest.mark.local
def test_state_logs_success(caplog):
    with parsl.load():
        noop().result()

    # from the DFK logs, we wouldn't expect to see running or
    # running_done states as those exist only in the worker.
    for s in ("pending", "launched", "exec_done", ):
        assert s in caplog.text, f"Expected state {s} to be logged"


@pytest.mark.local
def test_state_logs_fail(caplog):
    with parsl.load():
        noop().result()

    for s in ("pending", "launched", "failed", ):
        assert s in caplog.text, f"Expected state {s} to be logged"
