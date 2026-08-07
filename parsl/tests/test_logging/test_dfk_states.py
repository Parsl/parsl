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

    expected_states = {"pending", "launched", "exec_done"}

    seen_states = {r.__dict__['parsl.task_state']
                   for r in caplog.records
                   if 'parsl.task_state' in r.__dict__}

    assert expected_states <= seen_states, "every expected state should have been seen"

    for s in expected_states:
        assert s in caplog.text, f"Expected state {s} to be logged as text"


@pytest.mark.local
def test_state_logs_fail(caplog):
    with parsl.load():
        fail().exception()

    expected_states = {"pending", "launched", "failed"}

    seen_states = {r.__dict__['parsl.task_state']
                   for r in caplog.records
                   if 'parsl.task_state' in r.__dict__}

    assert expected_states <= seen_states, "every expected state should have been seen"

    for s in expected_states:
        assert s in caplog.text, f"Expected state {s} to be logged as text"
