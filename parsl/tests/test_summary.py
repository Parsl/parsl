import pytest

import parsl
from parsl.tests.configs.local_threads import fresh_config


@parsl.python_app
def succeed():
    pass


@parsl.python_app
def fail():
    raise RuntimeError("Deliberate failure in fail() app")


@pytest.mark.local
def test_summary(caplog):

    parsl.load(fresh_config())

    succeed().result()
    fail().exception()

    parsl.dfk().cleanup()

    assert "Summary of tasks in DFK:" in caplog.text
    assert "Tasks in state States.exec_done: 1" in caplog.text
    assert "Tasks in state States.failed: 1" in caplog.text
