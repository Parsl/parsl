import parsl
import pytest
from parsl.tests.configs.local_threads import fresh_config
from parsl.tests.logfixtures import permit_severe_log


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

    with permit_severe_log():
        fail().exception()

    parsl.dfk().cleanup()
    parsl.clear()

    assert "Summary of tasks in DFK:" in caplog.text
    assert "Tasks in state States.done: 1" in caplog.text
    assert "Tasks in state States.failed: 1" in caplog.text
