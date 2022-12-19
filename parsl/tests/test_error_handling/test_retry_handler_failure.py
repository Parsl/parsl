import parsl
import pytest


@parsl.python_app
def always_fails():
    raise ValueError("always_fails deliberate exception")


def retry_handler_raises(exc, task_record):
    raise RuntimeError("retry_handler_raises deliberate exception")


def local_config():
    return parsl.config.Config(retry_handler=retry_handler_raises)


@pytest.mark.local
def test_retry_handler_exception():
    fut = always_fails()
    with pytest.raises(RuntimeError):
        fut.result()
    assert fut.exception().args[0] == "retry_handler_raises deliberate exception"
