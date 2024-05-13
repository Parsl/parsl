import logging

import pytest

from parsl.process_loggers import wrap_with_logs


@wrap_with_logs
def somefunc_ok():
    return 5


@wrap_with_logs
def somefunc_exception():
    raise RuntimeError("Deliberate failure")


@pytest.mark.local
def test_wrap_with_logs_ok(caplog):
    caplog.set_level(logging.DEBUG)

    # check that return value is passed back through wrap_with_logs
    x = somefunc_ok()
    assert x == 5
    assert 'Normal ending' in caplog.text


@pytest.mark.local
def test_wrap_with_logs_exception(caplog):
    caplog.set_level(logging.ERROR)
    # check that exception is passed back through wrap_with_logs
    with pytest.raises(RuntimeError):
        somefunc_exception()
    assert 'Exceptional ending' in caplog.text
