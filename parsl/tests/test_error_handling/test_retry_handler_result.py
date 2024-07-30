import os

import pytest

import parsl
from parsl import python_app
from parsl.dataflow.retries import CompleteWithAlternateValue
from parsl.tests.configs.local_threads import fresh_config


class SpecialException(Exception):
    pass


def error_to_result_handler(exc, task_record):
    """Given a particular exception, turn it into a specific result"""
    if isinstance(exc, SpecialException):
        # substitute the exception with an alternate success
        return CompleteWithAlternateValue(8)
    else:
        return 1  # regular retry cost


def local_config():
    c = fresh_config()
    c.retries = 2
    c.retry_handler = error_to_result_handler
    return c


@python_app
def returns_val():
    return 7


@python_app
def raises_runtime():
    raise RuntimeError("from raises_runtime")


@python_app
def raises_special():
    raise SpecialException(Exception)


@pytest.mark.local
def test_retry():

    # two pre-reqs to validate that results and normal exceptions are handled
    # correctly with the test retry handler
    assert returns_val().result() == 7
    with pytest.raises(RuntimeError):
        raises_runtime().result()

    # the actual test: check that a special exception has been replaced by a
    # a real value
    assert raises_special().result() == 8
