import pytest

from parsl import python_app
from parsl.tests.configs.htex_local import fresh_config
from parsl.serialize.errors import SerializationError


def local_config():
    config = fresh_config()
    config.retries = 2
    return config


@python_app
def fail_pickling(x):
    return next(x)


def generator():
    num = 0
    while True:
        yield num
        num += 1


@pytest.mark.local
def test_serialization_error():

    gen = generator()
    x = fail_pickling(gen)

    assert x.exception() is not None, "Should have got an exception, actually got: " + repr(x.result())
    assert isinstance(x.exception(), SerializationError)
