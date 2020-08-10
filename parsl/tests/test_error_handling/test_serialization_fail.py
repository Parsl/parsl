import pytest

from parsl import python_app
from parsl.tests.configs.htex_local import fresh_config
from parsl.executors.errors import SerializationError

local_config = fresh_config()
local_config.retries = 2


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

    try:
        x.result()
    except Exception as e:
        assert isinstance(e, SerializationError)
    else:
        raise ValueError
