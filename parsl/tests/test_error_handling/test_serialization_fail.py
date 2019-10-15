import pytest

import threading
from parsl import python_app
from parsl.tests.configs.htex_local import fresh_config
from parsl.executors.errors import SerializationError

local_config = fresh_config()
local_config.retries = 2


@python_app
def fail_pickling(x):
    return True


@pytest.mark.local
def test_serialization_error():

    lock = threading.Lock()
    x = fail_pickling(lock)

    try:
        x.result()
    except Exception as e:
        assert isinstance(e, SerializationError)
    else:
        raise ValueError
