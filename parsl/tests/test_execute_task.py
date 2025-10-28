import os

import pytest

from parsl.executors.execute_task import execute_task
from parsl.serialize.facade import pack_apply_message


def addemup(*args: int, name: str = "apples"):
    total = sum(args)
    return f"{total} {name}"


@pytest.mark.local
def test_execute_task():
    args = (1, 2, 3)
    kwargs = {"name": "boots"}
    buff = pack_apply_message(addemup, args, kwargs)
    res = execute_task(buff)
    assert res == addemup(*args, **kwargs)
