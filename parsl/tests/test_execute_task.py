import os

import pytest

from parsl.executors.execute_task import execute_task
from parsl.serialize.facade import pack_res_spec_apply_message


def addemup(*args: int, name: str = "apples"):
    total = sum(args)
    return f"{total} {name}"


@pytest.mark.local
def test_execute_task():
    args = (1, 2, 3)
    kwargs = {"name": "boots"}
    buff = pack_res_spec_apply_message(addemup, args, kwargs, {})
    res = execute_task(buff)
    assert res == addemup(*args, **kwargs)


@pytest.mark.local
def test_execute_task_resource_spec():
    resource_spec = {"num_nodes": 2, "ranks_per_node": 2, "num_ranks": 4}
    buff = pack_res_spec_apply_message(addemup, (1, 2), {}, resource_spec)
    execute_task(buff)
    for key, val in resource_spec.items():
        assert os.environ[f"PARSL_{key.upper()}"] == str(val)
