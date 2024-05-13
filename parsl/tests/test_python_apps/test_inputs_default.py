import pytest

import parsl
from parsl import python_app
from parsl.executors.threads import ThreadPoolExecutor


def local_config():
    return parsl.Config(executors=[ThreadPoolExecutor()])


@pytest.mark.local
def test_default_inputs():
    @python_app
    def identity(inp):
        return inp

    @python_app
    def add_inputs(inputs=[identity(1), identity(2)]):
        return sum(inputs)

    assert add_inputs().result() == 3
