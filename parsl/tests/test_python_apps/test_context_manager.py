import pytest

import parsl
from parsl.dataflow.dflow import DataFlowKernel
from parsl.errors import NoDataFlowKernelError
from parsl.tests.configs.local_threads import fresh_config


@parsl.python_app
def square(x):
    return x * x


@parsl.bash_app
def foo(x, stdout='foo.stdout'):
    return f"echo {x + 1}"


@pytest.mark.local
def test_within_context_manger(tmpd_cwd):
    config = fresh_config()
    with parsl.load(config=config) as dfk:
        assert isinstance(dfk, DataFlowKernel)

        bash_future = foo(1, stdout=tmpd_cwd / 'foo.stdout')
        assert bash_future.result() == 0

        with open(tmpd_cwd / 'foo.stdout', 'r') as f:
            assert f.read() == "2\n"

    with pytest.raises(NoDataFlowKernelError) as excinfo:
        square(2).result()
    assert str(excinfo.value) == "Must first load config"
