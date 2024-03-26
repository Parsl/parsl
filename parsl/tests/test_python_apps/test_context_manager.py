import parsl
from parsl.tests.configs.local_threads import fresh_config
import pytest
from parsl.errors import NoDataFlowKernelError


@parsl.python_app
def square(x):
    return x * x


@parsl.bash_app
def foo(x, stdout='foo.stdout'):
    return f"echo {x + 1}"


def local_setup():
    pass


def local_teardown():
    parsl.clear()


@pytest.mark.local
def test_within_context_manger():
    config = fresh_config()
    with parsl.load(config=config):
        py_future = square(2)
        assert py_future.result() == 4

        bash_future = foo(1)
        assert bash_future.result() == 0

        with open('foo.stdout', 'r') as f:
            assert f.read() == "2\n"

    with pytest.raises(NoDataFlowKernelError) as excinfo:
        square(2).result()
    assert str(excinfo.value) == "Cannot submit to a DFK that has been cleaned up"
