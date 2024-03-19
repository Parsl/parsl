import parsl
from parsl.tests.configs.local_threads import config
import pytest
from parsl.errors import NoDataFlowKernelError


@parsl.python_app
def square(x):
    return x * x


@parsl.bash_app
def foo(x, y, z=30, stdout='foo.stdout', label=None):
    return f"echo {x} {y} {z}"


def local_setup():
    pass


def local_teardown():
    parsl.clear()


@pytest.mark.local
def test_within_context_manger():
    with parsl.load(config=config):
        py_future = square(2)
        assert py_future.result() == 4

        bash_future = foo(10, 20)
        assert bash_future.result() == 0

        with open('foo.stdout', 'r') as f:
            assert f.read() == "10 20 30\n"

    with pytest.raises(NoDataFlowKernelError) as excinfo:
        square(2).result()
    assert str(excinfo.value) == "Cannot submit to a DFK that has been cleaned up"
