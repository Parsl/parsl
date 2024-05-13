from concurrent.futures import Future
from threading import Event

import pytest

import parsl
from parsl.config import Config
from parsl.dataflow.dflow import DataFlowKernel, DataFlowKernelLoader
from parsl.errors import NoDataFlowKernelError
from parsl.tests.configs.local_threads import fresh_config


@parsl.python_app
def square(x):
    return x * x


@parsl.bash_app
def foo(x, stdout='foo.stdout'):
    return f"echo {x + 1}"


@parsl.python_app
def wait_for_event(ev: Event):
    ev.wait()


@parsl.python_app
def raise_app():
    raise RuntimeError("raise_app deliberate failure")


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


@pytest.mark.local
def test_exit_skip():
    config = fresh_config()
    config.exit_mode = "skip"

    with parsl.load(config) as dfk:
        ev = Event()
        fut = wait_for_event(ev)
        # deliberately don't wait for this to finish, so that the context
        # manager can exit

    assert parsl.dfk() is dfk, "global dfk should be left in place by skip mode"

    assert not fut.done(), "wait_for_event should not be done yet"
    ev.set()

    # now we can wait for that result...
    fut.result()
    assert fut.done(), "wait_for_event should complete outside of context manager in 'skip' mode"

    # now cleanup the DFK that the above `with` block
    # deliberately avoided doing...
    dfk.cleanup()


# 'wait' mode has two cases to test:
# 1. that we wait when there is no exception
# 2. that we do not wait when there is an exception
@pytest.mark.local
def test_exit_wait_no_exception():
    config = fresh_config()
    config.exit_mode = "wait"

    with parsl.load(config) as dfk:
        fut = square(1)
        # deliberately don't wait for this to finish, so that the context
        # manager can exit

    assert fut.done(), "This future should be marked as done before the context manager exits"

    assert dfk.cleanup_called, "The DFK should have been cleaned up by the context manager"
    assert DataFlowKernelLoader._dfk is None, "The global DFK should have been removed"


@pytest.mark.local
def test_exit_wait_exception():
    config = fresh_config()
    config.exit_mode = "wait"

    with pytest.raises(RuntimeError):
        with parsl.load(config) as dfk:
            # we'll never fire this future
            fut_never = Future()

            fut_raise = raise_app()

            fut_depend = square(fut_never)

            # this should cause an exception, which should cause the context
            # manager to exit, without waiting for fut_depend to finish.
            fut_raise.result()

    assert dfk.cleanup_called, "The DFK should have been cleaned up by the context manager"
    assert DataFlowKernelLoader._dfk is None, "The global DFK should have been removed"
    assert fut_raise.exception() is not None, "fut_raise should contain an exception"
    assert not fut_depend.done(), "fut_depend should have been left un-done (due to dependency failure)"


@pytest.mark.local
def test_exit_wrong_mode():

    with pytest.raises(Exception) as ex:
        Config(exit_mode="wrongmode")

    # with typeguard 4.x this is TypeCheckError,
    # with typeguard 2.x this is TypeError
    # we can't instantiate TypeCheckError if we're in typeguard 2.x environment
    # because it does not exist... so check name using strings.
    assert ex.type.__name__ == "TypeCheckError" or ex.type.__name__ == "TypeError"
