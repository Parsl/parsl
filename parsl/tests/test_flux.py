import os
import concurrent.futures as cf

import pytest

from parsl.app.errors import AppException
from parsl.executors.flux.executor import (
    FluxExecutor,
    FluxFutureWrapper,
    _complete_future,
)

try:
    import flux.job.executor  # noqa: F401
except ImportError:
    FLUX_AVAIL = False
else:
    FLUX_AVAIL = True

require_flux = pytest.mark.skipif(
    not FLUX_AVAIL, reason="Flux not available, test will fail"
)
ERRMSG = "Some error message"


def multiply(x, y):
    return x * y


def bad_foo():
    raise ValueError(ERRMSG)


@require_flux
@pytest.mark.local
def test_multiply():
    with FluxExecutor() as executor:
        executor.start()
        futures = [executor.submit(multiply, {}, i, 7) for i in range(5)]
        for i, future in enumerate(futures):
            assert future.result() == i * 7
            assert future.done()
            assert future.exception() is None
            assert isinstance(future, FluxFutureWrapper)


@require_flux
@pytest.mark.local
def test_except():
    with FluxExecutor() as executor:
        executor.start()
        future = executor.submit(bad_foo, {})
        with pytest.raises(ValueError, match=ERRMSG):
            future.result()
        assert isinstance(future.exception(), ValueError)


@require_flux
@pytest.mark.local
@pytest.mark.skipif(
    not hasattr(os, "sched_getaffinity") or len(os.sched_getaffinity(0)) < 2,
    reason="Not Linux or too few CPUs",
)
def test_affinity():
    with FluxExecutor() as executor:
        executor.start()
        future = executor.submit(os.sched_getaffinity, {"cores_per_task": 2}, 0)
        assert len(future.result()) > 1


@require_flux
@pytest.mark.local
def test_cancel():
    with FluxExecutor() as executor:
        executor.start()
        futures = [executor.submit(multiply, {}, i, 9) for i in range(3)]
        for i, future in enumerate(futures):
            if future.cancel():
                assert future.cancelled()
                assert future.done()
                with pytest.raises(cf.CancelledError):
                    future.exception()
                with pytest.raises(cf.CancelledError):
                    future.result()
            else:
                assert future.running()
                assert future.done()
                assert not future.cancelled()
                assert future.result() == i * 9


@pytest.mark.local
def test_future_cancel():
    underlying_future = cf.Future()
    wrapper_future = FluxFutureWrapper()
    wrapper_future._flux_future = underlying_future
    assert not wrapper_future.done()
    assert not wrapper_future.running()
    assert not wrapper_future.cancelled()
    assert wrapper_future.cancel()  # should cancel underlying future
    assert wrapper_future.cancel()  # try again for good measure
    assert wrapper_future.cancelled()
    assert wrapper_future.done()
    assert underlying_future.cancelled()
    assert underlying_future.done()


@pytest.mark.local
def test_future_running():
    underlying_future = cf.Future()
    wrapper_future = FluxFutureWrapper()
    assert not underlying_future.running()
    assert underlying_future.set_running_or_notify_cancel()
    assert underlying_future.running()
    assert not wrapper_future.running()
    wrapper_future._flux_future = underlying_future
    assert wrapper_future.running()


@pytest.mark.local
def test_future_callback_returncode():
    testfile = ".fluxexecutortest.txt"
    returncode = 1
    underlying_future = cf.Future()
    wrapper_future = FluxFutureWrapper()
    wrapper_future._flux_future = underlying_future
    underlying_future.add_done_callback(
        lambda fut: _complete_future(testfile, wrapper_future, fut)
    )
    underlying_future.set_result(returncode)
    assert wrapper_future.done()
    assert isinstance(wrapper_future.exception(), AppException)


@pytest.mark.local
def test_future_callback_nofile():
    testfile = ".fluxexecutortest.txt"
    returncode = 0
    if os.path.isfile(testfile):
        os.remove(testfile)
    underlying_future = cf.Future()
    wrapper_future = FluxFutureWrapper()
    wrapper_future._flux_future = underlying_future
    underlying_future.add_done_callback(
        lambda fut: _complete_future(testfile, wrapper_future, fut)
    )
    underlying_future.set_result(returncode)
    assert wrapper_future.done()
    assert isinstance(wrapper_future.exception(), FileNotFoundError)


@pytest.mark.local
def test_future_callback_flux_exception():
    underlying_future = cf.Future()
    wrapper_future = FluxFutureWrapper()
    wrapper_future._flux_future = underlying_future
    underlying_future.add_done_callback(
        lambda fut: _complete_future(".fluxexecutortest.txt", wrapper_future, fut)
    )
    underlying_future.set_exception(ValueError())
    assert wrapper_future.done()
    assert isinstance(wrapper_future.exception(), ValueError)


@pytest.mark.local
def test_future_cancel_no_underlying_future():
    wrapper_future = FluxFutureWrapper()
    assert wrapper_future.cancel()
    assert wrapper_future.cancelled()
    assert not wrapper_future.running()
