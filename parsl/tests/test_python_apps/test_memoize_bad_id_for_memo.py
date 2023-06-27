import pytest
from parsl import python_app
from parsl.dataflow.memoization import id_for_memo


# this class should not have a memoizer registered for it
class Unmemoizable:
    pass


# this class should have a memoizer that always raises an
# exception
class FailingMemoizable:
    pass


class FailingMemoizerTestError(ValueError):
    pass


@id_for_memo.register(FailingMemoizable)
def failing_memoizer(v, output_ref=False):
    raise FailingMemoizerTestError("Deliberate memoizer failure")


@python_app(cache=True)
def noop_app(x, inputs=[], cache=True):
    return None


@python_app
def some_func(_t):
    pass


def test_python_unmemoizable():
    """Testing behaviour when an unmemoizable parameter is used
    """
    fut = noop_app(Unmemoizable())
    with pytest.raises(ValueError):
        fut.result()


def test_python_failing_memoizer():
    """Testing behaviour when id_for_memo raises an exception
    """
    fut = noop_app(FailingMemoizable())
    with pytest.raises(FailingMemoizerTestError):
        fut.result()


def test_python_unmemoizable_after_dep():
    memoizable_fut = some_func(1)
    fut = noop_app(Unmemoizable(), inputs=[memoizable_fut])
    with pytest.raises(ValueError):
        fut.result()


def test_python_failing_memoizer_afer_dep():
    memoizable_fut = some_func(1)
    fut = noop_app(FailingMemoizable(), inputs=[memoizable_fut])
    with pytest.raises(ValueError):
        fut.result()
