import pytest
from parsl import python_app
from parsl.dataflow.memoization import id_for_memo
from parsl.tests.logfixtures import permit_severe_log


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
    raise FailingMemoizerTestError("BENC TODO")


@python_app(cache=True)
def noop_app(x, cache=True):
    return None


def test_python_unmemoizable():
    """Testing behaviour when an unmemoizable parameter is used
    """
    with permit_severe_log():
        with pytest.raises(ValueError):
            noop_app(Unmemoizable())


def test_python_failing_memoizer():
    """Testing behaviour when id_for_memo raises an exception
    """
    with pytest.raises(FailingMemoizerTestError):
        noop_app(FailingMemoizable())
