"""Testing behavior of futures

We have the following cases for AppFutures:

1. App launched immmediately on call
2. App launch was delayed (due to unresolved dep(s))

Same applies to datafutures, and we need to know the behavior wrt.

1. result() called on 1, vs 2
2. done() called on 1, vs 2

"""
import pytest
from os.path import basename

from parsl.app.app import python_app
from parsl.data_provider.files import File


@python_app
def delay_incr(x, delay=0.0, outputs=()):
    import time
    if outputs:
        with open(outputs[0].filepath, 'w') as outs:
            outs.write(str(x + 1))
    time.sleep(delay)
    return x + 1


def get_contents(filename):
    with open(filename, 'r') as f:
        return f.read()


def test_fut_case_1():
    """Testing the behavior of AppFutures where there are no dependencies
    """

    app_fu = delay_incr(1, delay=0.01)
    assert app_fu.result() == 2


@pytest.mark.staging_required
def test_fut_case_2(tmp_path):
    """Testing the behavior of DataFutures where there are no dependencies
    """
    output_f = tmp_path / 'test_fut_case_2.txt'
    app_fu = delay_incr(1, delay=0.01, outputs=[File(str(output_f))])
    data_fu = app_fu.outputs[0]

    result = data_fu.result().filepath
    assert basename(result) == output_f.name, "DataFuture did not return filename"

    contents = get_contents(result)
    assert contents == '2'


def test_fut_case_3():
    """Testing the behavior of AppFutures where there are dependencies

    The first call has a delay, and the second call depends on the first
    """

    app_1 = delay_incr(1, delay=0.01)
    app_2 = delay_incr(app_1)

    assert app_2.result() == 3


@pytest.mark.staging_required
def test_fut_case_4(tmp_path):
    """Testing the behavior of DataFutures where there are dependencies

    The first call has a delay, and the second call depends on the first
    """
    output_f1 = tmp_path / 'test_fut_case_4_f1.txt'
    output_f2 = tmp_path / 'test_fut_case_4_f2.txt'
    app_1 = delay_incr(1, delay=0.01, outputs=[File(str(output_f1))])
    app_2 = delay_incr(app_1, delay=0.01, outputs=[File(str(output_f2))])
    data_2 = app_2.outputs[0]

    result = data_2.result().filepath
    assert basename(result) == output_f2.name, "DataFuture did not return the filename"

    contents = get_contents(result)
    assert contents == '3'
