import pytest

from parsl.app.app import python_app


@python_app
def times_two(x):
    return x * 2


@python_app
def accumulate(inputs=()):
    return sum(inputs)


@python_app
def accumulate_t(*args):
    return sum(args)


@pytest.mark.parametrize("width", (2, 3, 5))
def test_mapred_type1(width):
    """MapReduce test with the reduce stage taking futures in inputs=[]"""
    futs = [times_two(i) for i in range(width)]
    red = accumulate(inputs=futs)
    assert red.result() == 2 * sum(range(width))


@pytest.mark.parametrize("width", (2, 3, 5))
def test_mapred_type2(width):
    """MapReduce test with the reduce stage taking futures on the args"""
    futs = [times_two(i) for i in range(width)]
    red = accumulate_t(*futs)
    assert red.result() == 2 * sum(range(width))
