import pytest

from parsl.app.app import python_app


@python_app
def double(x):
    return x * 2


@python_app
def import_square(x):
    import math
    return math.pow(x, 2)


@python_app
def custom_exception():
    from globus_sdk import GlobusError
    raise GlobusError('foobar')


def test_simple(n=2):
    x = double(n)
    assert x.result() == n * 2


@pytest.mark.parametrize("n", (-2, -1, 0, 1, 2, 3))
def test_imports(n):
    x = import_square(n)
    assert x.result() == n * n


@pytest.mark.parametrize("n", (0, 1, 2, 3, 5, 8, 13, 21))
def test_parallel_for(n):
    d = {i: double(i) for i in range(n)}
    assert len(d.keys()) == n

    for i in d:
        assert d[i].result() == 2 * i


def test_custom_exception():
    from globus_sdk import GlobusError

    x = custom_exception()
    with pytest.raises(GlobusError):
        x.result()
