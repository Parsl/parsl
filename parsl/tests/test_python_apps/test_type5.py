import pytest

from parsl.app.app import python_app


@python_app
def map_x2(x):
    return x * 2


@python_app
def add_xy(x, y):
    return x + y


@pytest.mark.parametrize("n", (2, 3, 5))
def test_func_linked_to_previous_result(n):
    futs = (map_x2(i) for i in range(1, n + 1))
    for _ in range(1, n - 1):
        futs = (map_x2(fut) for fut in futs)
    futs = [map_x2(fut) for fut in futs]

    assert sum(f.result() for f in futs) == (2 ** n) * sum(range(1, n + 1))


@pytest.mark.parametrize("width", (2, 4, 8, 32))
def test_func_linked_to_multiple_previous_results(width):
    futs = (map_x2(i) for i in range(1, width + 1))
    f_it = iter(futs)

    # Requires that len(futs) % 2 == 0 (even number of items)
    futs = [add_xy(f, next(f_it)) for f in f_it]
    assert sum(f.result() for f in futs) == 2 * sum(range(1, width + 1))
