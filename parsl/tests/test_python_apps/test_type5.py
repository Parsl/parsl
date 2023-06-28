import pytest

from parsl.app.app import python_app


@python_app
def map_x2(x):
    return x * 2


@python_app
def add_xy(x, y):
    return x + y


def test_func_linked_to_previous_result(depth=2):
    futs = (map_x2(i) for i in range(1, depth + 1))
    for _ in range(1, depth - 1):
        futs = (map_x2(fut) for fut in futs)
    futs = [map_x2(fut) for fut in futs]

    assert sum(f.result() for f in futs) == (2 ** depth) * sum(range(1, depth + 1))


def test_func_linked_to_multiple_previous_results(width=4):
    futs = (map_x2(i) for i in range(1, width + 1))
    f_it = iter(futs)

    # Requires that len(futs) % 2 == 0 (even number of items)
    futs = [add_xy(f, next(f_it)) for f in f_it]
    assert sum(f.result() for f in futs) == 2 * sum(range(1, width + 1))
