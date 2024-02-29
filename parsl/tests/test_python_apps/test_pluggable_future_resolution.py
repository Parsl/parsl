import parsl

from typing import Sequence


@parsl.python_app
def a():
    return 7


@parsl.python_app
def b(x: int):
    return x + 1


def test_simple_pos_arg():
    s = a()
    assert b(s).result() == 8


@parsl.python_app
def b_first(x: Sequence[int]):
    return x[0] + 1


def test_tuple_pos_arg():
    s = (a(), )
    assert b_first(s).result() == 8
