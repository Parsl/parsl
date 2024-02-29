import parsl
import pytest

from parsl.tests.configs.local_threads import fresh_config as local_config

from typing import Sequence
from threading import Event


@parsl.python_app
def a(event):
    event.wait()
    return 7


@parsl.python_app
def b(x: int):
    return x + 1


@pytest.mark.local
def test_simple_pos_arg():
    e = Event()
    s = a(e)
    f_b = b(s)
    e.set()

    assert f_b.result() == 8


@parsl.python_app
def b_first(x: Sequence[int]):
    return x[0] + 1


@pytest.mark.local
def test_tuple_pos_arg():
    e = Event()
    s = (a(e), )
    f_b = b_first(s)
    e.set()
    assert f_b.result() == 8
