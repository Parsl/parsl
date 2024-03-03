from pathlib import Path
from threading import Event
from typing import Iterable

import pytest

import parsl
from parsl.tests.configs.local_threads import fresh_config as local_config


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
def b_first(x: Iterable[int]):
    return x[0] + 1


@pytest.mark.local
def test_tuple_pos_arg():
    e = Event()
    s = (a(e),)
    f_b = b_first(s)
    e.set()
    assert f_b.result() == 8


@parsl.python_app
def make_path(s: str):
    return Path(s)


@parsl.python_app
def append_paths(iterable: Iterable[Path], end_str: str = "end"):
    type_ = type(iterable)
    return type_([Path(s, end_str) for s in iterable])


@pytest.mark.local
@pytest.mark.parametrize(
    "type_",
    [
        tuple,
        list,
        set,
    ],
)
def test_resolving_iterables(type_):
    output1 = make_path("test1")
    output2 = make_path("test2")
    output3 = append_paths(type_([output1, output2]), end_str="end")
    assert output3.result() == type_([Path("test1", "end"), Path("test2", "end")])
