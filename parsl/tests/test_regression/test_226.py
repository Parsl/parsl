'''
Regression test for #226.
'''
import os

import pandas as pd
import pytest

from parsl.app.app import bash_app, python_app


class Foo:
    def __init__(self, x):
        self.x = x

    def __eq__(self, value):
        raise NotImplementedError


bar = Foo(1)


@python_app
def get_foo_x(a, b=bar, c=None):
    return b.x


@bash_app
def get_foo_x_bash(a, b=bar, c=None):
    return "echo {}".format(b.x)


data = pd.DataFrame({'x': [None, 2, [3]]})


@python_app
def get_dataframe(d=data):
    return d


@bash_app
def echo(msg, postfix='there', stdout='std.out'):
    return 'echo {} {}'.format(msg, postfix)


def test_no_eq():
    res = get_foo_x('foo').result()
    assert res == 1, 'Expected 1, returned {}'.format(res)


def test_get_dataframe():
    res = get_dataframe().result()
    assert res.equals(data), 'Unexpected dataframe'


@pytest.mark.shared_fs
def test_bash_default_arg():
    if os.path.exists('std.out'):
        os.remove('std.out')

    echo('hello').result()
    with open('std.out', 'r') as f:
        assert f.read().strip() == 'hello there', "Output should be 'hello there'"
