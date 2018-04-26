'''
Regression test for #226.
'''
from parsl import *

from parsl.configs.local import localThreads as config
dfk = DataFlowKernel(config=config)
import pandas as pd


class Foo(object):
    def __init__(self, x):
        self.x = x

    def __eq__(self, value):
        raise NotImplementedError


bar = Foo(1)


@App('python', dfk)
def get_foo_x(a, b=bar, c=None):
    return b.x


data = pd.DataFrame({'x': [None, 2, [3]]})


@App('python', dfk)
def get_dataframe(d=data):
    return d


@App('bash', dfk)
def echo(msg, postfix='there', stdout='std.out'):
    return 'echo {} {}'.format(msg, postfix)


def test_no_eq():
    res = get_foo_x('foo').result()
    assert res == 1, 'Expected 1, returned {}'.format(res)


def test_get_dataframe():
    res = get_dataframe().result()
    assert res.equals(data), 'Unexpected dataframe'


def test_bash_default_arg():
    echo('hello').result()
    with open('std.out', 'r') as f:
        assert f.read().strip() == 'hello there', "Output should be 'hello there'"


if __name__ == '__main__':
    test_no_eq()
    test_bash_default_arg()
    test_get_dataframe()
