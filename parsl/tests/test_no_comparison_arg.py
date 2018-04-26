'''
Regression test for #226.
'''
from parsl import *

config = {
    "sites": [
        {
            "site": "local_threads",
            "auth": {
                "channel": None
            },
            "execution": {
                "executor": "threads",
                "provider": None,
                "maxThreads": 10,
            }
        }
    ]
}
dfk = DataFlowKernel(config=config)


class Foo(object):
    def __init__(self, x):
        self.x = x

    def __eq__(self, value):
        raise NotImplementedError


bar = Foo(1)


@App('python', dfk)
def get_foo_x(b=bar):
    return b.x


def test_no_comparison_arg():
    res = get_foo_x().result()
    assert res == 1, 'Expected 1, returned {}'.format(res)


if __name__ == '__main__':
    test_no_comparison_arg()
