import argparse

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.local_threads import config


@python_app(cache=True)
def random_uuid(x, cache=True):
    import uuid
    return str(uuid.uuid4())


def test_python_memoization(n=2):
    """Testing python memoization disable
    """
    x = random_uuid(0)
    print(x.result())

    for i in range(0, n):
        foo = random_uuid(0)
        print(foo.result())
        assert foo.result() == x.result(), "Memoized results were not used"
