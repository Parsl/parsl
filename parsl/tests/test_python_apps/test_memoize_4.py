import argparse

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.local_threads import config


def test_python_memoization(n=2):
    """Testing python memoization when func bodies differ
    """
    @python_app
    def random_uuid(x):
        import uuid
        return str(uuid.uuid4())

    x = random_uuid(0)
    print(x.result())

    @python_app
    def random_uuid(x):
        import uuid
        print("hi")
        return str(uuid.uuid4())

    y = random_uuid(0)
    assert x.result() != y.result(), "Memoized results were not used"
