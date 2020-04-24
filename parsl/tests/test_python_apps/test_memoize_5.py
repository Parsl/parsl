import argparse

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.local_threads import config


def test_python_memoization(n=2):
    """Testing python memoization when func bodies differ
    This is the canonical use case.
    """
    @python_app
    def random_uuid(x, cache=True):
        import uuid
        return str(uuid.uuid4())

    x = random_uuid(0)
    print(x.result())

    @python_app
    def random_uuid(x, cache=True):
        import uuid
        print("hi")
        return str(uuid.uuid4())

    y = random_uuid(0)
    assert x.result() != y.result(), "Memoized results were not used"


if __name__ == '__main__':

    parsl.clear()
    dfk = parsl.load(config)

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_python_memoization(n=4)
