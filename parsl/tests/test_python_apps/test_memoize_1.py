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


if __name__ == '__main__':
    parsl.clear()
    parsl.load(config)

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_python_memoization(n=4)
