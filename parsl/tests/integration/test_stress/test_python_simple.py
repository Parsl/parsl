import argparse
import time

import parsl
from parsl import python_app
from parsl.configs.htex_local import config


@python_app
def increment(x):
    return x + 1


def test_stress(count=1000):
    """Threaded app RTT stress test"""

    start = time.time()
    x = []
    for i in range(count):
        f = increment(i)
        x.append(f)
    end = time.time()
    print("Launched {0} tasks in {1:.2f} s".format(count, end - start))

    [f.result() for f in x]
    end = time.time()
    print("Completed {0} tasks in {1:.2f} s".format(count, end - start))


if __name__ == '__main__':
    parsl.load(config)

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", type=int, default=1000,
                        help="width of the pipeline")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Enable stream logging")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    test_stress(count=args.count)
