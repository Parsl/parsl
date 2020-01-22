import argparse

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.local_threads import config


@python_app
def fibonacci(n):
    if n == 0:
        return 0
    elif n == 2 or n == 1:
        return 1
    else:
        return fibonacci(n - 1).result() + fibonacci(n - 2).result()


def test_fibonacci(x=3):
    results = []
    for i in range(x):
        results.append(fibonacci(i))
    for j in range(len(results)):
        while results[j].done() is not True:
            pass
        print(results[j].result())


if __name__ == '__main__':
    parsl.clear()
    parsl.load(config)
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--x", default='5',
                        action="store", dest="b", type=int)
    args = parser.parse_args()
    test_fibonacci(args.b)
