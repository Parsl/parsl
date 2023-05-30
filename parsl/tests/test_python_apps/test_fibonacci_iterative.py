import argparse

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.local_threads import config


@python_app
def get_num(first, second):
    return first + second


def test_fibonacci(num=3):
    counter = 0
    results = []
    results.append(0)
    results.append(1)
    while counter < num - 2:
        counter += 1
        results.append(get_num(results[counter - 1], results[counter]))
    for i in range(len(results)):
        if isinstance(results[i], int):
            print(results[i])
        else:
            print(results[i].result())
