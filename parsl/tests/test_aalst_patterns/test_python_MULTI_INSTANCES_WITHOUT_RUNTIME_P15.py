# #In this pattern, multiple instances of an activity can be enabled, but the
# number of instances of a given activity is not known at any point during design
# time or runtime.

from parsl import *
import random
import argparse

workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers])


@App('python', dfk)
def rand():
    x = random.randint(1, 10)
    return x


@App('python', dfk)
def increment_one(x):
    return x + 1


@App('python', dfk)
def sum_elements(x=[]):
    total = 0
    for i in range(len(x)):
        total += x[i]
    return total


def test_multi_instances():
    numbers = []
    while sum_elements(numbers).result() < 20:
        numbers.append(increment_one(rand().result()).result())
    print(sum_elements(numbers).result())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-x", "--int", default="5",
                        action="store", dest="x", type=int)
    args = parser.parse_args()
    test_multi_instances()
