# In this pattern, multiple instances of an activity can be enabled, with the
# number of instances of a given activity known during runtime before the
# instances of the activity are created

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
def square(x):
    return x**2


@App('python', dfk)
def increment_one(x):
    return x + 1


@App('python', dfk)
def sum_elements(x=[]):
    total = 0
    for i in range(len(x)):
        total += x[i].result()
    return total


def test_multi_instances(x=5):
    numbers = []
    [numbers.append(increment_one(square(rand().result()).result()))
     for i in range(x)]
    total = sum_elements(numbers)
    print(total.result())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-x", "--int", default="5",
                        action="store", dest="x", type=int)
    args = parser.parse_args()
    test_multi_instances(args.x)
