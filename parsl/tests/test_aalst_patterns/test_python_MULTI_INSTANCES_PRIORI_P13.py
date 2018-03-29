# In this pattern, multiple instances of an activity can be enabled, with the
# number of instances of a given activity known at design time

from parsl import *
import random
import argparse

workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers])


@App('python', dfk)
def rand():
    x = random.randint(1, 10)
    print(x)
    return x


@App('python', dfk)
def square(x):
    return x**2


@App('python', dfk)
def increment_one(x):
    return x + 1


@App('python', dfk)
def sum_elements(x, y, z):
    return x + y + z


def test_multi_instances():
    x = increment_one(square(rand().result()).result()).result()
    y = increment_one(square(rand().result()).result()).result()
    z = increment_one(square(rand().result()).result()).result()
    total = sum_elements(x, y, z).result()
    print(total)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-x", "--int", default="5",
                        action="store", dest="x", type=int)
    args = parser.parse_args()
    test_multi_instances()
