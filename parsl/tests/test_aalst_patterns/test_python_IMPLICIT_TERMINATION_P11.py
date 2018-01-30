# A given process is terminated once there are no longer any active activities
# in the workflow

from parsl import *
import random
import argparse
import time

workers = ThreadPoolExecutor(max_workers=10)
dfk = DataFlowKernel(executors=[workers])


@App('python', dfk)
def rand():
    x = random.randint(1, 10)
    return x


@App('python', dfk)
def square(x):
    return x**2


@App('python', dfk)
def increment(x):
    return x + 1


@App('python', dfk)
def increment_slow(x):
    time.sleep(5)
    return x + 1


@App('python', dfk)
def sum_elements(x, y):
    return x + y


def test_implicit_termination(x=5):
    numbers = []
    numbers.append(increment_slow(rand().result()))
    for i in range(x):
        y = rand().result()
        numbers.append(square(increment(y).result()).result())
    print(numbers[0].result())
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-x", "--int", default="5",
                        action="store", dest="x", type=int)
    args = parser.parse_args()
    test_implicit_termination(args.x)
