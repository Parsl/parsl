# A set of activities is executed in an arbitrary order. Each activity
# in the set is executed, and no two activities are active at the same
# time

from parsl import *
import random
import argparse
import time

workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers])


@App('python', dfk)
def rand():
    x = random.randint(1, 10)
    print(x)
    return x


@App('python', dfk)
def square(x):
    if x > 5:
        return x**2
    else:
        time.sleep(5)
        return x**2


@App('python', dfk)
def increment(x):
    time.sleep(1)
    return x + 1


def test_unordered_sequence():
    r = rand().result()
    s = square(r)
    i = increment(r)
    print(s.done())
    while s.done() is not True and i.done() is not True:
        pass
    if i.done() is True:
        print(i.result())
        print(square(i.result()).result())
    elif s.done() is True:
        print(s.result())
        print(increment(s.result()).result())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-x", "--input", default="4",
                        action="store", dest="x", type=int)
    args = parser.parse_args()
    test_unordered_sequence()
