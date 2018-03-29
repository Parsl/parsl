# A point in the workflow process where when one of several branches is
# activated, the other branches are withdrawn

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


def test_deferred_choice():
    r = rand().result()
    s = square(r)
    i = increment(r)
    print(s.done())
    while s.done() is not True and i.done() is not True:
        pass
    if s.done() is True:
        print(s.result())
    elif i.done() is True:
        print(i.result())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-x", "--input", default="4",
                        action="store", dest="x", type=int)
    args = parser.parse_args()
    test_deferred_choice()
