# A point in a process where two or more branches reconverge without synchronization

from parsl import *
import random
import argparse

workers = ThreadPoolExecutor(max_workers=20)
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
def cubed(x):
    return x**3


@App('python', dfk)
def increment(x):
    return x + 1


@App('python', dfk)
def add_two(x):
    return x + 2


@App('python', dfk)
def sum_elements(x, y):
    return x + y


def test_multi_merge():
    r = rand().result()
    num1 = cubed(add_two(square(r).result()).result())
    num2 = cubed(add_two(increment(r).result()).result())
    print(sum_elements(num1.result(), num2.result()).result())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-x", "--num1", default="4",
                        action="store", dest="x", type=int)
    args = parser.parse_args()
    test_multi_merge()
