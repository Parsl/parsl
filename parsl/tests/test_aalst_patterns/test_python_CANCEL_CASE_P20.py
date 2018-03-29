# A point in a workflow process where an instance of an activity is removed completely

from parsl import *
import random
import argparse

workers = ThreadPoolExecutor(max_workers=10)
dfk = DataFlowKernel(executors=[workers])


@App('python', dfk)
def rand():
    x = random.randint(1, 10)
    print(x)
    return x


@App('python', dfk)
def square(x):
    z = x**2
    return z


@App('python', dfk)
def increment(x):
    return x + 1


@App('python', dfk)
def cubed(x):
    return x ** 3


@App('python', dfk)
def sum_elements(x=[], y=[], z=[]):
    total = 0
    for i in range(len(x)):
        total += x[i].result()
    for j in range(len(y)):
        total += y[j].result()
    for k in range(len(z)):
        total += z[k].result()
    return total


def test_withdraw(x=3):
    cubes = []
    squares = []
    increments = []
    for i in range(x):
        r = rand().result()
        cubes.append(cubed(r))
        squares.append(square(r))
        increments.append(increment(r))
    r = random.randint(1, 30)
    print(r)
    if r > 20:
        del cubes[:]
    if r < 20 and r > 10:
        del squares[:]
    if r < 10:
        del increments[:]
    print(sum_elements(cubes, squares, increments).result())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-x", "--input", default="3",
                        action="store", dest="x", type=int)
    args = parser.parse_args()
    test_withdraw(args.x)
