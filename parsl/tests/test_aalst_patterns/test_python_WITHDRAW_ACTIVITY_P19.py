# A point in the workflow process where an enabled activity is disabled, or
# a thread waiting for execution is removed

from parsl import *
import random
import argparse
import time

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
    if x < 5:
        time.sleep(7)
        return x**3
    else:
        return x**3


@App('python', dfk)
def sum_elements(x=[], y=[], z=[]):
    total = 0
    for i in range(len(x)):
        total += x[i].result()
    for j in range(len(y)):
        if y[j] is not None:
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
        if cubes[i].done() is True:
            print(True)
            squares[i] = None
        else:
            print(False)
        increments.append(increment(r))
    print(sum_elements(cubes, squares, increments).result())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-x", "--input", default="3",
                        action="store", dest="x", type=int)
    args = parser.parse_args()
    test_withdraw(args.x)
