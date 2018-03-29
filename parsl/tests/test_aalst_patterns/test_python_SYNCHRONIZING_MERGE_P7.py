# A point in the workflow process where multiple paths converge into one single thread with
# synchronization

from parsl import *
import argparse

workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers])


@App('python', dfk)
def add(x, y):
    return x + y


@App('python', dfk)
def subtract(x, y):
    return y - x


@App('python', dfk)
def square(x, y):
    return (x + y)**2


@App('python', dfk)
def double(x, y):
    return 2 * (x + y)


@App('python', dfk)
def sum_elements(n=[]):
    s = 0
    for i in range(len(n)):
        s += n[i]
    return s


def test_or_split(x=4, y=5):
    num = []
    if x < 5:
        num.append(add(x, y).result())
    if y > 7:
        num.append(subtract(x, y).result())
    if x >= 5:
        num.append(square(x, y).result())
    if y <= 7:
        num.append(double(x, y).result())
    print(sum_elements(num).result())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-x", "--num1", default="4",
                        action="store", dest="x", type=int)
    parser.add_argument("-y", "--num2", default="5",
                        action="store", dest="y", type=int)
    args = parser.parse_args()
    test_or_split(args.x, args.y)
