# In this workflow, there is a point in the process where multiple subprocesses/activities
# converge into one single thread of control

from parsl import *
import argparse

workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers])


@App('python', dfk)
def square(x):
    return x**2


@App('python', dfk)
def increment(x):
    return x + 1


@App('python', dfk)
def join(x, y):
    return x + y


def test_join(x=5):
    i = []
    for j in range(x):
        a = increment(j).result()
        b = square(j).result()
        i.append(join(a, b).result())
    total = sum(i)
    print(total)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--num", default="5",
                        action="store", dest="a", type=int)
    args = parser.parse_args()
    test_join(args.a)
