# A point in the workflow were two or more separate branches, that are not
# being executed in parallel,  come together into one branch

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
def cubed(x):
    return x**3


@App('python', dfk)
def join(x, y, z):
    return x + y + z


def test_join(x=2):
    print(join(square(x).result(), increment(
        x).result(), cubed(x).result()).result())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--num", default="2",
                        action="store", dest="a", type=int)
    args = parser.parse_args()
    test_join(args.a)
