# A point in the workflow process where a single thread of control splits into multiple
# threads of control which can be executed in parallel, this allows calls to be executed either
# simultaneously or in any order

from parsl import *
import random
import argparse

workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers])


@App('python', dfk)
def rand():
    return random.randint(1, 10)


@App('python', dfk)
def square(x):
    return x**2


@App('python', dfk)
def cubed(x):
    return x**3


def test_parallel_split(x=4):
    for i in range(x):
        num = rand().result()
        y = square(num).result()
        z = cubed(num).result()
        print(y)
        print(z)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--input", default="4",
                        action="store", dest="a", type=int)
    args = parser.parse_args()
    test_parallel_split(args.a)
