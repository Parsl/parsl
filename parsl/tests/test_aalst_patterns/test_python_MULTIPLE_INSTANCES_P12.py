# In this workflow pattern, multiple instances of an activity can
# be created. Each time a new instance is created, a new thread is created that
# is independent of all the other threads

from parsl import *
import random
import argparse

workers = ThreadPoolExecutor(max_workers=10)
dfk = DataFlowKernel(executors=[workers])


@App('python', dfk)
def rand():
    x = random.randint(1, 10)
    return x


@App('python', dfk)
def square(x):
    return x**2


def test_multi_instances(x=5):
    numbers = []
    for i in range(x):
        numbers.append(square(rand().result()))
    print(numbers[i])
    print([numbers[i].result() for i in range(len(numbers))])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-x", "--num", default="5",
                        action="store", dest="x", type=int)
    args = parser.parse_args()
    test_multi_instances(args.x)
