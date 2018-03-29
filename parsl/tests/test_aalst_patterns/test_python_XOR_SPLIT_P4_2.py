# A point in the workflow process where one of several branches is chosen baced on a decision or
# workflow control data

from parsl import *
import argparse
import random

workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers])


@App('python', dfk)
def rand():
    x = random.randint(1, 10)
    return x


@App('python', dfk)
def eval_number(x):
    if x > 5:
        print("Larger than 5")
        return return_one().result()
    else:
        print("Less than or equal to 5")
        return return_zero().result()


@App('python', dfk)
def return_one():
    return 1


@App('python', dfk)
def return_zero():
    return 0


def test_XOR_split(x=4):
    num = []
    for i in range(x):
        num.append(eval_number(rand().result()))
    for i in range(len(num)):
        print(num[i].result())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--a", "--number", default="4",
                        action="store", dest="a", type=int)
    args = parser.parse_args()
    test_XOR_split(args.a)
