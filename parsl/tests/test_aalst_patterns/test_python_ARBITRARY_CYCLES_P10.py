# A point in a workflow process where one or more activities can
# be repeated 

import parsl
from parsl import *
import random 
import argparse

workers = ThreadPoolExecutor(max_workers = 10)
dfk = DataFlowKernel(workers)

@App('python', dfk)
def rand():
    x = random.randint(2, 10)
    print(x)
    print("random")
    return square(x).result()

@App('python', dfk)
def square(x):
    y = x**2
    if y > 25: 
        return increment(y).result()
    else:
        return cubed(x).result()

@App('python', dfk)
def cubed(x):
    y = x**3
    return square(y).result()

@App('python', dfk)
def increment(x):
    return x + 1

def test_arbitrary(x = 2):
    numbers = []
    for i in range(x):
        numbers.append(rand())
    for i in range(x):
        print(numbers[i].result())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-x", "--num", default = "2", action = "store", dest = "x", type = int)
    args = parser.parse_args()
    test_arbitrary(args.x)
