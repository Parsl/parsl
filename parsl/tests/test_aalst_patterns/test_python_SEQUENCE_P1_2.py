# An activity in a workflow process is enabled after the completion of
# another activity in the same process

from parsl import *
import random
import argparse

workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers])


@App('python', dfk)
def rand():
    x = random.randint(1, 1000)
    return x


@App('python', dfk)
def multiply_rand(x):
    return x * random.randint(1, 10)


def test_sequence(x=5):
    flights = []
    miles = []
    for i in range(x):
        flights.append(rand())
    for j in range(len(flights)):
        miles.append(multiply_rand(flights[j].result()))
    for k in range(len(miles)):
        print(miles[k].result())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--flight", default="5",
                        action="store", dest="a", type=int)
    args = parser.parse_args()
    test_sequence(args.a)
