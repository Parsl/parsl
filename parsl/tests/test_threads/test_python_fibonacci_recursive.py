from parsl import *
import argparse

workers = ThreadPoolExecutor(max_workers=5)
dfk = DataFlowKernel(executors=[workers])


@App('python', dfk)
def fibonacci(n):
    if n == 0:
        return 0
    if n == 2 or n == 1:
        return 1
    else:
        return fibonacci(n - 1).result() + fibonacci(n - 2).result()


def test_fibonacci(x=5):
    results = []
    for i in range(x):
        results.append(fibonacci(i))
    for j in range(len(results)):
        print(results[j].result())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--num", default='5',
                        action="store", dest="a", type=int)
    args = parser.parse_args()
    test_fibonacci(args.a)
