import parsl
from parsl import *
import argparse
workers = ThreadPoolExecutor(max_workers = 30)
dfk = DataFlowKernel(workers)

@App('python', dfk)

def fibonacci(n):
    if n == 0:
        return 0
    if n == 2 or n ==1:
        return 1
    else:
        return fibonacci(n - 1).result() + fibonacci(n - 2).result()
    
parser = argparse.ArgumentParser()
parser.add_argument("-a", "--num", action = "store", dest = "a", type = int)
args = parser.parse_args()
x = args.a
results = []
for i in range(x):
    results.append(fibonacci(i))
    
for j in range(len(results)):
        print(results[j].result())
