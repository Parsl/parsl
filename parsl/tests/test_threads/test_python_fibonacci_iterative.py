
import parsl
from parsl import *
import argparse

workers = ThreadPoolExecutor(max_workers = 6)
dfk = DataFlowKernel(workers)

@App('python', dfk)
def get_num(first, second):
    return first + second 

x1 = 0
x2 = 1
counter = 0
parser = argparse.ArgumentParser()
parser.add_argument("-a", "--num", action = "store", dest = "a", type = int)
args = parser.parse_args()
num = args.a
results = [ ]
results.append(0)
results.append(1)
while counter < num - 2:
    counter += 1 
    results.append(get_num(x1, x2))
    temp = x2
    x2 = get_num(x1, x2)
    x1 = temp
for i in range(len(results)):
    if type(results[i]) is int:
        print(results[i])
    else:
        print(results[i].result())
        
    

