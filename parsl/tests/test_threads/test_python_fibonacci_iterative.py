
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
<<<<<<< HEAD
num = 5
counter = 0
if __name__ == '__main__':   
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--num", default = "5", action = "store", dest = "a", type = int)
    args = parser.parse_args()
    num = args.a
=======
counter = 0
parser = argparse.ArgumentParser()
parser.add_argument("-a", "--num", action = "store", dest = "a", type = int)
args = parser.parse_args()
num = args.a
>>>>>>> e9736a6ed54a86e55af52d1ee1d58588f69622ba
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
        
    

