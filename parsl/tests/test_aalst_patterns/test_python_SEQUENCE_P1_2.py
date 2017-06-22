import parsl
from parsl import *
import random 
import argparse

workers = ThreadPoolExecutor(max_workers = 4)
dfk = DataFlowKernel(workers)

@App('python', dfk)
def book_flight():
    x = random.randint(1, 1000)
    return x

@App('python', dfk)
def add_air_miles(x):
    return x * random.randint(1, 10)

def test_air_sequence(x = 5):
    flights = []
    miles = []
    for i in range(x):
        flights.append(book_flight())
    for j in range(len(flights)):
        miles.append(add_air_miles(flights[j].result()))
    for k in range(len(miles)):
        print(miles[k].result())

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--flight", default = "5", action = "store", dest = "a", type = int)
    args = parser.parse_args()
    test_air_sequence(args.a)
    
                   
    
    
