import parsl
from parsl import *
#from nose.tools import nottest
import os
import time
import shutil
import argparse
from parsl.execution_provider.slurm.slurm import Slurm
from parsl.execution_provider.provider_factory import ExecProviderFactory
#parsl.set_stream_logger()

'''
================== Block
| ++++++++++++++ | Node
| |            | |
| |    Task    | |             . . .
| |            | |
| ++++++++++++++ |
==================
'''
config = {  "site" : "midway_westmere",
            "execution" :
            {  "executor" : "ipp",
               "provider" : "slurm",
               "channel"  : "local",
               "options" :
               {"init_parallelism" : 2,
                "max_parallelism" : 2,
                "min_parallelism" : 0,
                "tasks_per_node"  : 1,
                "nodes_granularity" : 1,
                "partition" : "westmere",
                "walltime" : "00:05:00",
                "account" : "pi-wilde",
                "submit_script_dir" : ".scripts"
                }
            }}

epf = ExecProviderFactory()
resource = epf.make(config)
dfk = DataFlowKernel(resource)


@App('bash', dfk)
def echo_stuff(stdout='std.out', stderr='std.err'):
    cmd_line = '''echo "hostame :$HOSTNAME"
    echo "date:", $(date)
    '''

@App('python', dfk)
def pi(total):
    import random      # App functions have to import modules they will use.     
    width = 10000      # Set the size of the box in which we drop random points
    center = width/2
    c2  = center**2
    count = 0
    for i in range(total):
        # Drop a random point in the box.
        x,y = random.randint(1, width),random.randint(1, width)
        # Count points within the circle
        if (x-center)**2 + (y-center)**2 < c2:
            count += 1
    return (count*4/total)

@App('python', dfk)
def avg_three(a,b,c):
    return (a+b+c)/3

if __name__ == "__main__" :

    if os.path.exists("./outputs"):
        shutil.rmtree("./outputs")
        os.makedirs("./outputs")

    # Run some comptutations
    fus = []
    for i in range(1,10):
        f = echo_stuff(stdout="outputs/{0}.bash.out".format(i),
                       stderr="outputs/{0}.bash.err".format(i))

    print([fu.result() for fu in fus])

    # Do the pi ()
    a, b, c = pi(10**6), pi(10**6), pi(10**6)
    
    x = avg_three(a,b,c)
    
    print("Got pi : ", x.result())
