import parsl
from parsl import *
#from nose.tools import nottest
import os
import time
import shutil
import argparse
from parsl.execution_provider.midway.slurm import Midway

parsl.set_stream_logger()

pool1_config = {"site"        : "pool1",
                "queue"       : "bigmem",
                "walltime"    : "00:10:00",
                "min_engines" : 4,
                "max_engines" : 4,
                "engines_per_node" : 1,
                "nodes_per_job" : 1,
                "account" : 'pi-wilde' # Replace with your account
}


pool1 = IPyParallelExecutor(Midway(pool1_config))
print(pool1)

#pool1 = IPyParallelExecutor() #Midway(pool1_config))
dfk = DataFlowKernel(pool1)

@App('python', pool1, walltime=1)
def double(x):
    return x*2


if __name__ == "__main__":

    jobs = []
    for i in range(0,10):
        jobs.extend([double(i)])


    print([job.result() for job in jobs])
