#!/usr/bin/env python3

from parsl import App, DataFlowKernel
from parsl.dataflow.futures import Future

config = {
    "sites": [
        {"site": "Local_IPP",
         "auth": {
             "channel": "ssh",
             "hostname": "swift.rcc.uchicago.edu",
             "username": "yadunand",
             "scriptDir": "/scratch/midway/yadunand/parsl_scripts"
         },
         "execution": {
             "executor": "ipp",
             "provider": "slurm",
             "script_dir": ".scripts",
             "block": {                 # Definition of a block
                 "nodes": 1,            # nodes in that block
                 "taskBlocks": 1,       # total tasks in a block

                 "walltime": "00:05:00",

                 "initBlocks": 1,

                 "minBlocks": 0,

                 "maxBlocks": 1,

                 "scriptDir": ".",

                 "options": {

                     "partition": "westmere"

                 }

             }

         }

         }

    ],
    "globals": {"lazyErrors": True},
    "controller": {"publicIp": '*'}
}


dfk = DataFlowKernel(config=config)


@App('bash', dfk)
def sort(unsorted: str,
         outputs: list = [],
         stderr: str='output/p4_c_sort.err',
         stdout: str='output/p4_c_sort.out') -> Future:
    """Call sort executable on file `unsorted`."""
    return "sort -g {} > {}".format(unsorted, outputs[0])


s = sort("p4/unsorted.txt", outputs=["output/sorted_c.txt"])

s.result()


with open('p4/unsorted.txt', 'r') as f:

    print(f.read().replace("\n", ","))


with open('output/sorted_c.txt', 'r') as f:

    print(f.read().replace("\n", ","))
