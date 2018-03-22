import os
import sys

import parsl
from parsl import *
from parsl.data_provider.data_manager import DataManager
from parsl.data_provider.files import File

config = {
    "sites": [
        {
            "site": "Local_Threads",
            "auth": {
                "channel": None
            },
            "execution": {
                "executor": "threads",
                "provider": None,
                "maxThreads": 4
            },
            "data": {
                "globus": {
                    "endpoint_name": "1afdfb30-1102-11e8-a7ed-0a448319c2f8",
                    "endpoint_path": "/",
                    "local_directory": "/home/lukasz/projects/parsl/share"
                }
            }
        }
    ],
    "globals": {
        "lazyErrors": True
    }
}

dfk = DataFlowKernel(config=config)


@App('python', dfk)
def sort_strings(inputs=[], outputs=[]):
    with open(inputs[0], 'r') as u:
        strs = u.readlines()
        strs.sort()
        with open(outputs[0].filepath, 'w') as s:
            for e in strs:
                s.write(e)

'''
Create a remote input file that points to unsorted.txt on a publicly shared
endpoint.
'''
unsorted_file = File('globus://037f054a-15cf-11e8-b611-0ac6873fc732/unsorted.txt')

'''
Create a remote output file that points to sorted.txt on the go#ep1 Globus
endpoint.
'''
sorted_file = File('globus://ddb59aef-6d04-11e5-ba46-22000b92c6ec/~/sorted.txt')

dfu = unsorted_file.stage_in()
dfu.result()

f = sort_strings(inputs=[dfu], outputs=[sorted_file])
f.result()

fs = sorted_file.stage_out()
fs.result()
