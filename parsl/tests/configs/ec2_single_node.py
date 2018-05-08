"""Config for EC2.

Block {Min:0, init:1, Max:1}
==================
| ++++++++++++++ |
| |    Node    | |
| |            | |
| | Task  Task | |
| |            | |
| ++++++++++++++ |
==================

"""
import pytest

from parsl.tests.runtime import runtime

if 'ec2' not in runtime:
    pytest.skip('ec2 runtime not configured')
else:
    info = runtime['ec2']

config = {
    "sites": [
        {"site": "ec2_single_node",
         "auth": {
             "channel": None,
             "profile": "default",
         },
         "execution": {
             "executor": "ipp",
             "provider": "aws",
             "block": {
                 "nodes": 1, # number of nodes per block
                 "taskBlocks": 2, # total tasks in a block
                 "walltime": "01:00:00",
                 "initBlocks": 1,
                 "options": info['options']
             }
         }
         }
    ],
    "globals": {"lazyErrors": True},
    "controller": {"publicIp": '*'}
}
