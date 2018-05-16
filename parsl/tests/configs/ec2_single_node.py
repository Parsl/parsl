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

from parsl.tests.utils import get_rundir
from parsl.tests.user_opts import user_opts

if 'ec2' in user_opts:
    info = user_opts['ec2']
else:
    pytest.skip('ec2 user_opts not configured', allow_module_level=True)

config = {
    "sites": [
        {
            "site": "ec2_single_node",
            "auth": {
                "channel": None,
                "profile": "default",
            },
            "execution": {
                "executor": "ipp",
                "provider": "aws",
                "block": {
                    "nodes": 1,  # number of nodes per block
                    "taskBlocks": 2,  # total tasks in a block
                    "walltime": "01:00:00",
                    "initBlocks": 1,
                    "options": info['options']
                }
            }
        }
    ],
    "globals": {
        "lazyErrors": True,
        "runDir": get_rundir()
    },
    "controller": {
        "publicIp": '*'
    }
}
