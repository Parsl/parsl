"""
================== Block
| ++++++++++++++ | Node
| |            | |
| |    Task    | |             . . .
| |            | |
| ++++++++++++++ |
==================
"""
import pytest
from parsl.tests.utils import get_rundir
from parsl.tests.user_opts import user_opts

if 'beagle' in user_opts:
    info = user_opts['beagle']
else:
    pytest.skip('beagle user_opts not configured', allow_module_level=True)

singleNode = {
    "sites": [
        {
            "site": "beagle_single_node",
            "auth": {
                "channel": "ssh",
                "hostname": "login4.beagle.ci.uchicago.edu",
                "username": info['username'],
                "scriptDir": "/lustre/beagle2/{}/parsl_scripts".format(info['username'])
            },
            "execution": {
                "executor": "ipp",
                "provider": "torque",
                "block": {
                    "nodes": 1,  # number of nodes in a block
                    "launcher": 'aprun',
                    "taskBlocks": 1,  # total tasks in a block
                    "initBlocks": 1,
                    "maxBlocks": 1,
                    "options": info['options']
                }
            }
        }
    ],
    "globals": {
        "lazyErrors": True,
        'runDir': get_rundir()
    }
}
