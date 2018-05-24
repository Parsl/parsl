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
                "script_dir": "/lustre/beagle2/{}/parsl_scripts".format(info['username'])
            },
            "execution": {
                "executor": "ipp",
                "provider": "torque",
                "block": {
                    "nodes": 1,  # number of nodes in a block
                    "launcher": 'aprun',
                    "task_blocks": 1,  # total tasks in a block
                    "init_blocks": 1,
                    "max_blocks": 1,
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
