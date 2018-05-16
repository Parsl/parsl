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

if 'swan' not in user_opts:
    info = user_opts['swan']
else:
    pytest.skip('swan user_opts not configured', allow_module_level=True)

config = {
    "sites": [
        {
            "site": "swan_ipp",
            "auth": {
                "channel": "ssh",
                "hostname": "swan.cray.com",
                "username": info['username'],
                "scriptDir": "/home/users/{}/parsl_scripts".format(info['username'])
            },
            "execution": {
                "executor": "ipp",
                "provider": "torque",
                "block": {
                    "nodes": 1,
                    "launcher": 'aprun',
                    "taskBlocks": 1,
                    "initBlocks": 1,
                    "maxBlocks": 1,
                    'options': info['options']
                }
            }
        }
    ],
    "globals": {
        "lazyErrors": True,
        "runDir": get_rundir()
    }
}
