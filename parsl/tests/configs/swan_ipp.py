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
    pytest.skip('swan user_opts not configured', allow_module_level=True)
else:
    info = user_opts['swan']

config = {
    "sites": [
        {
            "site": "swan_ipp",
            "auth": {
                "channel": "ssh",
                "hostname": "swan.cray.com",
                "username": info['username'],
                "script_dir": "/home/users/{}/parsl_scripts".format(info['username'])
            },
            "execution": {
                "executor": "ipp",
                "provider": "torque",
                "block": {
                    "nodes": 1,
                    "launcher": 'aprun',
                    "task_blocks": 1,
                    "init_blocks": 1,
                    "max_blocks": 1,
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
