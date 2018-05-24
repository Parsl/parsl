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

if 'cori' in user_opts:
    info = user_opts['cori']
else:
    pytest.skip('cori user_opts not configured', allow_module_level=True)

config = {
    "sites": [
        {
            "site": "cori_local_ipp_single_node",
            "auth": {
                "channel": "local",
                "hostname": "cori.nersc.gov",
                "username": info['username'],
                "script_dir": info['script_dir']
            },
            "execution": {
                "executor": "ipp",
                "provider": "slurm",
                "block": {
                    "nodes": 1,
                    "task_blocks": 1,
                    "init_blocks": 1,
                    "max_blocks": 1,
                    "options": info['options']
                }
            }
        }
    ],
    "globals": {
        "lazyErrors": True,
        "runDir": get_rundir()
    }
}
