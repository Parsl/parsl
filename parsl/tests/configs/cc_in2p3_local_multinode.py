"""
                      Block {Min:0, init:1, Max:1}
========================================================================
| ++++++++++++++ || ++++++++++++++ || ++++++++++++++ || ++++++++++++++ |
| |    Node    | || |    Node    | || |    Node    | || |    Node    | |
| |            | || |            | || |            | || |            | |
| | Task  Task | || | Task  Task | || | Task  Task | || | Task  Task | |
| |            | || |            | || |            | || |            | |
| ++++++++++++++ || ++++++++++++++ || ++++++++++++++ || ++++++++++++++ |
========================================================================

"""
import pytest
from parsl.tests.utils import get_rundir
from parsl.tests.user_opts import user_opts

if 'cc_in2p3' in user_opts:
    info = user_opts['cc_in2p3']
else:
    pytest.skip('cc_in2p3 user_opts not configured', allow_module_level=True)

config = {
    "sites": [
        {
            "site": "cc_in2p3_local_multinode",
            "auth": {
                "channel": "local",
                "username": info['username'],
                "scriptDir": info['script_dir']
            },
            "execution": {
                "executor": "ipp",
                "provider": "gridEngine",
                "block": {
                    "taskBlocks": 2,
                    "initBlocks": 1,
                    "maxBlocks": 4,
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
