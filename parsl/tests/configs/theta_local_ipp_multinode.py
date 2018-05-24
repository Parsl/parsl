import pytest
from parsl.tests.utils import get_rundir
from parsl.tests.user_opts import user_opts

if 'theta' not in user_opts:
    pytest.skip('theta user_opts not configured', allow_module_level=True)
else:
    info = user_opts['theta']

config = {
    "sites": [
        {
            "site": "theta_local_ipp_multinode",
            "auth": {
                "channel": "local",
                "script_dir": info['script_dir']
            },
            "execution": {
                "executor": "ipp",
                "provider": "cobalt",
                "script_dir": "./scripts",
                "block": {
                    "init_blocks": 1,
                    "max_blocks": 1,  # Limiting to just one block
                    "launcher": 'aprun',
                    "nodes": 8,  # of nodes in that block
                    "task_blocks": 8,  # total tasks in a block
                    "walltime": "00:30:00",
                    "options": info['options']
                }
            }
        }
    ],
    "globals": {
        "lazyErrors": True,
        "strategy": None,
        "runDir": get_rundir()
    },
    "controller": {
        "publicIp": info['public_ip']
    }
}
