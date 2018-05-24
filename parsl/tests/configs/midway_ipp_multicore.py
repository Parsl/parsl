import pytest
from parsl.tests.utils import get_rundir
from parsl.tests.user_opts import user_opts

if 'midway' in user_opts:
    info = user_opts['midway']
else:
    pytest.skip('midway user_opts not configured', allow_module_level=True)

config = {
    "sites": [
        {
            "site": "midway_ipp_multicore",
            "auth": {
                "channel": "ssh",
                "hostname": "swift.rcc.uchicago.edu",
                "username": info['username'],
                "script_dir": "/scratch/midway2/{0}/parsl_scripts".format(info['username'])
            },
            "execution": {
                "executor": "ipp",
                "provider": "slurm",
                "block": {
                    "nodes": 1,
                    "task_blocks": "$CORES",
                    "walltime": "00:05:00",
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
