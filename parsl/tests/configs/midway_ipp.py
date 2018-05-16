import pytest
from parsl.tests.utils import get_rundir
from parsl.tests.user_opts import user_opts

if 'midway' in user_opts:
    info = user_opts['midway']
else:
    pytest.skip('midway user_opts not configured {}'.format(str(user_opts)), allow_module_level=True)

config = {
    "sites": [
        {
            "site": "midway_ipp",
            "auth": {
                "channel": "ssh",
                "hostname": "swift.rcc.uchicago.edu",
                "username": info['username'],
                "scriptDir": info['script_dir']
            },
            "execution": {
                "executor": "ipp",
                "provider": "slurm",
                "block": {
                    "nodes": 1,
                    "minBlocks": 1,
                    "maxBlocks": 2,
                    "initBlocks": 1,
                    "taskBlocks": 4,
                    "parallelism": 0.5,
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
