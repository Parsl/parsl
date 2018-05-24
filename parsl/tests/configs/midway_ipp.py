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
                "script_dir": info['script_dir']
            },
            "execution": {
                "executor": "ipp",
                "provider": "slurm",
                "block": {
                    "nodes": 1,
                    "min_blocks": 1,
                    "max_blocks": 2,
                    "init_blocks": 1,
                    "task_blocks": 4,
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
