"""
                      Block {Min:0, init:1, Max:1}
========================================================================
| ++++++++++++++ || ++++++++++++++ || ++++++++++++++ || ++++++++++++++ |
| |    Node    | || |    Node    | || |    Node    | || |    Node    | |
| | +--------------------------+ | || | +--------------------------+ | |
| | |         MPI Task         | | || | |         MPI Task         | | |
| | +--------------------------+ | || | +--------------------------+ | |
| ++++++++++++++ || ++++++++++++++ || ++++++++++++++ || ++++++++++++++ |
========================================================================

"""
import pytest
from parsl.tests.utils import get_rundir
from parsl.tests.user_opts import user_opts

if 'beagle' in user_opts:
    info = user_opts['beagle']
else:
    pytest.skip('beagle user_opts not configured', allow_module_level=True)

config = {
    "sites": [
        {
            "site": "beagle_multinode_MPI",
            "auth": {
                "channel": "ssh",
                "hostname": "beagle.nersc.gov",
                "username": info['username'],
                "scriptDir": info['script_dir']
            },
            "execution": {
                "executor": "ipp",
                "provider": "slurm",
                "block": {
                    "launcher": "srun",
                    "nodes": 4,
                    "taskBlocks": 2,
                    "walltime": "00:10:00",
                    "initBlocks": 1,
                    "maxBlocks": 1,
                    'options': info['options']
                }
            }
        }
    ],
    "globals": {
        "lazyErrors": True,
        'runDir': get_rundir()
    }
}
