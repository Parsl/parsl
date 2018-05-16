import pytest
from parsl.tests.utils import get_rundir
from parsl.tests.user_opts import user_opts

if 'cooley' in user_opts:
    info = user_opts['cooley']
else:
    pytest.skip('cooley user_opts not configured', allow_module_level=True)

config = {
    "sites": [
        {
            "site": "cooley_local_single_node",
            "auth": {
                "channel": "local",
                "hostname": "cooleylogin1.alcf.anl.gov",
                "username": info['username'],
                "scriptDir": "/home/{}/parsl_scripts/".format(info['username'])
            },
            "execution": {
                "executor": "ipp",
                "provider": "cobalt",
                "block": {
                    "nodes": 1,
                    "taskBlocks": 1,
                    "walltime": "00:05:00",
                    "initBlocks": 1,
                    "maxBlocks": 1,
                    "options": info['options']
                }
            }
        }
    ],
    "controller": {
        "publicIp": "10.230.100.209"
    },
    "globals": {
        "lazyErrors": True,
        'runDir': get_rundir()
    }
}
