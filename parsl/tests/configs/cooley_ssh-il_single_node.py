import pytest
from parsl.tests.utils import get_rundir
from parsl.tests.user_opts import user_opts

if 'cooley' not in user_opts:
    pytest.skip('cooley user_opts not configured', allow_module_level=True)
else:
    info = user_opts['cooley']

config = {
    "sites": [{
        "site": "cooley_ssh-il_single_node",
        "auth": {
            "channel": "ssh-il",
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
                "options": {
                    "partition": "debug",
                    "overrides": "source /home/yadunand/setup_cooley_env.sh"
                }
            }
        }
    }
    ],
    "globals": {
        "lazyErrors": True,
        'runDir': get_rundir()
    }
}
