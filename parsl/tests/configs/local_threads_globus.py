import pytest
from parsl.tests.utils import get_rundir
from parsl.tests.user_opts import user_opts

if 'globus' in user_opts:
    info = user_opts['globus']
else:
    pytest.skip('globus user_opts not configured', allow_module_level=True)

config = {
    "sites": [
        {
            "site": "local_threads_globus",
            "auth": {
                "channel": None
            },
            "execution": {
                "executor": "threads",
                "provider": None,
                "maxThreads": 4
            },
            "data": {
                "globus": {
                    "endpoint_name": user_opts['globus']['endpoint'],
                    "endpoint_path": user_opts['globus']['path']
                },
                "working_dir": user_opts['globus']['path']
            }
        }
    ],
    "globals": {
        "lazyErrors": True,
        "runDir": get_rundir()
    }
}
