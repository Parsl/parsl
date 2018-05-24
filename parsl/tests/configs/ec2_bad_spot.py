import pytest

from parsl.tests.utils import get_rundir
from parsl.tests.user_opts import user_opts

if 'ec2' in user_opts:
    info = user_opts['ec2']
else:
    pytest.skip('ec2 user_opts not configured', allow_module_level=True)

info["spotMaxBid"] = 0.001  # Price too low

config = {
    "sites": [
        {
            "site": "ec2_bad_spot",
            "auth": {
                "channel": None,
                "profile": "default"
            },
            "execution": {
                "executor": "ipp",
                "provider": "aws",
                "channel": None,
                "block": {
                    "init_blocks": 1,
                    "max_blocks": 1,
                    "min_blocks": 0,
                    "task_blocks": 1,
                    "nodes": 1,
                    "walltime": "00:25:00",
                    "options": info["options"]
                }
            }
        }
    ],
    "globals": {
        "lazyErrors": True,
        "runDir": get_rundir()
    }
}
