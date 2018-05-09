"""The following config uses threads say for local lightweight apps and IPP workers for
heavy weight applications.

The app decorator has a parameter `sites=[<list of sites>]` to specify the site to which
apps should be directed.
"""
from parsl.tests.utils import get_rundir

config = {
    "sites": [
        {
            "site": "local_threads",
            "auth": {
                "channel": None,
            },
            "execution": {
                "executor": "threads",
                "provider": None,
                "maxThreads": 4
            }
        }, {
            "site": "local_ipp",
            "auth": {
                "channel": None,
            },
            "execution": {
                "executor": "ipp",
                "provider": "local",
                "block": {
                    "nodes": 1,
                    "taskBlocks": 1,
                    "walltime": "00:05:00",
                    "initBlocks": 4,
                }
            }
        }
    ],
    "globals": {
        "lazyErrors": True,
        "runDir": get_rundir()
    }
}
