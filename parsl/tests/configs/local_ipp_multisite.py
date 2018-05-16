"""The following config uses two IPP sites designed for python apps which may
not show any performance improvements on local threads. This also allows you to
send work to two separate remote sites, or to two separate partitions.
"""
from parsl.tests.utils import get_rundir

config = {
    "sites": [
        {"site": "local_ipp_1",
         "auth": {"channel": None},
         "execution": {
             "executor": "ipp",
             "provider": "local",
             "block": {
                 "nodes": 1,
                 "taskBlocks": 1,
                 "walltime": "00:15:00",
                 "initBlocks": 4,
             }
         }
         },
        {"site": "local_ipp_2",
         "auth": {"channel": None},
         "execution": {
             "executor": "ipp",
             "provider": "local",
             "block": {
                 "nodes": 1,
                 "taskBlocks": 1,
                 "walltime": "00:15:00",
                 "initBlocks": 2,
             }
         }
         }],
    "globals": {
        "lazyErrors": True,
        'runDir': get_rundir()
    }
}
