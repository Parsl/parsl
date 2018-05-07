import pytest
from parsl.tests.runtime import runtime

if 'midway' not in runtime:
    pytest.skip('midway runtime not configured')
else:
    info = runtime['midway']

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
    ]
}
