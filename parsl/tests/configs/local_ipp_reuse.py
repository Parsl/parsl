""" Use the following config with caution.
"""
import pytest
from parsl.tests.utils import get_rundir

pytest.skip('Should this be being tested?', allow_module_level=True)

config = {
    "sites": [
        {
            "site": "local_ipp_reuse",
            "auth": {
                "channel": None,
            },
            "execution": {
                "executor": "ipp",
                "provider": "local",
                "script_dir": ".scripts",
                "block": {
                    "nodes": 1,
                    "taskBlocks": 1,
                    "initBlocks": 1,
                    "maxBlocks": 1,
                }
            }
        }
    ],
    "globals": {
        "lazyErrors": True,
        'runDir': get_rundir()
    },
    "controller": {
        "reuse": True
    }
}
