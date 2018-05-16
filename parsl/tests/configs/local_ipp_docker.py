import pytest
import shutil

from parsl.tests.utils import get_rundir

if shutil.which('docker') is None:
    pytest.skip('docker not installed', allow_module_level=True)

config = {
    "sites": [
        {
            "site": "local_ipp_docker",
            "auth": {
                "channel": None
            },
            "execution": {
                "executor": "ipp",
                "container": {
                    "type": "docker",
                    "image": "parslbase_v0.1",
                },
                "provider": "local",
                "block": {
                    "initBlocks": 2,
                },
            }
        }
    ],
    "globals": {
        "lazyErrors": True,
        'runDir': get_rundir()
    }
}
