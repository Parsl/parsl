import pytest
from parsl.tests.runtime import runtime

if 'osg' not in runtime:
    pytest.skip('osg runtime not configured')
else:
    info = runtime['osg']

config = {
    "sites": [
        {
            "site": "OSG_local_IPP",
            "auth": {
                "channel": "local",
                "username": info['username'],
                "scriptDir": info['script_dir']
            },
            "execution": {
                "executor": "ipp",
                "provider": "condor",
                "block": {
                    "nodes": 1,  # of nodes in block
                    "taskBlocks": 1,  # total tasks in a block
                    "initBlocks": 4,
                    "maxBlocks": 1,
                    "options": {
                        "partition":
                        "debug",
                        # The following override is used to specify condor class-ads
                        # to ensure that we only get machines with cvmfs, modules and
                        # consequently python3
                        "overrides":
                        'Requirements = OSGVO_OS_STRING == "RHEL 6" && Arch == "X86_64" &&  HAS_MODULES == True',
                        # Worker setup is used to specify instructions to load the
                        # appropriate env on the worker nodes.
                        "workerSetup":
                        """module load python/3.5.2; python3 -m venv parsl_env; source parsl_env/bin/activate; pip3 install ipyparallel"""
                    }
                }
            }
        }
    ],
    "controller": {
        "publicIp": '192.170.227.195'
    },
    "globals": {
        "lazyErrors": True
    }
}
