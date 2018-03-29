import os

USERNAME = os.environ["THETA_USERNAME"]


multiNode = {
    "sites": [{
        "site": "ALCF_Theta_Local",
        "auth": {
            "channel": "local",
            "scriptDir": "/home/{}/parsl_scripts/".format(USERNAME)
        },
        "execution": {
            "executor": "ipp",
            "provider": "cobalt",
            "scriptDir": "./scripts",
            "block": {      # Definition of a block
                "initBlocks": 1,
                "maxBlocks": 1,  # Limiting to just one block
                "launcher": 'aprun',
                "nodes": 8,            # of nodes in that block
                "taskBlocks": 8,       # total tasks in a block
                "walltime": "00:30:00",
                "options": {
                    "account": "CSC249ADCD01",
                    "partition": "default",
                    "overrides": """export PATH=/home/yadunand/theta_parsl/anaconda3/bin/:$PATH;
source activate /home/yadunand/theta_parsl/anaconda3/envs/theta_parslenv"""
                }
            }
        }
    }
    ],
    "globals": {
        "lazyErrors": True,
        "strategy": None,
    },
    "controller": {
        # Once you log onto theta, get the ip address of the login machine
        # by running >> ip addr show | grep -o 10.236.1.[0-9]*
        "publicIp": '10.236.1.193'
    }
}
