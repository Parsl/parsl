import os

USERNAME = os.environ["COOLEY_USERNAME"]

singleNodeLocal = {
    "sites": [{
        "site": "ALCF_Cooley_Remote",
        "auth": {
            "channel": "local",
            "hostname": "cooleylogin1.alcf.anl.gov",
            "username": USERNAME,
            "scriptDir": "/home/{}/parsl_scripts/".format(USERNAME)
        },
        "execution": {
            "executor": "ipp",
            "provider": "cobalt",
            "block": {      # Definition of a block
                "nodes": 1,            # of nodes in that block
                "taskBlocks": 1,       # total tasks in a block
                "walltime": "00:05:00",
                "initBlocks": 1,
                "maxBlocks": 1,
                "options": {
                    "partition": "debug",
                    "account": 'CSC249ADCD01',
                    "overrides": "source /home/yadunand/setup_cooley_env.sh"
                }
            }
        }
    }
    ],
    "controller": {"publicIp": "10.230.100.209"},
    "globals": {"lazyErrors": True}

}


singleNode = {
    "sites": [{
        "site": "ALCF_Cooley_Remote",
        "auth": {
            "channel": "ssh-il",
            "hostname": "cooleylogin1.alcf.anl.gov",
            "username": USERNAME,
            "scriptDir": "/home/{}/parsl_scripts/".format(USERNAME)
        },
        "execution": {
            "executor": "ipp",
            "provider": "cobalt",
            "block": {      # Definition of a block
                "nodes": 1,            # of nodes in that block
                "taskBlocks": 1,       # total tasks in a block
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
        "lazyErrors": True
    }
}

multiNode = {
    "sites": [{
        "site": "ALCF_Cooley_Remote",
        "auth": {
            "channel": "ssh-il",
            "hostname": "cooleylogin1.alcf.anl.gov",
            "username": USERNAME,
            "scriptDir": "/home/{}/parsl_scripts/".format(USERNAME)
        },
        "execution": {
            "executor": "ipp",
            "provider": "cobalt",  # LIKELY SHOULD BE BOUND TO SITE
            "block": {  # Definition of a block
                "nodes": 1,            # of nodes in that block
                "taskBlocks": 1,       # total tasks in a block
                "initBlocks": 2,
                "maxBlocks": 2,
                "options": {
                    "partition": "debug",
                    "overrides": "source /home/yadunand/setup_cooley_env.sh"
                }
            }
        }
    }
    ],
    "globals": {
        "lazyErrors": True
    }
}
