import getpass
import os

HOME = os.environ["HOME"]
USERNAME = getpass.getuser()

"""
================== Block
| ++++++++++++++ | Node
| |            | |
| |    Task    | |             . . .
| |            | |
| ++++++++++++++ |
==================
"""

singleNode = {
    "sites": [
        {"site": "Local_CC-IN2P3",
         "auth": {
             "channel": "ssh-il",
             "username": os.environ["CC_IN2P3_USERNAME"],
             "scriptDir": "{}/parsl_scripts".format(os.environ["CC_IN2P3_HOME"])
         },
         "execution": {
             "executor": "ipp",
             "provider": "gridEngine",
             "block": {  # Definition of a block
                 "nodes": 1,            # of nodes in that block
                 "taskBlocks": 1,       # total tasks in a block
                 "initBlocks": 1,
                 "maxBlocks": 1,
                 "options": {
                     "partition": "debug",
                     "overrides": """export PATH=/pbs/throng/lsst/software/anaconda/anaconda3-5.0.1/bin:$PATH;
source activate parsl_env_3.5"""
                 }
             }
         }
         }
    ],
    "globals": {"lazyErrors": True}
}

"""
================== Block
| ++++++++++++++ | Node
| |            | |
| |    Task    | |             . . .
| |            | |
| ++++++++++++++ |
==================
"""

singleNodeLocal = {
    "sites": [
        {"site": "Local_CC-IN2P3",
         "auth": {
             "channel": "local",
             "username": USERNAME,
             "scriptDir": "{}/parsl_scripts".format(HOME)
         },
         "execution": {
             "executor": "ipp",
             "provider": "gridEngine",
             "block": {  # Definition of a block
                 "nodes": 1,            # of nodes in that block
                 "taskBlocks": 1,       # total tasks in a block
                 "initBlocks": 1,
                 "maxBlocks": 1,
                 "options": {
                     "partition": "debug",
                     "overrides": """export PATH=/pbs/throng/lsst/software/anaconda/anaconda3-5.0.1/bin:$PATH;
source activate parsl_env_3.5"""
                 }
             }
         }
         }
    ],
    "globals": {"lazyErrors": True}
}

"""
                      Block {Min:0, init:1, Max:1}
========================================================================
| ++++++++++++++ || ++++++++++++++ || ++++++++++++++ || ++++++++++++++ |
| |    Node    | || |    Node    | || |    Node    | || |    Node    | |
| |            | || |            | || |            | || |            | |
| | Task  Task | || | Task  Task | || | Task  Task | || | Task  Task | |
| |            | || |            | || |            | || |            | |
| ++++++++++++++ || ++++++++++++++ || ++++++++++++++ || ++++++++++++++ |
========================================================================

"""
multiNodeLocal = {
    "sites": [
        {"site": "Local_CC-IN2P3",
         "auth": {
             "channel": "local",
             "username": USERNAME,
             "scriptDir": "{}/parsl_scripts".format(HOME)
         },
         "execution": {
             "executor": "ipp",
             "provider": "gridEngine",
             "block": {  # Definition of a block
                 "taskBlocks": 2,       # total tasks in a block
                 "initBlocks": 1,
                 "maxBlocks": 4,
                 "options": {
                     "partition": "debug",
                     "overrides": """export PATH=/pbs/throng/lsst/software/anaconda/anaconda3-5.0.1/bin:$PATH;
source activate parsl_env_3.5"""
                 }
             }
         }
         }
    ],
    "globals": {"lazyErrors": True}
}
