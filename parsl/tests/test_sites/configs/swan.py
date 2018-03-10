import os

USERNAME = os.environ['SWAN_USERNAME']
""" SWAN NOTES:

Env:
swan has a module cray-python/3.6.1.1 which can be used readily to setup and virtual
env :

module load cray-python/3.6.1.1
python3 -m venv parsl_env
source parsl_env/bin/activate
pip install parsl

"""

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
        {"site": "Remote_IPP",
         "auth": {
             "channel": "ssh",
             "hostname": "swan.cray.com",
             "username": USERNAME,
             "scriptDir": "/home/users/{}/parsl_scripts".format(USERNAME)
         },
         "execution": {
             "executor": "ipp",
             "provider": "torque",
             "block": {  # Definition of a block
                 "nodes": 1,            # of nodes in that block
                 "launcher": 'aprun',
                 "taskBlocks": 1,       # total tasks in a block
                 "initBlocks": 1,
                 "maxBlocks": 1,
                 "options": {
                     "partition": "debug",
                     "overrides": """module load cray-python/3.6.1.1
source /home/users/{}/parsl_env/bin/activate
""".format(USERNAME)
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
multiNodeSrun = {
    "sites": [
        {"site": "Local_IPP",
         "auth": {
             "channel": "ssh",
             "hostname": "beagle.nersc.gov",
             "username": USERNAME,
             "scriptDir": "/global/homes/y/{}/parsl_scripts".format(USERNAME),
         },
         "execution": {
             "executor": "ipp",
             "provider": "slurm",  # LIKELY SHOULD BE BOUND TO SITE
             "block": {  # Definition of a block
                 "launcher": "srun",
                 "nodes": 4,            # of nodes in that block
                 "taskBlocks": 8,       # total tasks in a block
                 "walltime": "00:10:00",
                 "initBlocks": 1,
                 "maxBlocks": 1,
                 "options": {
                     "partition": "debug",
                     "overrides": """#SBATCH --constraint=haswell
module load python/3.5-anaconda ; source activate parsl_env_3.5"""
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
| | +--------------------------+ | || | +--------------------------+ | |
| | |         MPI Task         | | || | |         MPI Task         | | |
| | +--------------------------+ | || | +--------------------------+ | |
| ++++++++++++++ || ++++++++++++++ || ++++++++++++++ || ++++++++++++++ |
========================================================================

"""
multiNodeMPI = {
    "sites": [
        {"site": "Remote_IPP_MultiNode",
         "auth": {
             "channel": "ssh",
             "hostname": "beagle.nersc.gov",
             "username": USERNAME,
             "scriptDir": "/global/homes/y/{}/parsl_scripts".format(USERNAME)
         },
         "execution": {
             "executor": "ipp",
             "provider": "slurm",
             "block": {  # Definition of a block
                 "launcher": "srun",
                 "nodes": 4,            # of nodes in that block
                 "taskBlocks": 2,       # total tasks in a block
                 "walltime": "00:10:00",
                 "initBlocks": 1,
                 "maxBlocks": 1,
                 "options": {
                     "partition": "debug",
                     "overrides": """#SBATCH --constraint=haswell
module load python/3.5-anaconda ; source activate parsl_env_3.5"""
                 }
             }
         }
         }
    ],
    "globals": {"lazyErrors": True}
}
