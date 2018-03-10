import os

USERNAME = os.environ['CORI_USERNAME']

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
             "hostname": "cori.nersc.gov",
             "username": USERNAME,
             "scriptDir": "/global/homes/y/{}/parsl_scripts".format(USERNAME)
         },
         "execution": {
             "executor": "ipp",
             "provider": "slurm",
             "block": {  # Definition of a block
                 "nodes": 1,            # of nodes in that block
                 "taskBlocks": 1,       # total tasks in a block
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

singleNodeLocal = {
    "sites": [
        {"site": "Local_IPP_Cori",
         "auth": {
             "channel": "local",
             "hostname": "cori.nersc.gov",
             "username": USERNAME,
             "scriptDir": "/global/homes/y/{}/parsl_scripts".format(USERNAME)
         },
         "execution": {
             "executor": "ipp",
             "provider": "slurm",
             "block": {  # Definition of a block
                 "nodes": 1,            # of nodes in that block
                 "taskBlocks": 1,       # total tasks in a block
                 "initBlocks": 1,
                 "maxBlocks": 1,
                 "options": {
                     "partition": "regular",
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
             "hostname": "cori.nersc.gov",
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
             "hostname": "cori.nersc.gov",
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
