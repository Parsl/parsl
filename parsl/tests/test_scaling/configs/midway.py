import os
USERNAME = os.environ['MIDWAY_USERNAME']

singleNode = {
    "sites": [
        {"site": "Midway_Remote_IPP",
          "auth": {
              "channel": "ssh",
              "hostname": "swift.rcc.uchicago.edu",
              "username": USERNAME,
              "scriptDir": "/scratch/midway2/{0}/parsl_scripts".format(USERNAME)
          },
          "execution": {
              "executor": "ipp",
              "provider": "slurm",
              "block": { # Definition of a block
                  "nodes": 1,            # of nodes in that block
                  "taskBlocks": 1,       # total tasks in a block
                  "initBlocks": 1,
                  "maxBlocks": 1,
                  "options": {
                      "partition": "westmere",
                      "overrides": '''module load python/3.5.2+gcc-4.8; source /scratch/midway2/yadunand/parsl_env_3.5.2_gcc/bin/activate'''
                  }
              }
          }
         }
        ],
    "globals": {"lazyErrors": True}
}

singleNodeLocal = {
    "sites": [
        {"site": "Midway_Local_IPP",
          "auth": {
              "channel": "local",
              "scriptDir": "/scratch/midway2/{0}/parsl_scripts".format(USERNAME)
          },
          "execution": {
              "executor": "ipp",
              "provider": "slurm",
              "block": { # Definition of a block
                  "nodes": 1,            # of nodes in that block
                  "taskBlocks": 1,       # total tasks in a block
                  "initBlocks": 1,
                  "maxBlocks": 1,
                  "options": {
                      "partition": "westmere",
                      "overrides": '''module load python/3.5.2+gcc-4.8; source /scratch/midway2/yadunand/parsl_env_3.5.2_gcc/bin/activate'''
                  }
              }
          }
         }
        ],
    "globals": {"lazyErrors": True}
}


multiCore = {
    "sites": [
        {"site": "Midway_N_Workers_IPP",
          "auth": {
              "channel": "ssh",
              "hostname": "swift.rcc.uchicago.edu",
              "username": USERNAME,
              "scriptDir": "/scratch/midway2/{0}/parsl_scripts".format(USERNAME)
          },
          "execution": {
              "executor": "ipp",
              "provider": "slurm",
              "block": {                  # Definition of a block
                  "nodes": 1,             # of nodes in that block
                  "taskBlocks": "$CORES", # total tasks in a block
                  "walltime": "00:05:00",
                  "initBlocks": 1,
                  "maxBlocks": 1,
                  "options": {
                      "partition": "westmere",
                      "overrides": '''module load python/3.5.2+gcc-4.8; source /scratch/midway2/yadunand/parsl_env_3.5.2_gcc/bin/activate'''
                  }
              }
          }
         }
        ],
    "globals": {"lazyErrors": True}
}

multiNode = {
    "sites": [
        {"site": "Midway_MultiNode",
          "auth": {
              "channel": "ssh",
              "hostname": "swift.rcc.uchicago.edu",
              "username": USERNAME,
              "scriptDir": "/scratch/midway2/{0}/parsl_scripts".format(USERNAME)
          },
          "execution": {
              "executor": "ipp",
              "provider": "slurm",
              "block": {                  # Definition of a block
                  "nodes": 1,             # of nodes in that block
                  "taskBlocks": "$(($CORES*1))", # total tasks in a block
                  "walltime": "00:05:00",
                  "initBlocks": 8,
                  "maxBlocks": 1,
                  "options": {
                      "partition": "westmere",
                      "overrides": '''module load python/3.5.2+gcc-4.8; source /scratch/midway2/yadunand/parsl_env_3.5.2_gcc/bin/activate'''
                  }
              }
          }
         }
        ],
    "globals": {"lazyErrors": True}
}
