import parsl
from parsl import *
#from nose.tools import nottest
import os
import time
import shutil
import argparse

parsl.set_stream_logger()

'''
                      Block {Min:0, init:1, Max:1}
========================================================================
| ++++++++++++++ || ++++++++++++++ || ++++++++++++++ || ++++++++++++++ |
| |    Node    | || |    Node    | || |    Node    | || |    Node    | |
| |            | || |            | || |            | || |            | |
| | Task  Task | || | Task  Task | || | Task  Task | || | Task  Task | |
| |            | || |            | || |            | || |            | |
| ++++++++++++++ || ++++++++++++++ || ++++++++++++++ || ++++++++++++++ |
========================================================================

'''
config = {
    "sites" : [
        { "site" : "Local_IPP",
          "auth" : {
              "channel" : "ssh",
              "hostname" : "cori.nersc.gov",
              "username" : "yadunand",
              "scriptDir" : "/global/homes/y/yadunand/parsl_scripts"
          },
          "execution" : {
              "executor" : "ipp",
              "provider" : "slurm",  # LIKELY SHOULD BE BOUND TO SITE
              "script_dir" : ".scripts",
              "block" : { # Definition of a block
                  "launcher" : "srun",
                  "nodes" : 4,            # of nodes in that block
                  "taskBlocks" : 8,       # total tasks in a block
                  "walltime" : "00:10:00",
                  "initBlocks" : 1,
                  "minBlocks" : 0,
                  "maxBlocks" : 1,
                  "scriptDir" : ".",
                  "options" : {
                      "partition" : "debug",
                      "overrides" : '''#SBATCH --constraint=haswell
module load python/3.5-anaconda ; source activate parsl_env_3.5'''
                  }
              }
          }
        }
        ],
    "globals" : {   "lazyErrors" : True },
    "controller" : { "publicIp" : '*' }
}

dfk = DataFlowKernel(config=config)

@App("python", dfk)
def python_app_slow(duration):
    import platform
    import time
    time.sleep(duration)
    return "Hello from {0}".format(platform.uname())


def test_python_remote(count=10):
    ''' Run with no delay
    '''
    fus = []
    for i in range(0, count):
        fu = python_app_slow(0)
        fus.extend([fu])

    for fu in fus:
        print(fu.result())

def test_python_remote_slow(count=20):

    fus = []
    for i in range(0, count):
        fu = python_app_slow(count)
        fus.extend([fu])

    for fu in fus:
        print(fu.result())


if __name__ == "__main__" :


    test_python_remote()
    test_python_remote_slow()
