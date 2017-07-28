import parsl
from parsl import *
#from nose.tools import nottest
import os
import time
import shutil
import argparse
from parsl.execution_provider.slurm.slurm import Slurm
parsl.set_stream_logger()

def test_config_A ():
    '''
    ================== Block
    | ++++++++++++++ | Node
    | |            | |
    | |    Task    | |             . . .
    | |            | |
    | ++++++++++++++ |
    ==================
    '''
    config = {  "site" : "midway_westmere",
                "execution" :
                { "options" :
                  {"init_parallelism" : 2,
                   "max_parallelism" : 2,
                   "min_parallelism" : 0,
                   "tasks_per_node"  : 1,
                   "nodes_granularity" : 1,
                   "queue" : "westmere",
                   "walltime" : "00:25:00",
                   "account" : "pi-wilde",
                   "submit_script_dir" : ".scripts"
                  }
                }}
    print("Config : ", config)
    slurm = Slurm (config)
    x = slurm.submit('''echo "sleeping"
    sleep 120
    echo "Done sleeping" ''', 1)
    time.sleep(3)
    y  = slurm.submit('''echo "sleeping"
    sleep 120
    echo "Done sleeping" ''', 1)
    time.sleep(3)
    stats = slurm.status([x,y])

    slurm.cancel([x,y])
    print(stats)


def test_config_B () :
    '''
    ======================== Block
    | ++++++++++++++++++++ | Node
    | |                  | |
    | |  Task     Task   | |
    | |                  | |
    | |  Task     Task   | |
    | |                  | |
    | ++++++++++++++++++++ |
    ========================
    '''
    config = { "execution" :
               { "options" :
                 {"init_parallelism" : 2,
                  "max_parallelism" : 2,
                  "min_parallelism" : 0,
                  "tasks_per_node"  : 4,
                  "nodes_granularity" : 1,
                  "queue" : "westmere",
                  "walltime" : "00:25:00",
                  "account" : "pi-wilde"
                 }
               }
    }
    slurm = Slurm (config)

def test_config_C () :

    '''
    ========================    Block    ========================
    | ++++++++++++++++++++ |    Node     | ++++++++++++++++++++ |
    | |                  | |             | |                  | |
    | |         T           A          S           K          | |
    | |                  | |             | |                  | |
    | ++++++++++++++++++++ |             | ++++++++++++++++++++ |
    ========================             ========================
                              ...
    '''
    config = { "execution" :
               { "options" :
                 {"init_parallelism" : 8,
                  "max_parallelism" : 8,
                  "min_parallelism" : 0,
                  "tasks_per_node"  : 1/2,
                  "nodes_granularity" : 2,
                  "queue" : "westmere",
                  "walltime" : "00:25:00",
                  "account" : "pi-wilde"
                 }
               }}
    slurm = Slurm (config)
    x = slurm.submit("echo 'Hello'", 1)

if __name__ == "__main__" :

    test_config_A()
    #test_config_B
    #test_config_C
