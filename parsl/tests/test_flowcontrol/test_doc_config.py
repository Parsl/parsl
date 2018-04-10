from parsl import *
import os
USERNAME = os.environ["MIDWAY_USERNAME"]

localIPP = {
    "sites": [
        {"site": "Midway_RCC_Slurm",
         "auth": {
             "channel": "ssh",
             "hostname": "midway.rcc.uchicago.edu",
             "username": USERNAME,
             "scriptDir": "/scratch/midway2/{0}/parsl_scripts".format(USERNAME)
         },
         "execution": {
             "executor": "ipp",
             "provider": "slurm",  # Slurm scheduler
             "block": {  # Definition of a block
                 "nodes": 1,  # }
                 "minBlocks": 1,  # |
                 "maxBlocks": 2,  # |<---- Shape of the blocks
                 "initBlocks": 1,  # }
                 "taskBlocks": 4,  # <----- No. of workers in a block
                 "parallelism": 0.5,  # <- Parallelism
                 "options": {
                     "partition": "westmere",
                     "overrides": '''module load python/3.5.2+gcc-4.8; source /scratch/midway2/yadunand/parsl_env_3.5.2_gcc/bin/activate'''
                 }
             }
         }
         }]
}
dfk = DataFlowKernel(config=localIPP)


@App("python", dfk)
def python_app():
    import os
    import time
    import platform
    time.sleep(20)
    return "Hello from {0}:{1}".format(os.getpid(), platform.uname())


def test_python(N=20):
    ''' Testing basic scaling|Python 0 -> 1 block on SSH.Midway  '''

    results = {}
    for i in range(0, N):
        results[i] = python_app()

    print("Waiting ....")
    for i in range(0, N):
        print(results[0].result())


if __name__ == '__main__':

    test_python()
