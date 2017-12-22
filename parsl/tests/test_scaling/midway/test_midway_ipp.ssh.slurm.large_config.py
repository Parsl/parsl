from parsl import *
import parsl
import libsubmit
import os
import time
#parsl.set_stream_logger()
print(parsl.__version__)
print(libsubmit.__version__)

MIDWAY_USERNAME = "yadunand"
config = {
    "sites" : [
        { "site" : "Local_IPP",
          "auth" : {
              "channel" : "ssh",
              "hostname" : "swift.rcc.uchicago.edu",
              "username" : MIDWAY_USERNAME,
              "scriptDir" : "/scratch/midway2/{0}/parsl_scripts".format(MIDWAY_USERNAME)
          },
          "execution" : {
              "executor" : "ipp",
              "provider" : "slurm",
              "block" : {                  # Definition of a block
                  "nodes" : 1,             # of nodes in that block
                  "taskBlocks" : "$(($CORES*1))", # total tasks in a block
                  "walltime" : "00:05:00",
                  "initBlocks" : 8,
                  "maxBlocks" : 1,
                  "options" : {
                      "partition" : "westmere",
                      "overrides" : '''module load python/3.5.2+gcc-4.8; source /scratch/midway2/yadunand/parsl_env_3.5.2_gcc/bin/activate'''
                  }
              }
          }
        }
        ],
    "globals" : {   "lazyErrors" : True },
    #"controller" : { "publicIp" : '*' }
}

dfk = DataFlowKernel(config=config)


@App("python", dfk)
def python_app(sleep_duration=0.5):
    import platform
    import time
    time.sleep(sleep_duration)
    return "Hello from {0}".format(platform.uname())


@App("bash", dfk)
def bash_app(stdout=None, stderr=None):
    return 'echo "Hello from $(uname -a)" ; sleep 2'


def test_python(N=2000, sleep_duration=0.5):
    results = {}

    start = time.time()
    for i in range(0,N):
        results[i] = python_app(sleep_duration=sleep_duration)
    end = time.time()
    print("Launched {} tasks in : {}. Task rate: {} Tasks/sec".format(N, end-start, float(N)/(end-start)))
    print("Waiting ....")

    start = time.time()
    x = [results[i].result() for i in results]
    end = time.time()
    print("Completed all tasks in :", end-start)
    print("Ideal time : {}*{} = {} / parallelism".format(N, sleep_duration, N*sleep_duration))
    print("Unique items : ")
    for item in set(x):
        print(item)


def test_bash():
    import os
    fname = os.path.basename(__file__)

    x = bash_app(stdout="{0}.out".format(fname))
    print("Waiting ....")
    print(x.result())


if __name__ == "__main__" :

    test_python()
    #test_bash()
