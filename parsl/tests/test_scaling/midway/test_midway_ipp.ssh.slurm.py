from parsl import *
import parsl
import libsubmit

#parsl.set_stream_logger()
print(parsl.__version__)
print(libsubmit.__version__)

config = {
    "sites" : [
        { "site" : "Local_IPP",
          "auth" : {
              "channel" : "ssh",
              "hostname" : "swift.rcc.uchicago.edu",
              "username" : "yadunand",
              "scriptDir" : "/scratch/midway/yadunand/parsl_scripts"
          },
          "execution" : {
              "executor" : "ipp",
              "provider" : "slurm",  # LIKELY SHOULD BE BOUND TO SITE
              "script_dir" : ".scripts",
              "block" : { # Definition of a block
                  "nodes" : 1,            # of nodes in that block
                  "taskBlocks" : 1,       # total tasks in a block
                  "walltime" : "00:05:00",
                  "initBlocks" : 1,
                  "minBlocks" : 0,
                  "maxBlocks" : 1,
                  "scriptDir" : ".",
                  "options" : {
                      "partition" : "westmere",
                      "overrides" : '''module load python/3.5.2+gcc-4.8; source /scratch/midway/yadunand/parsl_env_3.5.2_gcc/bin/activate'''
                  }
              }
          }
        }
        ],
    "globals" : {   "lazyErrors" : True },
    #"controller" : { "publicIp" : '128.135.250.229' }
    "controller" : { "publicIp" : '*' }
}

dfk = DataFlowKernel(config=config)


@App("python", dfk)
def python_app():
    import platform
    return "Hello from {0}".format(platform.uname())


@App("bash", dfk)
def bash_app(stdout=None, stderr=None):
    return 'echo "Hello from $(uname -a)" ; sleep 2'


def test_python():
    import os
    results = {}
    for i in range(0,2):
        results[i] = python_app()

    print("Waiting ....")
    print(results[0].result())


def test_bash():
    import os
    fname = os.path.basename(__file__)

    x = bash_app(stdout="{0}.out".format(fname))
    print("Waiting ....")
    print(x.result())


if __name__ == "__main__" :

    test_python()
    test_bash()
