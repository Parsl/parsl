from parsl import *
import parsl
import libsubmit

parsl.set_stream_logger()
print(parsl.__version__)
print(libsubmit.__version__)

from libsubmit import Cobalt

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
                      "overrides" : ""
                  }
              }
          }
        }
        ],
        "globals" : {
            "lazyErrors" : True
        }

}

dfk = DataFlowKernel(config=config)


@App("python", dfk)
def python_app():
    import platform
    return "Hello from {0}".format(platform.uname())


@App("bash", dfk)
def bash_app(stdout=None, stderr=None):
    cmd_line = 'echo "Hello from $(uname -a)" ; sleep 2'


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
