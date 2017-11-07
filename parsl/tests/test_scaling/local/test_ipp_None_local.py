from parsl import *
import parsl
import libsubmit

print(parsl.__version__)
print(libsubmit.__version__)

config = {
    "sites" : [
        { "site" : "Local_IPP",
          "auth" : {
              "channel" : None,
          },
          "execution" : {
              "executor" : "ipp",
              "provider" : "local",  # LIKELY SHOULD BE BOUND TO SITE
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
                      "partition" : "debug",
                      "overrides" : "source /home/yadunand/setup_cooley_env.sh"
                  }
              }
          }
        }
        ],
    "globals" : {
        "lazyErrors" : True
    },
    "controller" : {"ip" : "*"}

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
    ''' Testing basic python functionality '''

    import os
    results = {}
    for i in range(0,2):
        results[i] = python_app()

    print("Waiting ....")
    print(results[0].result())


def test_bash():
    ''' Testing basic bash functionality '''

    import os
    fname = os.path.basename(__file__)

    x = bash_app(stdout="{0}.out".format(fname))
    print("Waiting ....")
    print(x.result())


if __name__ == "__main__" :

    test_python()
    test_bash()
