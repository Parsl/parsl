from parsl import *
import parsl
import libsubmit

parsl.set_stream_logger()
print(parsl.__version__)
print(libsubmit.__version__)

from libsubmit import Cobalt

config = {
    "sites" : [
        { "site" : "ALCF_Cooley_Remote",
          "auth" : {
              "channel" : "local",
              #"hostname" : "cooleylogin1.alcf.anl.gov",
              #"username" : "yadunand"
          },
          "execution" : {
              "executor" : "ipp",
              "provider" : "cobalt",  # LIKELY SHOULD BE BOUND TO SITE
              "script_dir" : ".scripts",
              "block" : { # Definition of a block
                  "nodes" : 1,            # of nodes in that block
                  "taskBlocks" : 1,       # total tasks in a block
                  "walltime" : "00:05:00",
                  "initBlocks" : 2,
                  "minBlocks" : 0,
                  "maxBlocks" : 2,
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
        }

}

dfk = DataFlowKernel(config=config)


@App("python", dfk)
def test():
    import platform
    return "Hello from {0}".format(platform.uname())


results = {}
for i in range(0,5):

    results[i] = test()

print("Waiting ....")
print(results[0].result())
