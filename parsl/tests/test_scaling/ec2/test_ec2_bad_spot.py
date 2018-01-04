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
==================
| ++++++++++++++ |
| |    Node    | |
| |            | |
| | Task  Task | |
| |            | |
| ++++++++++++++ |
==================

'''
config = {
    "sites" : [
        { "site" : "Remote_IPP",
          "auth" : {
              "channel" : None,
              "profile" : "default",
          },
          "execution" : {
              "executor" : "ipp",
              "provider" : "aws",  # LIKELY SHOULD BE BOUND TO SITE
              "block" : { # Definition of a block
                  "nodes" : 1,            # of nodes in that block
                  "taskBlocks" : 2,       # total tasks in a block
                  "walltime" : "01:00:00",
                  "initBlocks" : 1,
                  "options" : {
                      "instanceType" : "m4.4xlarge",
                      "region" : "us-east-2",
                      "imageId" : 'ami-82f4dae7',
                      "stateFile" : "awsproviderstate.json",
                      "keyName" : "parsl.test",
                      "spotMaxBid" : 0.001,
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
    #test_python_remote_slow()
