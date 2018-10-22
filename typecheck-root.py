#!/usr/bin/env python3

import parsl
from parsl import File
from parsl import DataFlowKernel
from parsl import DataFlowKernelLoader
from parsl.config import *
from parsl.app.app import bash_app

parsl.set_stream_logger()

c = Config(executors=[ThreadPoolExecutor()])

dfk = DataFlowKernel(config = c)

# workaround, issue #381
DataFlowKernelLoader._dfk = dfk

@bash_app(data_flow_kernel = dfk)
def foo(inputs=[]):
  return ("ls ; echo X{}X ; [ -f {} ] ".format(inputs[0], inputs[0]))


# myfile = File("myproto://stuff.txt")
myfile = 'tmp.txt'
# myfile = 'file:///etc/passwd'

future = foo(inputs=[myfile])

print("RESULT: {}".format(future.result()))

