Configuring
===========

Parsl allows the user to compose a workflow that is completely separate from
the details of it's execution. So far we've seen how apps can be constructed
from pure python as well calls to external applications. Once a workflow is
created, the execution substrate on which it is to be executed over needs to
described to parsl. There are two ways to do this:

1. **Executors** which use threads, ipyparallel workers etc could be constructed manually

   .. code-block:: python

       from parsl import *
       workers = ThreadPoolExecutor(max_workers=4)
       dfk = DataFlowKernel(executors=[workers])

2. A **config** could be passed to the data flow kernel which will initialize the required resources.

   .. code-block:: python

      from parsl import *
      config = {
          "sites" : [
              { "site" : "Local_Threads",
                "auth" : { "channel" : None },
                "execution" : {
                    "executor" : "threads",
                    "provider" : None,
                    "max_workers" : 4
                }
              }],
          "globals" : {"lazyErrors" : True}
      }
      dfk = DataFlowKernel(config=config)

