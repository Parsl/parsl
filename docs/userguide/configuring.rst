Configuring
===========

Parsl allows the user to compose a workflow that is completely separate from
the details of its execution. So far, we've seen how apps can be constructed
from pure python as well calls to external applications. Once a workflow is
created, the execution substrate on which it is to be executed over needs to
described to parsl. There are two ways to do this:

1. **Executors** which use threads, iPyParallel workers, etc. can be constructed manually

   .. code-block:: python

       from parsl import *
       workers = ThreadPoolExecutor(max_workers=4)
       dfk = DataFlowKernel(executors=[workers])

2. A **config** can be passed to the data flow kernel, which will initialize the required resources.

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


The config data structure is a python dictionary that is organized as follows:

.. code-block :: python

     {
       "sites" : [ list of site definitions ],
       "globals" : { dict of attributes global to the workflow }
       "controller" : { dict of attributes specific to the local IPP controller(s) }
     }

The `sites` field in the top level config definition is a list of execution sites that are
defined by a dictionary of the following structure :

.. code-block :: python

    {
       "site" : < str name of the site being defined>

       # dictionary of attributes that define how the execution resource is accessed
       "auth" : {
          # Define the channel type used to reach the site
          "channel" : <str (local, ssh, ssh-il)>,
       }

       # The execution block defines how resources can be requested and how the resources should be
       # shaped to best match the workflow needs.
       "execution" : {
           # The executor is the mechanism that executes tasks on the compute
           # resources provisioned from the site
           "executor" : <str (ipp, threads, swift_t)>,

           # Select the kind of scheduler or resource type of the site
           "provider" : <str (slurm, torque, cobalt, condor, aws, azure, local ...)>

           # A block is the unit by which resources are requested from the site
           "block" : {
                "nodes" : <int: nodes to request per block>,
                "taskBlocks" : <str: workers to start per block, or bash expression>,
                "initBlocks" : <int: blocks to provision at the execution start>,
                "minBlocks" : <int: min blocks to maintain during execution>,
                "maxBlocks" : <int: max blocks that can be provisioned>,
                "walltime" : <str: Walltime allowed for the block in HH:MM:SS format>,
                # The "options" block contains attributes that are provider specific
                # such as scheduler options
                "options" : { dict of provider specific attributes },
           }
       }
    }
