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


The config data structure is a python dictionary organized as follows :

.. code-block :: python

     {
       "sites" : [ <an list of site definitions> ],
       "globals" : { <dictionary of attributes global to the workflow> }
       "controller" : { <dictionary of attributes specific to the local IPP controller(s) }
     }

The `sites` field in the top level config definition is a list of execution sites that are
defined by a dictionary of the following structure :

.. code-block :: python

    {
       "site" : <String: Name of the site being defined>

       # dictionary of attributes that define how the execution resource is accessed
       "auth" : {
          # Define the channel type used to reach the site
          "channel" : <local, ssh, ssh-il>,
       }

       # The execution block defines how resources can be requested and the resources should be
       # shaped to best match the workflow needs.
       "execution" : {
           # The executor is the mechanism that executes tasks on the compute
           # resources provisioned from the site
           "executor" : <ipp, threads, swift_t>,

           # Select the kind of scheduler or resource type of the site
           "provider" : <slurm, torque, cobalt, condor, aws, azure, local>

           # A block is the unit by which resources are requested from the site
           "block" : {
                "nodes" : <number of nodes to request per block>,
                "taskBlocks" : <number of workers to start per block>,
                "initBlocks" : <number of blocks to provision at the execution start>,
                "minBlocks" : <min blocks to maintain during execution>,
                "maxBlocks" : <max blocks that can be provisioned>,
                "walltime" : <Walltime allowed for the block in HH:MM:SS format>,
                # The "options" block contains attributes that are provider specific
                # such as scheduler options
                "options" : { <provider specific attributes> },
           }
       }
    }
