Configuration
=============

Parsl workflows are developed completely independently from their execution environment. Parsl offers an extensible configuration model through which the execution environment and communication with that environment is configured.

Parsl can be configured using a Python configuration object. For simple cases, such as threads these configurations can be easily specified inline. For more complex environments using different block configurations and communication channels it is easiest to define a full configuration object. The following shows how the configuration can be passed to the Dataflow Kernel.


1. **Executors**, which use threads, iPyParallel workers, etc. can be constructed manually

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
                    "maxThreads" : 4
                }
              }],
          "globals" : {"lazyErrors" : True}
      }
      dfk = DataFlowKernel(config=config)


Configuration Structure
-----------------------

The configuration data structure is a python dictionary that describes execution sites as well as other information such as global attributes, controller information, and in the future data staging information.

.. code-block :: python

     {
       "sites" : [ list of site definitions ],
       "globals" : { dict of attributes global to the workflow }
       "controller" : { dict of attributes specific to the local IPP controller(s) }
     }

The most important part of this configuration is the `sites` key. Parsl allows multiple sites to be specified in a list. The configuration for an individual site is as follows:

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
                "nodes"      : <int: nodes to request per block>,
                "taskBlocks" : <str: workers to start per block, or bash expression>,
                "initBlocks" : <int: blocks to provision at the execution start>,
                "minBlocks"  : <int: min blocks to maintain during execution>,
                "maxBlocks"  : <int: max blocks that can be provisioned>,
                "walltime"   : <str: walltime allowed for the block in HH:MM:SS format>,

                # The "options" block contains attributes that are provider specific
                # such as scheduler options
                "options" : {
                     #dict of provider specific attributes, please refer to provider
                     # specific documentation.
                }
           }
       }
    }


The following shows an example configuration for accessing NERSC's Cori supercomputer. This example uses the IPythonParallel executor and connects to Cori's Slurm scheduler. It uses a remote SSH channel that allows the IPythonParallel controller to be hosted on the script's submission machine (e.g., a PC).  It is configured to request 2 nodes configured with 1 TaskBlock per node. Finally it includes override information to request a particular node type (Haswell) and to configure a specific Python environment on the worker nodes using Anaconda.

.. code-block :: python

    config = {
        "sites" : [
            { "site" : "Cori.Remote.IPP",
              "auth" : {
                  "channel" : "ssh",
                  "hostname" : "cori.nersc.gov",
                  "username" : "username",
                  "scriptDir" : "/global/homes/y/username/parsl_scripts"
              },
              "execution" : {
                  "executor" : "ipp",
                  "provider" : "slurm",
                  "block" : {
                      "nodes" : 2,
                      "taskBlocks" : 1,
                      "walltime" : "00:10:00",
                      "initBlocks" : 1,
                      "minBlocks" : 0,
                      "maxBlocks" : 1,
                      "scriptDir" : ".",
                      "options" : {
                          "partition" : "debug",
                          "overrides" : """#SBATCH --constraint=haswell
    module load python/3.5-anaconda ;
    source activate /global/homes/y/yadunand/.conda/envs/parsl_env_3.5"""
                      }
                  }
              }
            }
            ],
        "globals" : {   "lazyErrors" : True },
    }
