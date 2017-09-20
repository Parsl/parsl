Running Parsl on Midway
=======================

This is a brief guide to running Parsl on UChicago RCC's midway cluster.

Requirements
============

Make sure you have python3.6 and Parsl installed with all it's dependencies.

Running IPP
===========

In order to run Parsl apps on Midway nodes, we need to first start an IPython controller on the login node.

>>> ipcontroller --port=5XXXX --ip=*

Once the ipcontroller is started in a separate terminal or in a screen session, we can now run parsl scripts.

Parsl Config:
=============

Here's a config for Midway that starts with a request for 2 nodes.

.. code:: python3

     config = {"site" : "midway-westmere",
               "execution" :
                  {"executor" : "ipp",
                   "provider" : "slurm",
                   "channel"  : "local",
                   "options" :
                       {"init_parallelism" : 2,      # Starts with 2 nodes
                        "max_parallelism" : 2,       # Limits this run to 2 nodes
                        "min_parallelism" : 0,
                        "tasks_per_node"  : 1,       # One engine per node
                        "nodes_granularity" : 1,     # Request one node per slurm request
                        "partition" : "westmere",       # Send request to the debug partition
                        "walltime" : "00:05:00",     # Walltime
                        "submit_script_dir" : ".scripts"
                       }
                   }
              }




