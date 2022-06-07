Running Parsl on Midway
=======================

This is a brief guide to running Parsl on UChicago RCC's midway cluster.

Requirements
============

Make sure you have python3.6 and Parsl installed with all it's dependencies.

Running IPP
===========

In order to run Parsl apps on Midway nodes, we need to first start an IPython controller on the login node.::

  ipcontroller --port=5XXXX --ip=*

Once the ipcontroller is started in a separate terminal or in a screen session, we can now run parsl scripts.

Parsl Config:
=============

Here's a config for Midway that starts with a request for 2 nodes.

.. code:: python3

        USERNAME = <SET_YOUR_USERNAME>

        { "site" : "Midway_Remote_Westmere",
          "auth" : {
              "channel" : "ssh",
              "hostname" : "swift.rcc.uchicago.edu",
              "username" : USERNAME,
              "script_dir" : "/scratch/midway2/{0}/parsl_scripts".format(USERNAME)
          },
          "execution" : {
              "executor" : "ipp",
              "provider" : "slurm",
              "block" : { # Definition of a block
                  "nodes" : 1,            # of nodes in that block
                  "task_blocks" : 1,       # total tasks in a block
                  "init_blocks" : 1,
                  "max_blocks" : 1,
                  "options" : {
                      "partition" : "westmere",
                      "overrides" : """module load python/3.5.2+gcc-4.8; source /scratch/midway2/yadunand/parsl_env_3.5.2_gcc/bin/activate"""
                  }
              }
          }
        }


