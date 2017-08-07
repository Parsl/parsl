Running Parsl on Cori
=====================

This is a brief guide to running Parsl on NERSC's newest supercomputer, named Cori, a Cray XC40.

Requirements
============

Cori makes Python3.6 available via Conda. We'll use these packages to install Parsl and it's dependencies.

>>> module load python/3.6-anaconda-4.4

Now let's create a Conda virtual environment to isolate the following package installations and activate it.
 
>>> conda create --name parsl-env
>>> source activat parsl-env

If the last step worked your prompt is now prefixed by (parsl-env). Let's install the packages:

>>> conda install -c yadudoc1729 parsl
>>> conda install boto3


I'd recommend downloading the latest source and adding the source path to your PYTHONPATH.

>>> git clone https://github.com/Parsl/parsl.git
>>> export PYTHONPATH=$PWD/parsl:$PYTHONPATH

Running IPP
===========

In order to run Parsl apps on Cori nodes, we need to first start an IPython controller on the login node,
figuring out the IP address to use for the controller is a bit of a hassle on Cori. The internal IP addresses
used that allows the compute nodes to contact the login nodes is usually : 128.55.144.130 + CoriLoginServer number.
For eg Cori03 has 128.55.144.133 and Cori09 uses 128.55.144.139.

>>> ipcontroller --port=5XXXX --ip=<InternalIPAddress> 

Once the ipcontroller is started in a separate terminal or in a screen session, we can now run parsl scripts.

Parsl Config:
=============

Here's a config for Cori that starts with a request for 2 nodes.

.. code ::python

     config = {  "site" : "midway_westmere",
              "execution" :
              {  "executor" : "ipp",
                 "provider" : "slurm",
                 "channel"  : "local",
                 "options" :
                 {"init_parallelism" : 2,      # Starts with 2 nodes
                  "max_parallelism" : 2,       # Limits this run to 2 nodes
                  "min_parallelism" : 0,  
                  "tasks_per_node"  : 1,       # One engine per node
                  "nodes_granularity" : 1,     # Request one node per slurm request
                  "partition" : "debug",       # Send request to the debug partition
                  "walltime" : "00:05:00",     # Walltime 
                 "slurm_overrides" : "#SBATCH --constraint=haswell", # All additional slurm constraints
                  "submit_script_dir" : ".scripts"
                 }
              }
              } 


