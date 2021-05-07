Running Parsl on Cori
=====================

This is a brief guide to running Parsl on NERSC's newest supercomputer, named Cori, a Cray XC40.

Requirements
============

Cori makes Python3.6 available via Conda. We'll use these packages to install Parsl and it's dependencies.
Load the conda module. Make sure to match your python version with that env you setup on Cori.::

  module load python/3.6-anaconda-4.4


Now let's create a Conda virtual environment to isolate the following package installations and activate it.::

  conda create --name parsl_env_3.6
  source activate parsl_env_3.6

Install pip to your conda env::

  conda install -n parsl_env_3.6 pip

Ensure that the pip package is coming from your local conda env dirs.::

  which pip
  pip install parsl

I'd recommend downloading the latest source and adding the source path to your PYTHONPATH.::

  git clone https://github.com/Parsl/parsl.git
  export PYTHONPATH=$PWD/parsl:$PYTHONPATH

Running IPP
===========

In order to run Parsl apps on Cori nodes, we need to first start an IPython controller on the login node.::

  ipcontroller --port=5XXXX --ip=*

.. note:: If you are timeout errors from ipengines in the submit script logs it is most likely due to
          connectivity issues between the controller and engine.In which case, try specifying the internal
          IP addresses uses on the login node. On Cori the address used for login nodes that  allows the
          compute nodes to contact the login nodes is usually : 128.55.144.130 + CoriLoginServer number.
          For eg Cori03 has 128.55.144.133 and Cori09 uses 128.55.144.139.

Once the ipcontroller is started in a separate terminal or in a screen session, we can now run parsl scripts.

Parsl Config:
=============

Here's a config for Cori that starts with a request for 2 nodes.

.. code:: python3

    config = {
    "sites" : [
        { "site" : "Local_IPP",
          "auth" : {
              "channel"   : "ssh",
              "hostname"  : "cori.nersc.gov",
              "username"  : "yadunand",
              "script_dir" : "/global/homes/y/yadunand/parsl_scripts"
          },
          "execution" : {
              "executor"   : "ipp",
              "provider"   : "slurm",
              "script_dir" : ".scripts",
              "block" : {                 # Definition of a block
                  "nodes"      : 1,       # of nodes in that block
                  "task_blocks" : 1,       # total tasks in a block
                  "walltime"   : "00:10:00",
                  "init_blocks" : 1,
                  "min_blocks"  : 0,
                  "max_blocks"  : 1,
                  "script_dir"  : ".",
                  "options"    : {
                      "partition" : "debug",
                      "overrides" : """#SBATCH --constraint=haswell
       module load python/3.5-anaconda ; source activate parsl_env_3.5"""
                  }
              }
            }
        }],
        "globals" : { "lazyErrors" : True },
        "controller" : { "publicIp" : '*' }
    }




