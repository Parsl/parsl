Environment setup on CC-IN2P3
=============================

For the DESC folks using CC-IN2P3 resources under the LSST allocation, there's an anaconda installation
on the shared project space that can be used. If you are not under this allocation, please follow
instructions to install anaconda to your $HOME directory from `here <https://conda.io/docs/user-guide/install/index.html>`_.

For DESC users :

.. code-block:: bash

    # Add this to your $HOME/.bashrc file 
    export PATH=/pbs/throng/lsst/software/anaconda/anaconda3-5.0.1/bin:$PATH

    # Create your conda env for Parsl, this will by default install to your $HOME/.conda directory
    conda create --name parsl_env_3.6 python=3.6

    # Activate the env
    source activate parsl_env_3.6

    # Install Parsl
    pip install parsl

Important Notes
===============

The GridEngine is designed for the Grid, a loose collection with cores being 
the compute unit which is quite different from HPC systems and Clouds.
As a result resource requests are made in terms of cores, and multinode 
mpi-launch capabilities with "mpirun/gerun".

Please refer `here<https://wiki.rc.ucl.ac.uk/wiki/Example_Submission_Scripts>`_ for 
the config option supported by gridEngine.



