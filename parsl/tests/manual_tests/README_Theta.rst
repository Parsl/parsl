Using the MPI Executor to run locally
=====================================


Using the MPI Executor to run on Theta
======================================

Setup the environment on theta:

1. Module load intel python
2. Conda create a new environment `parsl_intel_py3.5`
3. Go to the mpi_executor branch and install using
    `pip install .`
4. Install mpi4py: `pip install mpi4py`
5. Check the overrides to make sure to follow the example in `parsl/parsl/tests/configs/theta_local_mpi_executor.py`
6. We don't yet have an executable for the mpi fabric program, so
   we need to specify it explicitly via the MPIExecutor.launch_cmd.

Using the ExtremeScaleExecutor on Theta
=======================================

[TODO] This is not complete.

Installing
----------

Loaded Modulefiles Prior to setup :

1) modules/3.2.10.6                                7) ugni/6.0.14-6.0.6.0_18.12__g777707d.ari        13) dvs/2.7_2.2.95-6.0.6.1_9.3__gd0b8528           19) craype-mic-knl
2) intel/18.0.0.128                                8) pmi/5.0.14                                     14) alps/6.6.1-6.0.6.1_4.1__ga6396bb.ari           20) cray-mpich/7.7.2
3) craype-network-aries                            9) dmapp/7.1.1-6.0.6.0_51.37__g5a674e0.ari        15) rca/2.2.18-6.0.6.0_19.14__g2aa4f39.ari         21) nompirun/nompirun
4) craype/2.5.15                                  10) gni-headers/5.0.12-6.0.6.0_3.26__g527b6e1.ari  16) atp/2.1.2                                      22) darshan/3.1.5
5) cray-libsci/18.07.1                            11) xpmem/2.2.14-6.0.6.0_10.1__g34333c9.ari        17) perftools-base/7.0.2                           23) trackdeps
6) udreg/2.3.2-6.0.6.0_15.18__g5196236.ari        12) job/2.2.3-6.0.6.0_9.47__g6c4e934.ari           18) PrgEnv-intel/6.0.4                             24) xalt

1. Module load IntelPython::
     $ module load intelpython35/2017.0.035

2. Create a conda environment and activate it::
     $ conda create --name parsl_extreme_scale python=3.6
     $ source activate parsl_extreme_scale

3. Clone Parsl from the github::
     $ git clone git@github.com:Parsl/parsl.git
     $ git checkout mpi_executor_heartbeat

4. We need to install `mpi4py` with the Cray CC::
     $ env MPICC=/opt/cray/pe/craype/2.5.15/bin/cc pip install mpi4py

5. To test whether the `mpi4py` install works::
   $ qsub -t 60 -q debug-flat-quad -n 2 -A <PROJECT_NAME> -I
   $ aprun -n 8 -N 4 python3 -m mpi4py.bench helloworld

6. Install Parsl with support for extreme scale::
   $ cd parsl
   $ pip install .

