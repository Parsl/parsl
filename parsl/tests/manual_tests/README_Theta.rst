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
   
