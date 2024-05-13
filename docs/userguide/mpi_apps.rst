MPI and Multi-node Apps
=======================

The :class:`~parsl.executors.MPIExecutor` support running MPI applications or other computations which can
run on multiple compute nodes.

Background
----------

MPI applications run multiple copies of a program that complete a single task by
coordinating using messages passed within or across nodes.

Starting MPI application requires invoking a "launcher" code (e.g., ``mpiexec``) from one node
with options that define how the copies of a program should be distributed to others.

The launcher includes a series of options that control how copies of the program are distributed
across the nodes (e.g., how many copies per node) and
how each copy is configured (e.g., which CPU cores it can use),
among other options.

The options for launchers vary slightly between MPI implementations and compute clusters.

Configuring ``MPIExecutor``
---------------------------

The :class:`~parsl.executors.MPIExecutor` is a wrapper over
:class:`~parsl.executors.high_throughput.executor.HighThroughputExecutor`
which eliminates options that are irrelevant for MPI applications.

Define an

1. Set ``max_workers_per_block`` to maximum number of tasks to run per block of compute nodes.
   This value is typically the number of nodes per task divided by the number of nodes per task.
2. Set ``mpi_launcher`` to launcher used for your application.
3. Specify the provider that matches your cluster and use the :class:`~parsl.launchers.SimpleLauncher`,
   which will ensure that no Parsl processes are placed on the compute nodes.

An example for ALCF's Polaris supercomputer:

.. code-block:: python

    config = Config(
        executors=[
            MPIExecutor(
                address=address_by_interface('bond0'),
                max_workers_per_block=4,  # Up to 4 tasks
                provider=PBSProProvider(
                    account="parsl",
                    worker_init=f"""module load miniconda; source activate /lus/eagle/projects/parsl/env""",
                    walltime="1:00:00",
                    queue="debug",
                    scheduler_options="#PBS -l filesystems=home:eagle:grand",
                    launcher=SimpleLauncher(),
                    select_options="ngpus=4",
                    nodes_per_block=4,
                    max_blocks=1,
                    cpus_per_node=64,
                ),
            ),
        ]
    )


Writing an MPI App
------------------

:class:`~parsl.executors.high_throughput.MPIExecutor` can execute both Python or Bash Apps which invoke an MPI application.

Create the app by first defining a function which includes ``parsl_resource_specification`` keyword argument.

Then, replace the call to the MPI launcher with ``$PARSL_MPI_PREFIX``.
``$PARSL_MPI_PREFIX`` references an environmental variable which will be replaced with
the correct MPI launcher configured for the resource list provided when calling the function
and with options that map the task to nodes which Parsl knows to be available.

The function can be a Bash app

.. code-block:: python

    @bash_app
    def lammps_mpi_application(infile: File, parsl_resource_specification: Dict):
        # PARSL_MPI_PREFIX will resolve to `mpiexec -n 4 -ppn 2 -hosts NODE001,NODE002`
        return f"$PARSL_MPI_PREFIX lmp_mpi -in {infile.filepath}"


or a Python app:


.. code-block:: python

    @bash_app
    def lammps_mpi_application(infile: File, parsl_resource_specification: Dict):
        from subprocess import run
        with open('stdout.lmp', 'w') as fp, open('stderr.lmp', 'w') as fe:
            proc = run(['$PARSL_MPI_PREFIX', '-i', 'in.lmp'], stdout=fp, stderr=fe)
            return proc.returncode


Run either App by calling with its arguments and a resource specification which defines how to execute it

.. code-block:: python

    # Resources in terms of nodes and how ranks are to be distributed are set on a per app
    # basis via the resource_spec dictionary.
    resource_spec = {
        "num_nodes" = 2,
        "ranks_per_node" = 2,
        "num_ranks" = 4,
    }
    future = lammps_mpi_application(File('in.file'), parsl_resource_specification=resource_spec)

Advanced: More Environment Variables
++++++++++++++++++++++++++++++++++++

Parsl Apps which run using :class:`~parsl.executors.high_throughput.MPIExecutor`
can make their own MPI invocation by leveraging the other environment variables.

These include versions of the launch command for different launchers

- ``PARSL_MPIEXEC_PREFIX``: mpiexec launch command which works for a large number of batch systems especially PBS systems
- ``PARSL_SRUN_PREFIX``: srun launch command for Slurm based clusters
- ``PARSL_APRUN_PREFIX``: aprun launch command prefix for some Cray machines

And the information used by Parsl when assembling the launcher commands:

- ``PARSL_NUM_RANKS``: Total number of ranks to use for the MPI application
- ``PARSL_NUM_NODES``: Number of nodes to use for the calculation
- ``PARSL_MPI_NODELIST``: List of assigned nodes separated by commas (Eg, NODE1,NODE2)
- ``PARSL_RANKS_PER_NODE``: Number of ranks per node

Limitations
+++++++++++

Support for MPI tasks in HTEX is limited. It is designed for running many multi-node MPI applications within a single
batch job.

#. MPI tasks may not span across nodes from more than one block.
#. Parsl does not correctly determine the number of execution slots per block (`Issue #1647 <https://github.com/Parsl/parsl/issues/1647>`_)
#. The executor uses a Python process per task, which can use a lot of memory (`Issue #2264 <https://github.com/Parsl/parsl/issues/2264>`_)