MPI Apps
========

.. note::

    Parsl's support for MPI Apps described below is available
    in Parsl version vDD.MM.YYYY and newer.
    Join our Slack if you want to steer Parsl's future.

MPI applications run multiple copies of a program that complete a single task by
coordinating using messages passed within or across nodes.
Starting MPI application requires invoking a "launcher" code (e.g., ``mpiexec``) from one node
with options that define how the copies of a program should be distributed to others. Parsl simplifies this by
composing the "launcher" command from the resources specified at the time each app is invoked.

The broad strokes of a complete solution involves the following components:

1. Configuring the :class:`~parsl.executors.high_throughput.executor.HighThroughputExecutor` with:
    ``enable_mpi_mode=True``
2. Specify the provider that matches your cluster, (eg. user ``SlurmProvider`` for Slurm clusters)
3. Use the :class:`~parsl.launchers.SimpleLauncher`
4. Specify resources required by the application via ``resource_specification`` as shown below:

.. code-block:: python

    @bash_app
    def lammps_mpi_application(infile: File, resource_specification: Dict):
        # PARSL_DEFAULT_PREFIX will resolve to `mpiexec -n 4 -ppn 2 -hosts NODE001,NODE002`
        return f"$PARSL_DEFAULT_PREFIX lmp_mpi -in {infile.filepath}"

    # Resources in terms of nodes and how ranks are to be distributed are set on a per app
    # basis via the resource_spec dictionary.
    resource_spec = {
        "NUM_NODES" = 2,
        "RANKS_PER_NODE" = 2,
    }
    future = lammps_mpi_application(File('in.file'), resource_specification=resource_spec)



HTEX and MPI Tasks
------------------

The :class:`~parsl.executors.high_throughput.executor.HighThroughputExecutor` (HTEX) is the
default executor available through Parsl.
Parsl Apps which invoke MPI code require MPI specific configuration such that:

1. All workers are started on the lead-node (mom-node in case of Crays)
2. Resource requirements of Apps are propagated to workers who provision the required number of nodes from within the batch job.

Configuring the Provider
++++++++++++++++++++++++

Parsl must be configured to deploy workers on exactly one node per block.
This part is simple.
Instead of defining `a launcher <execution.html#launchers>`_ which will
place an executor on each node in the block, simply use the :class:`~parsl.launchers.SimpleLauncher`.

It is also necessary to specify the desired number of blocks for the executor.
Parsl cannot determine the number of blocks needed to run a set of MPI Tasks,
so they must bet set explicitly (see `Issue #1647 <https://github.com/Parsl/parsl/issues/1647>`_).
The easiest route is to set the ``max_blocks`` and ``min_blocks`` of the provider
to the desired number of blocks.

Configuring the Executor
++++++++++++++++++++++++

Here are the steps for configuring the executor:

1. Set ``HighThroughputExecutor(enable_mpi_mode=True)``
2. Set the ``max_workers`` to the number of MPI Apps per block.
3. Set ``cores_per_worker=1e-6`` to prevent HTEx from reducing the number of workers if you request more workers than cores.


Example Configuration
~~~~~~~~~~~~~~~~~~~~~

An example for an executor which runs MPI tasks on ALCF's Polaris supercomputer (HPE Apollo, PBSPro resource manager)
is below.

.. code-block:: python

    nodes_per_task = 2
    tasks_per_block = 16
    config = Config(
        executors=[
            HighThroughputExecutor(
                label='mpiapps',
                enable_mpi_mode=True,
                address=address_by_hostname(),
                start_method="fork",  # Needed to avoid interactions between MPI and os.fork
                max_workers=tasks_per_block,
                cores_per_worker=1e-6, # Prevents
                provider=PBSProProvider(
                    account="ACCT",
                    worker_init=f"""
    # Prepare the computational environment
    module swap PrgEnv-nvhpc PrgEnv-gnu
    module load conda
    module list
    conda activate /lus/grand/projects/path/to/env
    cd $PBS_O_WORKDIR

    # Print the environment details for debugging
    hostname
    pwd
    which python
                    """,
                    walltime="6:00:00",
                    launcher=SimpleLauncher(),  # Launches only a single executor per block
                    select_options="ngpus=4",
                    nodes_per_block=nodes_per_task * tasks_per_block,
                    min_blocks=0,
                    max_blocks=1,
                    cpus_per_node=64,
                ),
            ),
        ]
    )


Writing MPI-Compatible Apps
++++++++++++++++++++++++++++

The `App <apps.html>`_ can be either a Python or Bash App which invokes the MPI application.

For multi-node MPI applications, especially when running multiple applications within a single batch job,
it is important to specify the resource requirements for the app so that the Parsl worker can provision
the appropriate resources before the application starts. For eg, your Parsl script might contain a molecular
dynamics application that requires 8 ranks over 1 node for certain inputs and 32 ranks over 4 nodes for some
depending on the size of the molecules being simulated. By specifying resources via ``resource_specification``,
parsl workers will provision the requested resources and then compose MPI launch command prefixes
(Eg: ``mpiexec -n <ranks> -ppn <ranks_per_node> -hosts <node1..nodeN>``). These launch command prefixes are
shared with the app via environment variables.

.. code-block:: python

    @bash_app
    def echo_hello(n: int, stderr='std.err', stdout='std.out', resource_specification: Dict):
        return f'$PARSL_MPIEXEC_PREFIX hostname'

    # Alternatively, you could also use the resource_specification to compose a launch
    # command using env vars set by Parsl from the resource_specification like this:
    @bash_app
    def echo_something(n: int, stderr='std.err', stdout='std.out', resource_specification: Dict):
        total_ranks = os.environ("")
        return f'aprun -N $PARSL_RANKS_PER_NODE -n hostname'


All key-value pairs set in the resource_specification are exported to the application via env vars, for eg.
``resource_specification = {'foo': 'bar'} `` will set the env var `foo` to `bar` in the application's env.
However, the following options are **required** for MPI applications :

.. code-block:: python

    resource_specification = {
      'NUM_NODES': <int>,        # Number of nodes required for the application instance
      'RANKS_PER_NODE': <int>,   # Number of Ranks / application elements to be launched per node
    }

    # The above are made available in the worker env vars:
    # echo $PARSL_NUM_NODES, $PARSL_RANKS_PER_NODE

When the above are supplied, the following launch command prefixes are set:

.. code-block:: python

    PARSL_MPIEXEC_PREFIX: mpiexec launch command which works for a large number of batch systems especially PBS systems
    PARSL_SRUN_PREFIX: srun launch command for Slurm based clusters
    PARSL_APRUN_PREFIX: aprun launch command prefix for some Cray machines
    PARSL_DEFAULT_PREFIX: Parsl tries to identify the batch system and picks an appropriate launcher prefix

Limitations
+++++++++++

Support for MPI tasks in HTEX is limited. It is designed for running many multi-node MPI applications within a single
batch job.

#. MPI tasks may not span across nodes from more than one block.
#. Parsl does not correctly determine the number of execution slots per block (`Issue #1647 <https://github.com/Parsl/parsl/issues/1647>`_)
#. The executor uses a Python process per task, which can use a lot of memory (`Issue #2264 <https://github.com/Parsl/parsl/issues/2264>`_)
