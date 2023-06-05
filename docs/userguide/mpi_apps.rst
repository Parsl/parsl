MPI Apps
========

.. note::

    Parsl support for MPI Apps is being re-engineered.
    We describe the best practices with today's Parsl.
    Join our Slack if you want to steer Parsl's future.

MPI applications run multiple copies of a program that complete a single task by
coordinating using messages passed within or across nodes.
Starting MPI application requires invoking a "launcher" code (e.g., ``mpiexec``) from one node
with options that define how the copies of a program should be distributed to others.
The need to call a launcher from a specific node requires a special configuration of Parsl `Apps <apps.html>`_
and `Execution <execution.html>`_ environments to run Apps which use MPI.

HTEx and MPI Tasks
------------------

The :class:`~parsl.executors.high_throughput.executor.HighThroughputExecutor` (HTEx) is the default execution provider
available through Parsl.
Parsl Apps which invoke MPI code require a dedicated HTEx configured such that every Parsl app
will have access to any of the nodes available within a block,
and Apps that will invoke the MPI launcher with the correct settings.

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

Configure the executor to launch a number of workers equal to the number of MPI tasks per block.
First set the ``max_workers`` to the number of MPI Apps per block.
then set ``cores_per_worker=1e-6`` to prevent HTEx from reducing the number of workers
if you request more workers than cores.

If you plan to only launch one App per block, you are done!

If not, the executor may need to first partition nodes into distinct groups for each MPI task.
Partitioning is necessary if your MPI launcher does not automatically ensure MPI task gets exclusive nodes.
Resources vary in how the list of available nodes is provided,
but they typically are found as a "hostfile" referenced in an environment variable (e.g., ``PBS_NODEFILE``).
We recommend splitting this hostfile for the host block into hostfiles for each worker
by adding code like the following to your ``worker_init``:

.. code-block:: bash

    NODES_PER_TASK=2
    mkdir -p /tmp/hostfiles/
    split --lines=$NODES_PER_TASK -d --suffix-length=2 $PBS_NODEFILE /tmp/hostfiles/hostfile.

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

    # Prepare the host files
    mkdir -p /tmp/hostfiles/
    split --lines={nodes_per_task} -d --suffix-length=2 $PBS_NODEFILE /tmp/hostfiles/hostfile.""",
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

In the easiest case (i.e., single MPI task per block), write the MPI launcher options in the string returned by
the bash app or as part of a subprocess call from a Python app.


.. code-block:: python

    @bash_app
    def echo_hello(n: int, stderr='std.err', stdout='std.out'):
        return f'mpiexec -n {n} --ppn 1 hostname'

Complications arise when running more than one MPI task per block,
and the MPI launcher does not automatically spread jobs across nodes.
In this case, use the ``PARSL_WORKER_RANK`` environment variable
set by HTEx to select the correct hostfile:


.. code-block:: python

    @bash_app
    def echo_hello(n: int, stderr='std.err', stdout='std.out'):
        return (f'mpiexec -n {n} --ppn 1 '
                '--hostfile /tmp/hostfiles/local_hostfile.`printf %02d $PARSL_WORKER_RANK` '
                'hostname')

.. note::

    Use these Apps for testing! Submit many task using one of these Apps then ensure
    the number of unique nodes in the "std.out" files
    is the same as the number per block.


Limitations
+++++++++++

Support for MPI tasks in HTEx is limited:

#. All tasks must use the same number of nodes, which is fixed when creating the executor.
#. MPI tasks may not span across nodes from more than one block.
#. Parsl does not correctly determine the number of execution slots per block (`Issue #1647 <https://github.com/Parsl/parsl/issues/1647>`_)
#. The executor uses a Python process per task, which can use a lot of memory (`Issue #2264 <https://github.com/Parsl/parsl/issues/2264>`_)
