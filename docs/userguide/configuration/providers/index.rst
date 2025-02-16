.. _label-execution:

Providers
=========

The **Provider** defines how to acquire then start Parsl workers on remote resources.
Providers may, for example, interface with queuing system of a computer cluster,
provision virtual machines from a cloud computing vendor,
or manage containers via Kubernetes.

Define a Provider by first selecting the `appropriate interface <providers.html>`_,
specifying how to `launch workers on newly-acquired resources <launchers.html>`_
then configuring how Parsl will adjust `amount of resources according to demand <elasticity.html>`_.

.. toctree::
   :maxdepth: 2

   providers
   launchers
   elasticity

---------

Many LRMs offer mechanisms for spawning applications across nodes 
inside a single job and for specifying the
resources and task placement information needed to execute that
application at launch time. Common mechanisms include
`srun <https://slurm.schedmd.com/srun.html>`_ (for Slurm), 
`aprun <https://cug.org/5-publications/proceedings_attendee_lists/2006CD/S06_Proceedings/pages/Authors/Karo-4C/Karo_alps_paper.pdf>`_ (for Crays), and `mpirun <https://www.open-mpi.org/doc/v2.0/man1/mpirun.1.php>`_ (for MPI). 
Thus, to run Parsl programs on such systems, we typically want first to 
request a large number of nodes and then to *launch* "pilot job" or 
**worker** processes using the system launchers. 
Parsl's Launcher abstraction enables Parsl programs
to use these system-specific launcher systems to start workers across 
cores and nodes.

Parsl currently supports the following set of launchers:

1. `parsl.launchers.SrunLauncher`: Srun based launcher for Slurm based systems.
2. `parsl.launchers.AprunLauncher`: Aprun based launcher for Crays.
3. `parsl.launchers.SrunMPILauncher`: Launcher for launching MPI applications with Srun.
4. `parsl.launchers.GnuParallelLauncher`: Launcher using GNU parallel to launch workers across nodes and cores.
5. `parsl.launchers.MpiExecLauncher`: Uses Mpiexec to launch.
6. `parsl.launchers.SimpleLauncher`: The launcher default to a single worker launch.
7. `parsl.launchers.SingleNodeLauncher`: This launcher launches ``workers_per_node`` count workers on a single node.

Additionally, the launcher interface can be used to implement specialized behaviors
in custom environments (for example, to
launch node processes inside containers with customized environments). 
For example, the following launcher uses Srun to launch ``worker-wrapper``, passing the
command to be run as parameters to ``worker-wrapper``. It is the responsibility of ``worker-wrapper``
to launch the command it is given inside the appropriate environment.

.. code:: python

   class MyShifterSRunLauncher:
       def __init__(self):
           self.srun_launcher = SrunLauncher()

       def __call__(self, command, tasks_per_node, nodes_per_block):
           new_command="worker-wrapper {}".format(command)
           return self.srun_launcher(new_command, tasks_per_node, nodes_per_block)

Blocks
------

One challenge when making use of heterogeneous 
execution resource types is the need to provide a uniform representation of
resources. Consider that single requests on clouds return individual
nodes, clusters and supercomputers provide batches of nodes, grids
provide cores, and workstations provide a single multicore node

Parsl defines a resource abstraction called a *block* as the most basic unit
of resources to be acquired from a provider. A block contains one
or more nodes and maps to the different provider abstractions. In
a cluster, a block corresponds to a single allocation request to a
scheduler. In a cloud, a block corresponds to a single API request
for one or more instances. 
Parsl can then execute *tasks* (instances of apps)
within and across (e.g., for MPI jobs) nodes within a block.
Blocks are also used as the basis for
elasticity on batch scheduling systems (see Elasticity below).
Three different examples of block configurations are shown below.

1. A single block comprised of a node executing one task:

   .. image:: img/N1_T1.png
      :scale: 75%

2. A single block with one node executing several tasks. This configuration is
   most suitable for single threaded apps running on multicore target systems.
   The number of tasks executed concurrently is proportional to the number of cores available on the system.

   .. image:: img/N1_T4.png
       :scale: 75%

3. A block comprised of several nodes and executing several tasks, where a task can span multiple nodes. This configuration
   is generally used by MPI applications. Starting a task requires using a specific
   MPI launcher that is supported on the target system (e.g., aprun, srun, mpirun, mpiexec).
   The `MPI Apps <mpi_apps.html>`_ documentation page describes how to configure Parsl for this case.

   .. image:: img/N4_T2.png

The configuration options for specifying the shape of each block are:

1. ``workers_per_node``: Number of workers started per node, which corresponds to the number of tasks that can execute concurrently on a node.
2. ``nodes_per_block``: Number of nodes requested per block.
