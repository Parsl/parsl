.. _label-execution:

Execution
=========

Contemporary computing environments may include a wide range of computational platforms or **execution providers**, from laptops and PCs to various clusters, supercomputers, and cloud computing platforms. Different execution providers may require or allow for the use of different **execution models**, such as threads (for efficient parallel execution on a multicore processor), processes, and pilot jobs for running many small tasks on a large parallel system. 

Parsl is designed to abstract these low-level details so that an identical Parsl program can run unchanged on different platforms or across multiple platforms. 
To this end, Parsl uses a configuration file to specify which execution provider(s) and execution model(s) to use.
Parsl provides a high level abstraction, called a *block*, for providing a uniform description of a compute resource irrespective of the specific execution provider.

.. note::
   Refer to :ref:`configuration-section` for information on how to configure the various components described
   below for specific scenarios.

Execution providers
-------------------

Clouds, supercomputers, and local PCs offer vastly different modes of access. 
To overcome these differences, and present a single uniform interface, 
Parsl implements a simple provider abstraction. This
abstraction is key to Parsl's ability to enable scripts to be moved
between resources. The provider interface exposes three core actions: submit a
job for execution (e.g., sbatch for the Slurm resource manager), 
retrieve the status of an allocation (e.g., squeue), and cancel a running
job (e.g., scancel). Parsl implements providers for local execution
(fork), for various cloud platforms using cloud-specific APIs, and
for clusters and supercomputers that use a Local Resource Manager
(LRM) to manage access to resources, such as Slurm and HTCondor.

Each provider implementation may allow users to specify additional parameters for further configuration. Parameters are generally mapped to LRM submission script or cloud API options.
Examples of LRM-specific options are partition, wall clock time,
scheduler options (e.g., #SBATCH arguments for Slurm), and worker
initialization commands (e.g., loading a conda environment). Cloud
parameters include access keys, instance type, and spot bid price

Parsl currently supports the following providers:

1. `parsl.providers.LocalProvider`: The provider allows you to run locally on your laptop or workstation.
2. `parsl.providers.SlurmProvider`: This provider allows you to schedule resources via the Slurm scheduler.
3. `parsl.providers.CondorProvider`: This provider allows you to schedule resources via the Condor scheduler.
4. `parsl.providers.GridEngineProvider`: This provider allows you to schedule resources via the GridEngine scheduler.
5. `parsl.providers.TorqueProvider`: This provider allows you to schedule resources via the Torque scheduler.
6. `parsl.providers.AWSProvider`: This provider allows you to provision and manage cloud nodes from Amazon Web Services.
7. `parsl.providers.GoogleCloudProvider`: This provider allows you to provision and manage cloud nodes from Google Cloud.
8. `parsl.providers.KubernetesProvider`: This provider allows you to provision and manage containers on a Kubernetes cluster.
9. `parsl.providers.LSFProvider`: This provider allows you to schedule resources via IBM's LSF scheduler.




.. note::
   Refer to :ref:`configuration-section` for information on how to configure these executors.


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

   .. image:: ../../images/N1_T1.png
      :scale: 75%

2. A single block with one node executing several tasks. This configuration is
   most suitable for single threaded apps running on multicore target systems.
   The number of tasks executed concurrently is proportional to the number of cores available on the system.

   .. image:: ../../images/N1_T4.png
       :scale: 75%

3. A block comprised of several nodes and executing several tasks, where a task can span multiple nodes. This configuration
   is generally used by MPI applications. Starting a task requires using a specific
   MPI launcher that is supported on the target system (e.g., aprun, srun, mpirun, mpiexec).
   The `MPI Apps <mpi_apps.html>`_ documentation page describes how to configure Parsl for this case.

   .. image:: ../../images/N4_T2.png

The configuration options for specifying the shape of each block are:

1. ``workers_per_node``: Number of workers started per node, which corresponds to the number of tasks that can execute concurrently on a node.
2. ``nodes_per_block``: Number of nodes requested per block.
