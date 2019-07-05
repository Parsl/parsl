Execution
=========

Parsl scripts can be executed on different execution providers (e.g., PCs, clusters, supercomputers) and using different execution models (e.g., threads, pilot jobs, etc.).
Parsl separates the code from the configuration that specifies which execution provider(s) and executor(s) to use.
Parsl provides a high level abstraction, called a *block*, for providing a uniform description of a resource configuration irrespective of the specific execution provider.

.. note::
   Refer to :ref:`configuration-section` for information on how to configure the various subsystems described
   below for your workflow's resource requirements.

Execution providers
-------------------

Execution providers are responsible for managing execution resources. In the simplest case a PC could be used for execution. For larger resources a Local Resource Manager (LRM) is usually used to manage access to resources. For instance, campus clusters and supercomputers generally use LRMs (schedulers) such as Slurm, Torque/PBS, HTCondor and Cobalt. Clouds, on the other hand, provide APIs that allow more fine-grained composition of an execution environment. Parsl's execution provider abstracts these different resource types and provides a single uniform interface.

Parsl currently supports the following providers:

1. `LocalProvider`: The provider allows you to run locally on your laptop or workstation.
2. `CobaltProvider`: This provider allows you to schedule resources via the Cobalt scheduler.
3. `SlurmProvider`: This provider allows you to schedule resources via the Slurm scheduler.
4. `CondorProvider`: This provider allows you to schedule resources via the Condor scheduler.
5. `GridEngineProvider`: This provider allows you to schedule resources via the GridEngine scheduler.
6. `TorqueProvider`: This provider allows you to schedule resources via the Torque scheduler.
7. `AWSProvider`: This provider allows you to provision and manage cloud nodes from Amazon Web Services.
8. `GoogleCloudProvider`: This provider allows you to provision and manage cloud nodes from Google Cloud.
9. `JetstreamProvider`: This provider allows you to provision and manage cloud nodes from Jetstream (NSF Cloud).
10. `KubernetesProvider`: This provider allows you to provision and manage containers on a Kubernetes cluster.

Executors
---------

Depending on the execution provider there are a number of ways to execute workloads on that resource. For example, for local execution a thread pool could be used, for supercomputers pilot jobs or various launchers could be used. Parsl supports these models via an *executor* model.
Executors represent a particular method via which tasks can be executed. As described below, an executor initialized with an execution provider can dynamically scale with the resource requirements of the workflow.

Parsl currently supports the following executors:

1. `ThreadPoolExecutor`: This executor supports multi-thread execution on local resources.

2. `IPyParallelExecutor`: This executor supports both local and remote execution using a pilot job model. The IPythonParallel controller is deployed locally and IPythonParallel engines are deployed to execution nodes. IPythonParallel then manages the execution of tasks on connected engines.

3. `HighThroughputExecutor`: [**Beta**] The HighThroughputExecutor is designed as a replacement for the IPyParallelExecutor. Implementing hierarchical scheduling and batching, the HighThroughputExecutor consistently delivers high throughput task execution on the order of 1000 Nodes.

4. `ExtremeScaleExecutor`: [**Beta**] The ExtremeScaleExecutor uses `mpi4py <https://mpi4py.readthedocs.io/en/stable/>` to scale over 4000+ nodes. This executor is typically used for executing on Supercomputers.

5. `Swift/TurbineExecutor`: [**Deprecated**] This executor uses the extreme-scale `Turbine <http://swift-lang.org/Swift-T/index.php>`_ model to enable distributed task execution across an MPI environment. This executor is typically used on supercomputers.

These executors cover a broad range of execution requirements. As with other Parsl components there is a standard interface (ParslExecutor) that can be implemented to add support for other executors.

.. note::
   Refer to :ref:`configuration-section` for information on how to configure these executors.


Launchers
---------

On many traditional batch systems, the user is expected to request a large number of nodes and launch tasks using a system such as `srun <https://slurm.schedmd.com/srun.html>`_ (for slurm), `aprun <https://cug.org/5-publications/proceedings_attendee_lists/2006CD/S06_Proceedings/pages/Authors/Karo-4C/Karo_alps_paper.pdf>`_ (for crays), `mpirun <https://www.open-mpi.org/doc/v2.0/man1/mpirun.1.php>`_ etc.
Launchers are responsible for abstracting these different task-launch systems to start the appropriate number of workers across cores and nodes. Parsl currently supports the following set of launchers:

1. `SrunLauncher`: Srun based launcher for Slurm based systems.
2. `AprunLauncher`: Aprun based launcher for Crays.
3. `SrunMPILauncher`: Launcher for launching MPI applications with Srun.
4. `GnuParallelLauncher`: Launcher using GNU parallel to launch workers across nodes and cores.
5. `MpiExecLauncher`: Uses Mpiexec to launch.
6. `SimpleLauncher`: The launcher default to a single worker launch.
7. `SingleNodeLauncher`: This launcher launches ``workers_per_node`` count workers on a single node.


Blocks
------

Providing a uniform representation of heterogeneous resources
is one of the most difficult challenges for parallel execution.
Parsl provides an abstraction based on resource units called *blocks*.
A block is a single unit of resources that is obtained from an execution provider.
Within a block are a number of nodes. Parsl can then execute *tasks* (instances of apps)
within and across (e.g., for MPI jobs) nodes.
Three different examples of block configurations are shown below.

1. A single block comprised of a node executing one task:

   .. image:: ../images/N1_T1.png
      :scale: 75%

2. A single block comprised on a node executing several tasks. This configuration is
   most suitable for single threaded apps running on multicore target systems.
   The number of tasks executed concurrently is proportional to the number of cores available on the system.

   .. image:: ../images/N1_T4.png
       :scale: 75%

3. A block comprised of several nodes and executing several tasks, where a task can span multiple nodes. This configuration
   is generally used by MPI applications. Starting a task requires using a specific
   MPI launcher that is supported on the target system (e.g., aprun, srun, mpirun, mpiexec).

   .. image:: ../images/N4_T2.png


.. _label-elasticity:

Elasticity
----------

Parsl implements a dynamic dependency graph in which the
graph is extended as new tasks are enqueued and completed.
As the Parsl script executes the workflow, new tasks are added
to a queue for execution. Tasks are then executed asynchronously
when their dependencies are met.
Parsl uses the selected executor(s) to manage task
execution on the execution provider(s).
The execution resources, like the workflow, are not static:
they can be elastically scaled to handle the variable workload generated by the
workflow.

During execution Parsl does not
know the full "width" of a particular workflow a priori.
Further, as a workflow executes, the needs of the tasks
may change, as well as the capacity available
on execution providers. Thus, Parsl can
elastically scale the resources it is using.
To do so, Parsl includes an extensible flow control system that
monitors outstanding tasks and available compute capacity.
This flow control monitor, which can be extended or implemented by users,
determines when to trigger scaling (in or out) events to match
workflow needs.

The animated diagram below shows how blocks are elastically
managed within an executor. The script configuration for an executor
defines the minimum, maximum, and initial number of blocks to be used.

.. image:: parsl_scaling.gif

The configuration options for specifying elasticity bounds are:

1. ``min_blocks``: Minimum number of blocks to maintain per executor.
2. ``init_blocks``: Initial number of blocks to provision at initialization of workflow.
3. ``max_blocks``: Maximum number of blocks that can be active per executor.

The configuration options for specifying the shape of each block are:

1. ``workers_per_node``: Number of workers started per node, which corresponds to the number of tasks that can execute concurrently on a node.
2. ``nodes_per_block``: Number of nodes requested per block.

Parallelism
^^^^^^^^^^^

Parsl provides a simple user-managed model for controlling elasticity.
It allows users to prescribe the minimum
and maximum number of blocks to be used on a given executor as well as
a parameter (*p*) to control the level of parallelism. Parallelism
is expressed as the ratio of task execution capacity and the sum of running tasks
and available tasks (tasks with their dependencies met, but waiting for execution).
A parallelism value of 1 represents aggressive scaling where as many resources
as possible are used; parallelism close to 0 represents the opposite situation in which
as few resources as possible (i.e., min_blocks) are used. By selecting a fraction between 0 and 1,
the aggressiveness in provisioning resources can be controlled.

For example:

- When p = 0: Use the fewest resources possible.

.. code:: python

   if active_tasks == 0:
       blocks = min_blocks
   else:
       blocks = max(min_blocks, 1)

- When p = 1: Use as many resources as possible.

.. code-block:: python

   blocks = min(max_blocks,
                ceil((running_tasks + available_tasks) / (workers_per_node * nodes_per_block))

- When p = 1/2: Stack up to 2 tasks before overflowing and requesting a new block.


Configuration
^^^^^^^^^^^^^

The example below shows how elasticity and parallelism can be configured. Here, a local IPythonParallel
environment is used with a minimum of 1 block and a maximum of 2 blocks, where each block may host
up to 2 tasks. Parallelism of 0.5 means that when more than 2 * the total task capacity are queued a new
block will be requested (up to 2 possible blocks). An example :class:`~parsl.config.Config` is:

.. code:: python

    from parsl.config import Config
    from libsubmit.providers.local.local import Local
    from parsl.executors.ipp import IPyParallelExecutor

    config = Config(
        executors=[
            IPyParallelExecutor(
                label='local_ipp',
                workers_per_node=2,
                provider=Local(
                    min_blocks=1,
                    init_blocks=1,
                    max_blocks=4,
                    nodes_per_block=1,
                    parallelism=0.5
                )
            )
        ]
    )

The animated diagram below illustrates the behavior of this executor.
In the diagram, the tasks are allocated to the first block, until
5 tasks are submitted. At this stage, as more than double the available
task capacity is used, Parsl provisions a new block for executing the remaining
tasks.

.. image:: parsl_parallelism.gif


Multi-executor
--------------

Parsl supports the definition of any number of executors in the configuration,
as well as specifying which of these executors can execute specific apps.

The common scenarios for this feature are:

* The workflow has an initial simulation stage that runs on the compute heavy
  nodes of an HPC system followed by an analysis and visualization stage that
  is better suited for GPU nodes.
* The workflow follows a repeated fan-out, fan-in model where the long running
  fan-out tasks are computed on a cluster and the quick fan-in computation is
  better suited for execution using threads on the login node.
* The workflow includes apps that wait and evaluate the results of a
  computation to determine whether the app should be relaunched.
  Only apps running on threads may launch apps. Often, science simulations
  have stochastic behavior and may terminate before completion.
  In such cases, having a wrapper app that checks the exit code
  and determines whether or not the app has completed successfully can
  be used to automatically re-execute the app (possibly from a
  checkpoint) until successful completion.


The following code snippet shows how executors can be specified in the app decorator.

.. code-block:: python

     #(CPU heavy app) (CPU heavy app) (CPU heavy app) <--- Run on compute queue
     #      |                |               |
     #    (data)           (data)          (data)
     #       \               |              /
     #       (Analysis and visualization phase)         <--- Run on GPU node

     # A mock molecular dynamics simulation app
     @bash_app(executors=["Theta.Phi"])
     def MD_Sim(arg, outputs=[]):
         return "MD_simulate {} -o {}".format(arg, outputs[0])

     # Visualize results from the mock MD simulation app
     @bash_app(executors=["Cooley.GPU"])
     def visualize(inputs=[], outputs=[]):
         bash_array = " ".join(inputs)
         return "viz {} -o {}".format(bash_array, outputs[0])
