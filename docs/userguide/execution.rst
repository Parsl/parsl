Execution
=========

Parsl is designed to support arbitrary execution providers (e.g., PCs, clusters, supercomputers) as well as arbitrary execution models (e.g., threads, pilot jobs, etc.). That is, Parsl scripts are independent of execution provider or executor. Instead, the configuration used to run the script tells Parsl how to execute apps on the desired environment.
Parsl provides a high level abstraction, called a *Block*, for providing a uniform description of a resource configuration for a particular app or script.


Execution Providers
-------------------

Execution providers are responsible for managing execution resources. In the simplest case the local computer is used and parallel tasks are forked to individual threads. For larger resources a Local Resource Manager (LRM) is usually used to manage access to resources. For instance, campus clusters and supercomputers generally use LRMs (schedulers) such as Slurm, Torque/PBS, Condor and Cobalt. Clouds, on the other hand, provide APIs that allow more fine-grained composition of an execution environment. Parsl's execution provider abstracts these different resource types and provides a single uniform interface.

Parsl's execution interface is called ``libsubmit`` (`https://github.com/Parsl/libsubmit <https://github.com/Parsl/libsubmit>`_) -- a Python library that provides a common interface to execution providers.
Libsubmit defines a simple interface which includes operations such as submission, status, and job management. It currently supports a variety of providers including Amazon Web Services, Azure, and Jetstream clouds as well as Cobalt, Slurm, Torque, GridEngine, and HTCondor LRMs. New execution providers can be easily added by implementing libsubmit's execution provider interface.

Executors
---------

Depending on the execution provider there are a number of ways to then submit workload to that resource. For example, for local execution threads or pilot jobs may be used, for supercomputing resources pilot jobs, various launchers, or even a distributed execution model such as that provided by Swift/T may be used. Parsl supports these models via an *executor* model.
Executors represent a particular method via which tasks can be executed. As described below, an executor initialized with an execution provider can dynamically scale with the resources requirements of the workflow.

Parsl currently supports the following executors:

1. **ThreadPoolExecutor**: This executor supports multi-thread execution on local resources.

2. **IPyParallelExecutor**: This executor supports both local and remote execution using a pilot job model. The IPythonParallel controller is deployed locally and IPythonParallel engines are deployed to execution nodes. IPythonParallel then manages the execution of tasks on connected engines.

3. **Swift/TurbineExecutor**: This executor uses the extreme-scale `Turbine <http://swift-lang.org/Swift-T/index.php>`_ model to enable distributed task execution across an MPI environment. This executor is typically used on supercomputers.

These executors cover a broad range of execution requirements. As with other Parsl components there is a standard interface (ParslExecutor) that can be implemented to add support for other executors.

Blocks
------

Providing a uniform representation of heterogeneous resources 
is one of the most difficult challenges for parallel execution. 
Parsl provides an abstraction based on resource units called *blocks*.
A block is a single unit of resources that is obtained from an execution provider.
Within a block are a number of nodes. Parsl can then create *TaskBlocks* 
within and across (e.g., for MPI jobs) nodes. 
A TaskBlock is a virtual suballocation in which individual tasks can be launched. 
Three different examples of block configurations are shown below.

1. A single Block comprised of a Node with one TaskBlock :

   .. image:: ../images/N1_T1.png
      :scale: 75%

2. A single Block comprised on a Node with several TaskBlocks. This configuration is
   most suitable for single threaded python/bash applications running on multicore target systems.
   With TaskBlocks proportional to the cores available on the system, apps can execute
   in parallel.

   .. image:: ../images/N1_T4.png
       :scale: 75%

3. A Block comprised of several Nodes and several TaskBlocks. This configuration
   is generally used by MPI applications and requires support from specific
   MPI launchers supported by the target system (e.g., aprun, srun, mpirun, mpiexec).

   .. image:: ../images/N4_T2.png


.. _label-elasticity:

Elasticity
----------


.. note::
   This feature is available from Parsl ``v0.4.0``

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

As Parsl manages a dynamic dependency graph, it does not
know the full "width" of a particular workflow a priori.
Further, as a workflow executes, the needs of the tasks
may change, as well as the capacity available
on execution providers. Thus, Parsl must
elastically scale the resources it is using. 
To do so, Parsl includes an extensible flow control system that 
monitors outstanding tasks and available compute capacity. 
This flow control monitor, which can be extended or implemented by users, 
determines when to trigger scaling (in or out) events to match
workflow needs.

The animated diagram below shows how blocks are elastically 
managed within a site. The script configuration for a site
defines the minimum, maximum, and initial number of blocks to be used. 
Depending on workload, Parsl provisions or deprovisions blocks. 

.. image:: parsl_scaling.gif

The configuration options for specifying elasticity bounds are:

1. ``minBlocks``: Minimum number of blocks to maintain per site.
2. ``initBlocks``: Initial number of blocks to provision at initialization of workflow.
3. ``maxBlocks``: Maximum number of blocks that can be active at a site from one workflow.


Parallelism
^^^^^^^^^^^

Parsl provides a simple user-managed model for controlling elasticity. 
It allows users to prescribe the minimum
and maximum number of blocks to be used on a given site as well as 
a parameter (*p*) to control the level of parallelism. Parallelism
is expressed as the ratio of TaskBlocks to active tasks. 
Recall that each TaskBlock is capable of executing a single task at any given time. 
A parallelism value of 1 represents aggressive scaling where as many resources 
as possible are used; parallelism close to 0 represents the opposite situation in which
as few resources as possible (i.e., minBlocks) are used.

For example:

- When p = 0: Use the fewest resources possible. Infinite tasks are stacked per TaskBlock.

.. code:: python

    if active_tasks == 0:
        blocks = minBlocks
    else:
        blocks = max(minBlocks, 1)

- When p = 1: Use as many resources as possible. One task is stacked per TaskBlock.

.. code-block:: python

     blocks = min(maxBlocks,
                   ceil(active_tasks / TaskBlocks))

- When p = 1/2: Stack up to 2 tasks per TaskBlock before overflowing and requesting a new block.


Configuration
^^^^^^^^^^^^^

The example below shows how elasticity and parallelism can be configured. Here, a local IPythonParallel
environment is used with a minimum of 1 block and a maximum of 2 blocks, where each block may host
up to 4 TaskBlocks. Parallelism of 0.5 means that when more than 2 tasks are queue per TaskBlock a new
block will be requested (up to two possible blocks).

.. code:: python

    localIPP = {
        "sites": [
            {"site": "Local_IPP",
             "auth": {
                 "channel": None,
             },
             "execution": {
                 "executor": "ipp",
                 "provider": "local",
                 "block": {
                     "minBlocks" : 1,
                     "maxBlocks" : 2, # Shape of the blocks
                     "initBlocks": 1,
                     "TaskBlocks": 4, # Number of workers in a block
                     "parallelism" : 0.5
                 }
             }
            }]
    }

The animated diagram below illustrates the behavior of this site. 
In the diagram, the tasks are allocated to the first block, until 
5 tasks are submitted. At this stage, as more than 2 tasks are waiting
per TaskBlock, Parsl provisions a new block for executing the remaining
tasks. 

.. image:: parsl_parallelism.gif


Multi-Site
----------

.. note::
   This feature is available from Parsl 0.4.0

Parsl supports the definition of any number of execution sites in the configuration,
as well as specifying which of these sites could execute specific apps.

The common scenarios for this feature are:

* The workflow has an initial simulation stage that runs on the compute heavy
  nodes of an HPC system followed by an analysis and visualization stage that
  is better suited for the GPU nodes.
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


Here's a code snippet that shows how sites can be specified in the ``App`` decorator.

.. code-block:: python

     #(CPU Heavy app) (CPU Heavy app) (CPU Heavy app) <--- Run on compute queue
     #      |                |               |
     #    (data)           (data)          (data)
     #       \               |              /
     #       (Analysis & Visualization phase)         <--- Run on GPU node

     # A mock Molecular Dynamics simulation app
     @App('bash', dfk, sites=["Theta.Phi"])
     def MD_Sim(arg, outputs=[]):
         return "MD_simulate {} -o {}".format(arg, outputs[0])

     # Visualize results from the mock MD simulation app
     @App('bash', dfk, sites=["Cooley.GPU"])
     def Visualize(inputs=[], outputs=[]):
         bash_array = " ".join(inputs)
         return "viz {} -o {}".format(bash_array, outputs[0])
