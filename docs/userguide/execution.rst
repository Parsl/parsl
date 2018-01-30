Execution
=========

Parsl is designed to support arbitrary execution providers (e.g., PCs, clusters, supercomputers) and execution models (e.g., threads, pilot jobs, etc.).
Parsl scripts are independent of execution provider or executor. Instead, the configuration used to run the script tells Parsl how to execute apps on the desired environment.
Parsl provides a high level abstraction, called a *Block*, for describing the resource configuration for a particular app or script.


Execution Providers
-------------------

Execution providers are responsible for managing execution resources. In the simplest case the local computer is used and parallel workload can be forked to individual threads. For larger resources a Local Resource Manager (LRM) is usually used to manage access to resources. For instance, campus clusters and supercomputers generally use LRMs (schedulers) such as Slurm, Torque/PBS, Condor and Cobalt. Clouds, on the other hand, provide API interfaces that allow much more fine-graind composition of an execution environment. Parsl's execution provider abstracts these different resource types and provides a single uniform interface.

Parsl's execution interface is called ``libsubmit`` (`https://github.com/Parsl/libsubmit <https://github.com/Parsl/libsubmit>`_.)--a Python library that abstracts execution environments and provides a common access interface to resources.
Libsubmit defines a simple interface which includes operations such as submission, status, and job management. It currently supports a variety of providers including Amazon Web Services, Azure, and Jetstream clouds as well as Cobolt, Slurm, Torque, and HTCondor LRMs. New execution providers can be easily added by implementing libsubmit's execution provider interface.

Executors
---------

Depending on the execution provider there are a number of ways to then submit workload to that resource. For example, for local execution you could fork threads or use a pilot job manager, for supercomputing resources you may use pilot jobs, various launchers, or even a distributed execution model such as that provided by Swift/T. Parsl supports these models via an *executor*.
Executors represent available compute resources to which tasks can be submitted. An executor initialized with an Execution Provider can dynamically scale with the resources requirements of the workflow.

Parsl currently supports the following executors:

1. **ThreadPoolExecutor**: This executor supports multi-thread execution on local resources.

2. **IPyParallelExecutor**: This executor supports both local and remote execution using a pilot job model. The IPythonParallel controller is deployed locally and IPythonParallel engines are deployed to execution nodes. IPythonParallel then manages the execution of tasks on connected engines.

3. **Swift/TurbineExecutor**: This executor uses the extreme-scale Turbine model to enable distributed task execution across an MPI environment. This executor is typically used on supercomputers.

These executors cover a broad range of execution requirements. As with other Parsl components there is a standard interface (ParslExecutor) that can be implemented to add support for other executors.

Blocks
------

Configuring non-homogeneous resources in a uniform fashion is a challenging aspect of parallel workflow
execution. Parsl uses an abstraction based on resource units called blocks.

A *Block* is a single unit of resources that is obtained from a execution provider.
Within this block are a number of *Nodes*--individual slices of a computing resource.
Parsl can then create *TaskBlocks* within and across (e.g., for MPI jobs) nodes.  A TaskBlock is a
virtual suballocation in which tasks can be launched. Three different examples of block configurations
are shown in the following figures.

1. A single Block comprised of a Node with one TaskBlock :

   .. image:: ../images/N1_T1.png
      :scale: 75%

2. A single Block comprised on a Node with several TaskBlocks. This configuration is
   most suitable for single threaded python/bash applications running with multicore target systems.
   With taskBlocks proportional to the cores available on the system, several apps could execute
   in parallel.

   .. image:: ../images/N1_T4.png
       :scale: 75%

3. A Block comprised of several Nodes and several TaskBlocks. This configuration
   is generally used only by MPI applications and require support from specific
   MPI launchers supported by the target system such as aprun, srun, mpirun, mpiexec etc.

   .. image:: ../images/N4_T2.png


Elasticity
----------

Elasticity 
.. note::
   This feature is available since Parsl 0.4.0
