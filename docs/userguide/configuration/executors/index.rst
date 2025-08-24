Executors
---------

Parsl provides many Executors to handle a wide variety of workloads.

Workloads that execute a small number of tasks which run locally
benefit from the minimal configuration required for :class:`~parsl.executors.ThreadPoolExecutor`,
whereas those which require thousands of nodes should use the
:class:`~parsl.executors.HighThroughputExecutor`.
These executors include:

#. :class:`~parsl.executors.HighThroughputExecutor` (HTEx): The best starting choice for Parsl applications.
   HTEx deploys many identical Python workers as separate processes across multiple compute nodes.
   Implements a hierarchical scheduling strategy and can scale to thousands of workers in many applications..

#. :class:`~parsl.executors.taskvine.TaskVineExecutor` (TVEx): An executor which supports performance features including:
   resizing the resources pinned to workers,
   smart caching and sharing of large files between tasks and compute nodes,
   and automatically sharing Python environments between tasks.
   Scales to tens of thousands of cores.
   Built using `TaskVine <https://ccl.cse.nd.edu/software/taskvine/>`_.

#. :class:`~parsl.executors.WorkQueueExecutor` (WQEx): An Executor which supports dynamically adjusting the resources
   allocated to each worker, built using `Work Queue <http://ccl.cse.nd.edu/software/workqueue/>`_.
   WQEx scales to tens of thousands of cores.


#. :class:`~parsl.executors.GlobusComputeExecutor`: Employs `Globus Compute <https://www.globus.org/compute>`_,
   to manage execution of tasks through a cloud service.
   Functions are securely executed on `Globus Compute Endpoints <https://globus-compute.readthedocs.io/en/latest/endpoints/endpoints.html>`_
   running on resources that need not be accessible from the computer running the Parsl script.
   Endpoints can use any of Parsl's other Executors to execute the tasks.

#. :class:`~parsl.executors.MPIExecutor` (MPEx): A specialized wrapper over HTEx designed to manage tasks which span multiple compute nodes.
   See `MPI and Multi-node Apps <../apps/mpi_apps.rst>`_.

#. :class:`~parsl.executors.ThreadPoolExecutor`: A lightweight executor that runs on the same computer as the Parsl script. 
   Tasks running on the ``ThreadPoolExecutor`` can start other Parsl tasks.

Start by following the documentation for the chosen executor.
Continue by specifying how to request compute nodes in the `Provider <../providers/index.html>`_ documentation.

.. toctree::
    :maxdepth: 2
    :caption: Table of Contents

    htex
    wqex
    advanced
