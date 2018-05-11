Developer Guide
***************

.. automodule:: parsl
   :no-undoc-members:

.. autofunction:: set_stream_logger
   :noindex:

.. autofunction:: set_file_logger
   :noindex:

Apps
====

Apps are parallelized functions that execute independent of the control flow of the main python
interpreter. We have two main types of Apps : PythonApps and BashApps. These are subclassed from
AppBase.

AppBase
-------

This is the base class that defines the two external facing functions that an App must define.
The  __init__ () which is called when the interpreter sees the definition of the decorated
function, and the __call__ () which is invoked when a decorated function is called by the user.

.. autoclass:: parsl.app.app.AppBase
   :members:
   :noindex:

PythonApp
---------

Concrete subclass of AppBase that implements the Python App functionality.

.. autoclass:: parsl.app.python_app.PythonApp
   :members:

BashApp
-------

Concrete subclass of AppBase that implements the Bash App functionality.

.. autoclass:: parsl.app.bash_app.BashApp
   :members:

Futures
=======

Futures are returned as proxies to a parallel execution initiated by a call to an ``App``.
We have two kinds of futures in Parsl: AppFutures and DataFutures.


AppFutures
----------

.. autoclass:: parsl.dataflow.futures.AppFuture
   :members:
   :special-members:
   :noindex:


DataFutures
-----------

.. autoclass:: parsl.app.futures.DataFuture
   :members:
   :special-members:
   :noindex:


Exceptions
==========

.. autoclass:: parsl.app.errors.ParslError

.. autoclass:: parsl.app.errors.NotFutureError

.. autoclass:: parsl.app.errors.InvalidAppTypeError

.. autoclass:: parsl.app.errors.AppException

.. autoclass:: parsl.app.errors.AppBadFormatting

.. autoclass:: parsl.app.errors.AppFailure

.. autoclass:: parsl.app.errors.MissingOutputs

.. autoclass:: parsl.app.errors.DependencyError

.. autoclass:: parsl.dataflow.error.DataFlowException

.. autoclass:: parsl.dataflow.error.DuplicateTaskError

.. autoclass:: parsl.dataflow.error.MissingFutError

DataFlowKernel
==============

.. autoclass:: parsl.dataflow.dflow.DataFlowKernel
   :members:
   :special-members:



Executors
=========

Executors are abstractions that represent available compute resources to which you
could submit arbitrary App tasks. An executor initialized with an Execution Provider
can dynamically scale with the resources requirements of the workflow.

We currently have thread pools for local execution, remote workers from `ipyparallel <https://ipyparallel.readthedocs.io/en/latest/>`_ for executing on high throughput
systems such as campus clusters, and a Swift/T executor for HPC systems.

ParslExecutor (Abstract Base Class)
-----------------------------------

.. autoclass:: parsl.executors.base.ParslExecutor
   :members:  __init__, submit, scale_out, scale_in, scaling_enabled


ThreadPoolExecutor
------------------

.. autoclass:: parsl.executors.threads.ThreadPoolExecutor
   :members:  __init__, submit, scale_out, scale_in, scaling_enabled

IPyParallelExecutor
-------------------

.. autoclass:: parsl.executors.ipp.IPyParallelExecutor
   :members:  __init__, submit, scale_out, scale_in, scaling_enabled, compose_launch_cmd


Swift/Turbine Executor
----------------------

.. autoclass:: parsl.executors.swift_t.TurbineExecutor
   :members: _queue_management_worker, _start_queue_management_thread, shutdown, __init__, submit, scale_out, scale_in

.. autofunction:: parsl.executors.swift_t.runner


Execution Providers
===================

Execution providers are responsible for managing execution resources that have a Local
Resource Manager (LRM). For instance, campus clusters and supercomputers generally have
LRMs (schedulers) such as Slurm, Torque/PBS, Condor and Cobalt. Clouds, on the other hand, have API interfaces
that allow much more fine-grained composition of an execution environment. An execution
provider abstracts these types of resources and provides a single uniform interface to them.


ExecutionProvider (Base)
------------------------

.. autoclass:: libsubmit.providers.provider_base.ExecutionProvider
   :members:
   :special-members:


Local
-----

.. autoclass:: libsubmit.providers.local.local.Local
   :members:
   :special-members:

Slurm
-----

.. autoclass:: libsubmit.providers.slurm.slurm.Slurm
   :members:
   :special-members:

Cobalt
------

.. autoclass:: libsubmit.providers.cobalt.cobalt.Cobalt
   :members:
   :special-members:

Condor
------

.. autoclass:: libsubmit.providers.condor.condor.Condor
   :members:
   :special-members:

Torque
------

.. autoclass:: libsubmit.providers.torque.torque.Torque
   :members:
   :special-members:

GridEngine
----------

.. autoclass:: libsubmit.providers.gridEngine.gridEngine.GridEngine
   :members:
   :special-members:


Amazon Web Services
-------------------

.. autoclass:: libsubmit.providers.aws.aws.EC2Provider
   :members:
   :special-members:


Azure
-----

.. autoclass:: libsubmit.providers.azure.azureProvider.AzureProvider
   :members:  __init__, submit, status, cancel

.. autoclass:: libsubmit.providers.azure.azureDeployer.Deployer
   :members: __init__, deploy, destroy

Google Cloud Platform
---------------------

.. autoclass:: libsubmit.providers.googlecloud.googlecloud.GoogleCloud
    :members:  __init__, submit, status, cancel, create_instance, get_correct_zone, delete_instance

Channels
========

For certain resources such as campus clusters or supercomputers at research laboratories, resource requirements
may require authentication. For instance, some resources may allow access to their job schedulers from only
their login-nodes, which require you to authenticate on through SSH, GSI-SSH and sometimes even require
two-factor authentication. Channels are simple abstractions that enable the ExecutionProvider component to talk
to the resource managers of compute facilities. The simplest Channel, *LocalChannel*, simply executes commands
locally on a shell, while the *SshChannel* authenticates you to remote systems.

.. autoclass:: libsubmit.channels.channel_base.Channel
   :members:
   :special-members:


LocalChannel
------------
.. autoclass:: libsubmit.channels.local.local.LocalChannel
   :members:
   :special-members:


SshChannel
----------
.. autoclass:: libsubmit.channels.ssh.ssh.SshChannel
   :members:
   :special-members:


SshILChannel
------------
.. autoclass:: libsubmit.channels.ssh_il.ssh_il.SshILChannel
   :members:
   :special-members:



Launchers
=========

Launchers are basically wrappers for user submitted scripts as they are submitted to
a specific execution resource.

singleNodeLauncher
------------------

.. autofunction:: libsubmit.launchers.singleNodeLauncher

srunLauncher
------------

.. autofunction:: libsubmit.launchers.srunLauncher

srunMpiLauncher
---------------

.. autofunction:: libsubmit.launchers.srunMpiLauncher


Flow Control
============

This section deals with functionality related to controlling the flow of tasks to various different
execution sites.

FlowControl
-----------

.. autoclass:: parsl.dataflow.flow_control.FlowControl
   :members:

FlowNoControl
-------------

.. autoclass:: parsl.dataflow.flow_control.FlowNoControl
   :members:
   :special-members:


Timer
-----

.. autoclass:: parsl.dataflow.flow_control.Timer
   :members:
   :special-members:



Strategy
--------

Strategies are responsible for tracking the compute requirements of a workflow as it
is executed and scaling the resources to match it.

.. autoclass:: parsl.dataflow.strategy.Strategy
   :members:
   :special-members:


Memoization
===========

.. autoclass:: parsl.dataflow.memoization.Memoizer
   :members:
   :special-members:










