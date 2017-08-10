Developer Documentation
***********************


.. automodule:: parsl
   :no-undoc-members:

.. autofunction:: set_stream_logger

.. autofunction:: set_file_logger

Apps
====

Apps are parallelized functions that execute independent of the control flow of the main python
interpretor. We have two main types of Apps : PythonApps and BashApps. These are subclassed from
AppBase.

AppBase
-------

This is the base class that defines the two external facing functions that an App must define.
The  __init__ () which is called when the interpretor sees the definition of the decorated
function, and the __call__ () which is invoked when a decorated function is called by the user.

.. autoclass:: parsl.app.app.AppBase
   :members: __init__, __call__

PythonApp
---------

Concrete subclass of AppBase that implements the Python App functionality.

.. autoclass:: parsl.app.app.PythonApp
   :members: __init__, __call__

BashApp
-------

Concrete subclass of AppBase that implements the Bash App functionality.

.. autoclass:: parsl.app.app.BashApp
   :members: __init__, __call__


Futures
=======

Futures are returned as proxies to a parallel execution initiated by a call to an ``App``.
We have two kinds of futures in Parsl: AppFutures and DataFutures.


AppFutures
----------

.. autoclass:: parsl.dataflow.futures.AppFuture
   :members:


DataFutures
-----------

.. autoclass:: parsl.app.futures.DataFuture
   :members:


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

.. autoclass:: parsl.dataflow.error.DataFlowExceptions

.. autoclass:: parsl.dataflow.error.DuplicateTaskError

.. autoclass:: parsl.dataflow.error.MissingFutError


Executors
=========

Executors are abstractions that represent available compute resources to which you
could submit arbitrary App tasks. An executor initialized with an Execution Provider
can dynamically scale with the resources requirements of the workflow.

We currently have thread pools for local execution, remote workers from `ipyparallel <https://ipyparallel.readthedocs.io/en/latest/>`_ for executing on high throughput
systems such as campus clusters, and a Swift/T executor for HPC systems.

ParslExecutor
-------------

.. autoclass:: parsl.executors.base.ParslExecutor
   :members:  __init__, submit, scale_out, scale_in, scaling_enabled


ThreadPoolExecutor
------------------

.. autoclass:: parsl.executors.threads.ThreadPoolExecutor
   :members:  __init__, submit, scale_out, scale_in, shutdown, scaling_enabled

IPyParallelExecutor
------------------

.. autoclass:: parsl.executors.threads.ThreadPoolExecutor
   :members:  __init__, submit, scale_out, scale_in, shutdown, scaling_enabled, compose_launch_cmd


Swift/Turbine Executor
----------------------

.. autoclass:: parsl.executors.swift_t.TurbineExecutor
   :members: _queue_management_worker, weakred_cb, _start_queue_management_thread, shutdown, __init__, submit, scale_out, scale_in

.. autofunction:: parsl.executors.swift_t.runner


Execution Providers
===================

Execution providers are responsible for managing execution resources with a Local
Resource Manager (LRM). For instance, campus clusters and supercomputers generally have
schedulers such as Slurm, PBS, Condor and. Clouds on the other hand have API interfaces
that allow much more fine grain composition of an execution environment. An execution
provider abstracts these resources and provides a single uniform interface to them.


ExecutionProvider
-----------------

.. autoclass:: parsl.execution_provider.execution_provider_base.ExecutionProvider
   :members:  __init__, submit, status, cancel, scaling_enabled

ExecutionProvider
-----------------

.. autoclass:: parsl.execution_provider.execution_provider_base.ExecutionProvider
   :members:  __init__, submit, status, cancel, scaling_enabled


Slurm
-----

.. autoclass:: parsl.execution_provider.slurm.slurm.Slurm
   :members:  __init__, submit, status, cancel, _status, scaling_enabled, _write_submite_script, current_capacity

.. autofunction:: parsl.execution_provider.slurm.slurm.execute_wait


Amazon Web Services
-------------------

.. autoclass:: parsl.execution_provider.aws.aws.EC2Provider
   :members:
