Developer Guide
****************

.. automodule:: parsl
   :no-undoc-members:

.. autofunction:: set_stream_logger
   :noindex:

.. autofunction:: set_file_logger
   :noindex:

Apps
====

Apps are parallelized functions that execute independent of the control flow of the main python
interpreter. We have two main types of Apps: PythonApps and BashApps. These are subclassed from
AppBase.

AppBase
-------

This is the base class that defines the two external facing functions that an App must define.
The  __init__ (), which is called when the interpreter sees the definition of the decorated
function, and the __call__ (), which is invoked when a decorated function is called by the user.

.. autoclass:: parsl.app.app.AppBase
   :members:
   :noindex:

PythonApp
---------

Concrete subclass of AppBase that implements the Python App functionality.

.. autoclass:: parsl.app.python.PythonApp
   :members:

BashApp
-------

Concrete subclass of AppBase that implements the Bash App functionality.

.. autoclass:: parsl.app.bash.BashApp
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

.. autoclass:: parsl.app.errors.AppException

.. autoclass:: parsl.app.errors.AppBadFormatting

.. autoclass:: parsl.app.errors.AppFailure

.. autoclass:: parsl.app.errors.MissingOutputs

.. autoclass:: parsl.dataflow.error.DataFlowException

.. autoclass:: parsl.dataflow.error.DependencyError

.. autoclass:: parsl.dataflow.error.DuplicateTaskError

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
   :members:  __init__, start, submit, scale_out, scale_in, scaling_enabled

HighThroughputExecutor
----------------------

.. autoclass:: parsl.executors.HighThroughputExecutor
   :members:  __init__, start, submit, scale_out, scale_in, scaling_enabled, compose_launch_cmd,
              _start_queue_management_thread, _start_local_queue_process,
              hold_worker, outstanding, connected_workers

WorkQueueExecutor
----------------------

.. autoclass:: parsl.executors.WorkQueueExecutor
   :members:  __init__, start, submit, scale_out, scale_in,
              shutdown, scaling_enabled, run_dir, create_new_name, create_name_tuple

ExtremeScaleExecutor
--------------------

.. autoclass:: parsl.executors.ExtremeScaleExecutor
   :members:  __init__, start, submit, scale_out, scale_in, scaling_enabled, compose_launch_cmd,
              _start_queue_management_thread, _start_local_queue_process,
              hold_worker, outstanding, connected_workers


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

.. autoclass:: parsl.providers.provider_base.ExecutionProvider
   :members:
   :special-members:


Local
-----

.. autoclass:: parsl.providers.LocalProvider
   :members:
   :special-members:

Slurm
-----

.. autoclass:: parsl.providers.SlurmProvider
   :members:
   :special-members:

Cobalt
------

.. autoclass:: parsl.providers.CobaltProvider
   :members:
   :special-members:

Condor
------

.. autoclass:: parsl.providers.CondorProvider
   :members:
   :special-members:

Torque
------

.. autoclass:: parsl.providers.TorqueProvider
   :members:
   :special-members:

GridEngine
----------

.. autoclass:: parsl.providers.GridEngineProvider
   :members:
   :special-members:


Amazon Web Services
-------------------

.. autoclass:: parsl.providers.AWSProvider
   :members:
   :special-members:


Google Cloud Platform
---------------------

.. autoclass:: parsl.providers.GoogleCloudProvider
   :members:  __init__, submit, status, cancel, create_instance, get_correct_zone, delete_instance


Kubernetes
----------

.. autoclass:: parsl.providers.KubernetesProvider
   :members:
   :special-members:


Channels
========

For certain resources such as campus clusters or supercomputers at research laboratories, resource requirements
may require authentication. For instance, some resources may allow access to their job schedulers from only
their login-nodes, which require you to authenticate on through SSH, GSI-SSH and sometimes even require
two-factor authentication. Channels are simple abstractions that enable the ExecutionProvider component to talk
to the resource managers of compute facilities. The simplest Channel, *LocalChannel*, simply executes commands
locally on a shell, while the *SshChannel* authenticates you to remote systems.

.. autoclass:: parsl.channels.base.Channel
   :members:
   :special-members:


LocalChannel
------------
.. autoclass:: parsl.channels.LocalChannel
   :members:
   :special-members:


SshChannel
----------
.. autoclass:: parsl.channels.SSHChannel
   :members:
   :special-members:


SSH Interactive Login Channel
-----------------------------
.. autoclass:: parsl.channels.SSHInteractiveLoginChannel
   :members:
   :special-members:

ExecutionProviders
------------------

An execution provider is basically an adapter to various types of execution resources. The providers abstract
away the interfaces provided by various systems to request, monitor, and cancel computate resources.

.. autoclass:: parsl.provider.provider_base.ExecutionProvider
   :members:  __init__, submit, status, cancel, scaling_enabled, channels_required


Slurm
^^^^^

.. autoclass:: parsl.providers.SlurmProvider
   :members:  __init__, submit, status, cancel, _status, scaling_enabled, _write_submit_script, current_capacity, channels_required

Cobalt
^^^^^^

.. autoclass:: parsl.providers.CobaltProvider
   :members:  __init__, submit, status, cancel, _status, scaling_enabled, _write_submit_script, current_capacity, channels_required

Condor
^^^^^^

.. autoclass:: parsl.providers.CondorProvider
   :members:  __init__, submit, status, cancel, _status, scaling_enabled, _write_submit_script, current_capacity, channels_required

Torque
^^^^^^

.. autoclass:: parsl.providers.TorqueProvider
   :members:  __init__, submit, status, cancel, _status, scaling_enabled, _write_submit_script, current_capacity, channels_required

Local
^^^^^

.. autoclass:: parsl.providers.LocalProvider
   :members:  __init__, submit, status, cancel, scaling_enabled, current_capacity, channels_required

AWS
^^^

.. autoclass:: parsl.providers.AWSProvider
   :members:  __init__, submit, status, cancel, scaling_enabled, current_capacity, channels_required, create_vpc, read_state_file, write_state_file, create_session, security_group

GridEngine
^^^^^^^^^^

.. autoclass:: parsl.providers.GridEngineProvider
   :members:  __init__, submit, status, cancel, scaling_enabled, current_capacity, channels_required, create_vpc, read_state_file, write_state_file, create_session, security_group



Channels
--------

For certain resources such as campus clusters or supercomputers at research laboratories, resource requirements
may require authentication. For instance some resources may allow access to their job schedulers from only
their login-nodes which require you to authenticate on through SSH, GSI-SSH and sometimes even require
two factor authentication. Channels are simple abstractions that enable the ExecutionProvider component to talk
to the resource managers of compute facilities. The simplest Channel, *LocalChannel*, simply executes commands
locally on a shell, while the *SshChannel* authenticates you to remote systems.

.. autoclass:: parsl.channels.base.Channel
   :members:  execute_wait, script_dir, execute_no_wait, push_file, close

LocalChannel
^^^^^^^^^^^^
.. autoclass:: parsl.channels.LocalChannel
   :members:  __init__, execute_wait, execute_no_wait, push_file, script_dir, close

SSHChannel
^^^^^^^^^^^^
.. autoclass:: parsl.channels.SSHChannel
   :members:  __init__, execute_wait, execute_no_wait, push_file, pull_file, script_dir, close

SSHILChannel
^^^^^^^^^^^^
.. autoclass:: parsl.channels.SSHInteractiveLoginChannel
   :members:  __init__, execute_wait, execute_no_wait, push_file, pull_file, script_dir, close



Launchers
=========

Launchers are basically wrappers for user submitted scripts as they are submitted to
a specific execution resource.

SimpleLauncher
--------------

.. autoclass:: parsl.launchers.SimpleLauncher
   :members:



SingleNodeLauncher
------------------

.. autoclass:: parsl.launchers.SingleNodeLauncher
   :members:

AprunLauncher
------------------

.. autoclass:: parsl.launchers.AprunLauncher
   :members:


SrunLauncher
------------

.. autoclass:: parsl.launchers.SrunLauncher
   :members:

SrunMPILauncher
---------------

.. autoclass:: parsl.launchers.SrunMPILauncher
   :members:


Flow Control
============

This section deals with functionality related to controlling the flow of tasks to various executors.

FlowControl
-----------

.. autoclass:: parsl.dataflow.flow_control.FlowControl
   :members:


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
