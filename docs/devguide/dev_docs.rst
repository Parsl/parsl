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


DataFlowKernel
==============

.. autoclass:: parsl.dataflow.dflow.DataFlowKernel
   :members:
   :special-members:

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
