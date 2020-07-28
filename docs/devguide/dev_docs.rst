Developer Guide
****************

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
