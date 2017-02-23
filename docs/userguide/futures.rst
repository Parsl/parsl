Futures
=======

When a python function is invoked, the python interpreter waits for the function to complete execution and the results are returned. With asynchronous apps, we do not want to wait for the completion of the apps. So, we return a `future <https://en.wikipedia.org/wiki/Futures_and_promises>`_. A future is essentially a construct that allows us to access the status, results, exceptions etc of an asynchronous function independent of the actual status of the App. They are proxies for a result that is not yet available.

In Parsl, we have two types of Futures: AppFutures and DataFutures.

AppFutures
----------

AppFutures are inherited from the python ``concurrent.futures.Future`` class. AppFutures represent an App itself and these can be used to get information about the status, results and exceptions if any of an App.


DataFutures
-----------

Similar to AppFutures, DataFuture are inherited from the python ``concurrent.futures.Future`` class.
They are future/proxies for output files that will be created by Apps.







